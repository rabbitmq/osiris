%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_replica).

-behaviour(gen_server).

-include("osiris.hrl").

%% osiris replica, spaws remote reader, TCP listener
%% replicates and confirms latest offset back to primary

%% API functions
-export([start/2,
         start_link/1,
         stop/2,
         delete/2]).
%% Test
-export([get_port/1]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% holds static or rarely changing fields
-record(cfg,
        {name :: string(),
         leader_pid :: pid(),
         directory :: file:filename(),
         port :: non_neg_integer(),
         socket :: undefined | gen_tcp:socket(),
         gc_interval :: non_neg_integer(),
         reference :: term(),
         offset_ref :: atomics:atomics_ref(),
         event_formatter :: undefined | mfa(),
         counter :: counters:counters_ref(),
         token :: undefined | binary()}).

-type parse_state() ::
    undefined |
    binary() |
    {non_neg_integer(), iolist(), non_neg_integer()}.

-record(?MODULE,
        {cfg :: #cfg{},
         parse_state :: parse_state(),
         log :: osiris_log:state(),
         committed_offset = -1 :: -1 | osiris:offset(),
         offset_listeners = [] ::
             [{pid(), osiris:offset(), mfa() | undefined}]}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-define(ADD_COUNTER_FIELDS, [committed_offset, forced_gcs, packets, readers]).
-define(C_COMMITTED_OFFSET, ?C_NUM_LOG_FIELDS + 1).
-define(C_FORCED_GCS, ?C_NUM_LOG_FIELDS + 2).
-define(C_PACKETS, ?C_NUM_LOG_FIELDS + 3).
-define(C_READERS, ?C_NUM_LOG_FIELDS + 4).
-define(DEFAULT_ONE_TIME_TOKEN_TIMEOUT, 30000).
-define(TOKEN_SIZE, 32).
-define(DEF_REC_BUF, 408300 * 5).

%%%===================================================================
%%% API functions
%%%===================================================================

start(Node, Config = #{name := Name}) when is_list(Name) ->
    supervisor:start_child({?SUP, Node},
                           #{id => Name,
                             start => {?MODULE, start_link, [Config]},
                             restart => temporary,
                             shutdown => 5000,
                             type => worker,
                             modules => [?MODULE]}).

stop(Node, #{name := Name}) ->
    ?SUP:stop_child(Node, Name).

delete(Node, Config = #{}) ->
    ?SUP:delete_child(Node, Config).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Conf) ->
    gen_server:start_link(?MODULE, Conf, []).

get_port(Server) ->
    gen_server:call(Server, get_port).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(#{name := Name,
       leader_pid := LeaderPid,
       reference := ExtRef} =
         Config) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    {ok, {Min, Max}} = application:get_env(port_range),
    RecBuf = application:get_env(osiris, replica_recbuf, ?DEF_REC_BUF),
    {Port, LSock} = open_tcp_port(RecBuf, Min, Max),
    Self = self(),
    spawn_link(fun() -> accept(LSock, Self) end),
    CntName = {?MODULE, ExtRef},
    Node = node(LeaderPid),

    ORef = atomics:new(2, [{signed, true}]),
    atomics:put(ORef, 1, -1),
    atomics:put(ORef, 2, -1),
    {ok, {_, LeaderEpochOffs}} =
        rpc:call(Node, osiris_writer, overview, [LeaderPid]),
    ?DEBUG("~s: writer epoch offset ~w", [?MODULE, LeaderEpochOffs]),

    Dir = osiris_log:directory(Config),
    Log = osiris_log:init_acceptor(LeaderEpochOffs,
                                   Config#{dir => Dir,
                                           first_offset_fun =>
                                           fun (Fst) ->
                                                   atomics:put(ORef, 2, Fst)
                                           end,
                                           counter_spec =>
                                               {CntName, ?ADD_COUNTER_FIELDS}}),
    CntRef = osiris_log:counters_ref(Log),
    {NextOffset, LastChunk} = TailInfo = osiris_log:tail_info(Log),

    ?DEBUG("~s: tail info: ~w", [Name, TailInfo]),
    case LastChunk of
        empty ->
            ok;
        {_, LastChId, LastTs} ->
            %% need to ack last chunk back to leader so that it can
            %% re-discover the committed offset
            osiris_writer:ack(LeaderPid, {LastChId, LastTs})
    end,
    ?INFO("osiris_replica:init/1: next offset ~b",
          [NextOffset]),
    %% spawn reader process on leader node
    {ok, HostName} = inet:gethostname(),
    {ok, Ips} = inet:getaddrs(HostName, inet),
    Token = crypto:strong_rand_bytes(?TOKEN_SIZE),
    ReplicaReaderConf =
        #{hosts => Ips,
          port => Port,
          name => Name,
          replica_pid => self(),
          leader_pid => LeaderPid,
          start_offset => TailInfo,
          reference => ExtRef,
          connection_token => Token},
    RRPid =
        case supervisor:start_child({osiris_replica_reader_sup, Node},
                                    #{id => make_ref(),
                                      start =>
                                          {osiris_replica_reader, start_link,
                                           [ReplicaReaderConf]},
                                      %% replica readers should never be
                                      %% restarted by their sups
                                      %% instead they need to be re-started
                                      %% by their replica
                                      restart => temporary,
                                      shutdown => 5000,
                                      type => worker,
                                      modules => [osiris_replica_reader]})
        of
            {ok, Pid} ->
                Pid;
            {ok, Pid, _} ->
                Pid
        end,
    true = link(RRPid),
    Interval = maps:get(replica_gc_interval, Config, 5000),
    erlang:send_after(Interval, self(), force_gc),
    counters:put(CntRef, ?C_COMMITTED_OFFSET, -1),
    EvtFmt = maps:get(event_formatter, Config, undefined),
    {ok,
     #?MODULE{cfg =
                  #cfg{name = Name,
                       leader_pid = LeaderPid,
                       directory = Dir,
                       port = Port,
                       gc_interval = Interval,
                       reference = ExtRef,
                       offset_ref = ORef,
                       event_formatter = EvtFmt,
                       counter = CntRef,
                       token = Token},
              log = Log,
              parse_state = undefined}}.

open_tcp_port(_RcvBuf, M, M) ->
    throw({error, all_busy});
open_tcp_port(RcvBuf, Min, Max) ->
    case gen_tcp:listen(Min,
                        [binary,
                         {packet, raw},
                         {active, false},
                         {buffer, RcvBuf * 2},
                         {recbuf, RcvBuf}])
    of
        {ok, LSock} ->
            {Min, LSock};
        {error, eaddrinuse} ->
            open_tcp_port(RcvBuf, Min + 1, Max);
        E ->
            throw(E)
    end.

accept(LSock, Process) ->
    {ok, Sock} = gen_tcp:accept(LSock),

    ?DEBUG("~s: socket accepted opts ~w",
           [?MODULE, inet:getopts(Sock, [buffer, recbuf])]),
    Process ! {socket, Sock},
    gen_tcp:controlling_process(Sock, Process),
    _ = gen_tcp:close(LSock),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(get_port, _From,
            #?MODULE{cfg = #cfg{port = Port}} = State) ->
    {reply, Port, State};
handle_call(get_reader_context, _From,
            #?MODULE{cfg =
                         #cfg{offset_ref = ORef,
                              name = Name,
                              directory = Dir,
                              reference = Ref,
                              counter = CntRef},
                     committed_offset = COffs} =
                State) ->
    Reply =
        #{dir => Dir,
          name => Name,
          committed_offset => COffs,
          offset_ref => ORef,
          reference => Ref,
          readers_counter_fun => fun(Inc) -> counters:add(CntRef, ?C_READERS, Inc) end},
    {reply, Reply, State};
handle_call({update_retention, Retention}, _From,
            #?MODULE{log = Log0} = State) ->
    Log = osiris_log:update_retention(Retention, Log0),
    {reply, ok, State#?MODULE{log = Log}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({committed_offset, Offs},
            #?MODULE{cfg = #cfg{offset_ref = ORef, counter = Cnt},
                     committed_offset = Last} =
                State) ->
    case Offs > Last of
        true ->
            %% notify offset listeners
            counters:put(Cnt, ?C_COMMITTED_OFFSET, Offs),
            ok = atomics:put(ORef, 1, Offs),
            {noreply,
             notify_offset_listeners(State#?MODULE{committed_offset = Offs})};
        false ->
            State
    end;
handle_cast({register_offset_listener, Pid, EvtFormatter, Offset},
            #?MODULE{offset_listeners = Listeners} = State0) ->
    State1 =
        State0#?MODULE{offset_listeners =
                           [{Pid, Offset, EvtFormatter} | Listeners]},
    State = notify_offset_listeners(State1),
    {noreply, State};
handle_cast(Msg, State) ->
    ?DEBUG("osiris_replica unhanded cast ~w", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(force_gc,
            #?MODULE{cfg = #cfg{gc_interval = Interval, counter = Cnt}} =
                State) ->
    garbage_collect(),
    counters:add(Cnt, ?C_FORCED_GCS, 1),
    erlang:send_after(Interval, self(), force_gc),
    {noreply, State};
handle_info({socket, Socket}, #?MODULE{cfg = #cfg{name = Name,
                                                  token = Token} = Cfg} = State) ->
    Timeout = application:get_env(osiris, one_time_token_timeout,
                                  ?DEFAULT_ONE_TIME_TOKEN_TIMEOUT),
    case gen_tcp:recv(Socket, ?TOKEN_SIZE, Timeout) of
        {ok, Token} ->
            %% token validated, all good we can let the flood of data begin
            ok = inet:setopts(Socket, [{active, 5}]),
            {noreply, State#?MODULE{cfg = Cfg#cfg{socket = Socket}}};
        {ok, Other} ->
            ?WARN("~s: ~s invalid token received ~w",
                  [?MODULE, Name, Other]),
            {stop, invalid_token, State};
        {error, Reason} ->
            ?WARN("~s: ~s error awaiting token ~w",
                  [?MODULE, Name, Reason])
    end;
handle_info({tcp, Socket, Bin},
            #?MODULE{cfg =
                         #cfg{socket = Socket,
                              leader_pid = LeaderPid,
                              counter = Cnt},
                     parse_state = ParseState0,
                     log = Log0} =
                State0) ->
    counters:add(Cnt, ?C_PACKETS, 1),
    %% deliberately ignoring return value here as it would fail if the
    %% tcp connection has been closed and we still want to try to process
    %% any messages still in the mailbox
    _ = inet:setopts(Socket, [{active, 1}]),
    %% validate chunk
    {ParseState, OffsetChunks} = parse_chunk(Bin, ParseState0, []),
    {OffsetTimestamp, Log} =
        lists:foldl(fun({OffsetTs, B}, {_OffsTs, Acc0}) ->
                       Acc = osiris_log:accept_chunk(B, Acc0),
                       {OffsetTs, Acc}
                    end,
                    {undefined, Log0}, OffsetChunks),
    State1 = State0#?MODULE{log = Log, parse_state = ParseState},
    case OffsetTimestamp of
        undefined ->
            {noreply, State1};
        _ ->
            State = notify_offset_listeners(State1),
            ok = osiris_writer:ack(LeaderPid, OffsetTimestamp),
            {noreply, State}
    end;
handle_info({tcp_passive, Socket},
            #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
    %% we always top up before processing each packet so no need to do anything
    %% here
    {noreply, State};
handle_info({tcp_closed, Socket},
            #?MODULE{cfg = #cfg{name = Name, socket = Socket}} = State) ->
    ?DEBUG("osiris_replica: ~s Socket closed. Exiting...", [Name]),
    {stop, normal, State};
handle_info({tcp_error, Socket, Error},
            #?MODULE{cfg = #cfg{name = Name, socket = Socket}} = State) ->
    ?DEBUG("osiris_replica: ~s Socket error ~p. Exiting...",
           [Name, Error]),
    {stop, {tcp_error, Error}, State};
handle_info({'DOWN', _Ref, process, Pid, Info}, State) ->
    ?DEBUG("osiris_replica:handle_info/2: DOWN received Pid "
           "~w, Info: ~w",
           [Pid, Info]),
    {noreply, State};
handle_info({'EXIT', Ref, Info}, State) ->
    ?DEBUG("osiris_replica:handle_info/2: EXIT received "
           "~w, Info: ~w",
           [Ref, Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, #?MODULE{cfg = #cfg{name = Name}, log = Log}) ->
    ?DEBUG("~s: ~s terminating with ~w ", [?MODULE, Name, Reason]),
    ok = osiris_log:close(Log),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

parse_chunk(<<>>, ParseState, Acc) ->
    {ParseState, lists:reverse(Acc)};
parse_chunk(<<?MAGIC:4/unsigned,
              ?VERSION:4/unsigned,
              _ChType:8/unsigned,
              _NumEntries:16/unsigned,
              _NumRecords:32/unsigned,
              Timestamp:64/signed,
              _Epoch:64/unsigned,
              FirstOffset:64/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              TSize:32/unsigned,
              _Reserved:32,
              _Data:Size/binary,
              _TData:TSize/binary,
              Rem/binary>> =
                All,
            undefined, Acc) ->
    TotalSize = Size + TSize + ?HEADER_SIZE_B,
    <<Chunk:TotalSize/binary, _/binary>> = All,
    parse_chunk(Rem, undefined, [{{FirstOffset, Timestamp}, Chunk} | Acc]);
parse_chunk(Bin, undefined, Acc)
    when byte_size(Bin) =< ?HEADER_SIZE_B ->
    {Bin, lists:reverse(Acc)};
parse_chunk(<<?MAGIC:4/unsigned,
              ?VERSION:4/unsigned,
              _ChType:8/unsigned,
              _NumEntries:16/unsigned,
              _NumRecords:32/unsigned,
              Timestamp:64/signed,
              _Epoch:64/unsigned,
              FirstOffset:64/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              TSize:32/unsigned,
              _Reserved:32,
              Partial/binary>> =
                All,
            undefined, Acc) ->
    {{{FirstOffset, Timestamp}, [All], Size + TSize - byte_size(Partial)},
     lists:reverse(Acc)};
parse_chunk(Bin, PartialHeaderBin, Acc)
    when is_binary(PartialHeaderBin) ->
    %% slight inneficiency but partial headers should be relatively
    %% rare and fairly small - also ensures the header is always intact
    parse_chunk(<<PartialHeaderBin/binary, Bin/binary>>, undefined, Acc);
parse_chunk(Bin, {FirstOffsetTs, IOData, RemSize}, Acc)
    when byte_size(Bin) >= RemSize ->
    <<Final:RemSize/binary, Rem/binary>> = Bin,
    parse_chunk(Rem, undefined,
                [{FirstOffsetTs, lists:reverse([Final | IOData])} | Acc]);
parse_chunk(Bin, {FirstOffsetTs, IOData, RemSize}, Acc) ->
    %% there is not enough data to complete the partial chunk
    {{FirstOffsetTs, [Bin | IOData], RemSize - byte_size(Bin)},
     lists:reverse(Acc)}.

notify_offset_listeners(#?MODULE{cfg =
                                     #cfg{reference = Ref,
                                          event_formatter = EvtFmt},
                                 committed_offset = COffs,
                                 log = Log,
                                 offset_listeners = L0} =
                            State) ->
    case osiris_log:tail_info(Log) of
        {_NextOffs, {_, LastChId, _LastTs}} ->
            Max = min(COffs, LastChId),
            %% do not notify offset listeners if the committed offset isn't
            %% available locally yet
            {Notify, L} =
                lists:splitwith(fun({_Pid, O, _}) -> O =< Max end, L0),
            [begin
                 Evt =
                 wrap_osiris_event(%% the per offset listener event formatter takes precedence of
                   %% the process scoped one
                   select_formatter(Fmt, EvtFmt),
                   {osiris_offset, Ref, COffs}),
                 P ! Evt
             end
             || {P, _, Fmt} <- Notify],
            State#?MODULE{offset_listeners = L};
        _ ->
            State
    end.

%% INTERNAL

wrap_osiris_event(undefined, Evt) ->
    Evt;
wrap_osiris_event({M, F, A}, Evt) ->
    apply(M, F, [Evt | A]).

select_formatter(undefined, Fmt) ->
    Fmt;
select_formatter(Fmt, _) ->
    Fmt.

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.
