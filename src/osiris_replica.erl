-module(osiris_replica).

-behaviour(gen_server).

-define(MAGIC, 5).
%% format version
-define(VERSION, 0).
-define(HEADER_SIZE, 31).

-define(SUP, osiris_server_sup).

%% osiris replica, spaws remote reader, TCP listener
%% replicates and confirms latest offset back to primary

%% API functions
-export([start/2, start_link/1, stop/2, delete/2]).
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
-record(cfg, {leader_pid :: pid(),
              directory :: file:filename(),
              port :: non_neg_integer(),
              listening_socket :: gen_tcp:socket(),
              socket :: undefined | gen_tcp:socket(),
              gc_interval :: non_neg_integer(),
              external_ref :: term()
             }).

-type parse_state() :: undefined |
                       binary() |
                       {non_neg_integer(), iolist(), non_neg_integer()}.

-record(?MODULE, {cfg :: #cfg{},
                  parse_state :: parse_state(),
                  log :: osiris_log:state(),
                  counter :: counters:counters_ref()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-define(COUNTER_FIELDS,
        [chunks_written,
         offset,
         forced_gcs,
         packets]).
-define(C_CHUNKS_WRITTEN, 1).
-define(C_OFFSET, 2).
-define(C_FORCED_GCS, 3).
-define(C_PACKETS, 4).

%%%===================================================================
%%% API functions
%%%===================================================================

start(Node, Config = #{name := Name}) when is_list(Name) ->
    %% READERS pumps data on replicas!!! replicas are the gen_tcp listeners - whch is
    %% different from this
    %% master unaware of replicas
    %% TODO if we select from a range, how do we know in the other side
    %% which one have we selected???
    %% TODO What's the Id? How many replicas do we have?
    %% TODO How do we know the name of the log to write to disk?
    %%` TODO another replica for the index?
    supervisor:start_child({?SUP, Node},
                           #{id => Name,
                             start => {?MODULE, start_link, [Config]},
                             restart => transient,
                             shutdown => 5000,
                             type => worker,
                             modules => [?MODULE]}) .

stop(Node, #{name := Name}) ->
    _ = supervisor:terminate_child({?SUP, Node}, Name),
    _ = supervisor:delete_child({?SUP, Node}, Name),
    ok.

delete(Node, Config) ->
    stop(Node, Config),
    rpc:call(Node, osiris_log, delete_directory, [Config]).

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
init(#{leader_pid := LeaderPid,
       external_ref := ExtRef} = Config) ->
    {ok, {Min, Max}} = application:get_env(port_range),
    %% TODO: use locally configured port range
    {Port, LSock} = open_tcp_port(Min, Max),
    Self = self(),
    spawn_link(fun() -> accept(LSock, Self) end),
    CntRef = osiris_counters:new({?MODULE, ExtRef}, ?COUNTER_FIELDS),
    Node = node(LeaderPid),

    {ok, {_, LeaderEpochOffs}} = rpc:call(Node, osiris_writer,
                                          overview, [LeaderPid]),

    Dir = osiris_log:directory(Config),
    Log = osiris_log:init_acceptor(LeaderEpochOffs, Config#{dir => Dir}),
    NextOffset = osiris_log:next_offset(Log),
    error_logger:info_msg("osiris_replica:init/1: next offset ~b",
                          [NextOffset]),
    %% spawn reader process on leader node
    {ok, HostName} = inet:gethostname(),
    {ok, Ip} = inet:getaddr(HostName, inet),
    ReplicaReaderConf = #{host => Ip,
                          port => Port,
                          leader_pid => LeaderPid,
                          start_offset => osiris_log:tail_info(Log),
                          external_ref => ExtRef},
    case supervisor:start_child({osiris_replica_reader_sup, Node},
                                #{
                                  id => make_ref(),
                                  start => {osiris_replica_reader, start_link,
                                            [ReplicaReaderConf]},
                                  restart => transient,
                                  shutdown => 5000,
                                  type => worker,
                                  modules => [osiris_replica_reader]}) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok
    end,
    Interval = maps:get(replica_gc_interval, Config, 5000),
    erlang:send_after(Interval, self(), force_gc),
    %% TODO: monitor leader pid
    {ok, #?MODULE{cfg = #cfg{leader_pid = LeaderPid,
                             directory = Dir,
                             port = Port,
                             listening_socket = LSock,
                             gc_interval = Interval,
                             external_ref = ExtRef},
                  log = Log,
                  counter = CntRef}}.


open_tcp_port(M, M) ->
    throw({error, all_busy});
open_tcp_port(Min, Max) ->
    RcvBuf = 408300 * 5,
    case gen_tcp:listen(Min, [binary,
                              {packet, raw},
                              {active, false},
                              {buffer, RcvBuf * 2},
                              {recbuf, RcvBuf}
                             ]) of
        {ok, LSock} ->
            {Min, LSock};
        {error, eaddrinuse} ->
            open_tcp_port(Min + 1, Max);
        E ->
            throw(E)
    end.

accept(LSock, Process) ->
    %% TODO what if we have more than 1 connection?
    {ok, Sock} = gen_tcp:accept(LSock),

    io:format("sock opts ~w", [inet:getopts(Sock, [buffer, recbuf])]),
    Process ! {socket, Sock},
    gen_tcp:controlling_process(Sock, Process),
    accept(LSock, Process).

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
handle_call(get_port, _From, #?MODULE{cfg = #cfg{port = Port}} = State) ->
    {reply, Port, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

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
handle_cast(_Msg, State) ->
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
handle_info(force_gc, #?MODULE{cfg = #cfg{gc_interval = Interval},
                               counter = Cnt} = State) ->
    garbage_collect(),
    counters:add(Cnt, ?C_FORCED_GCS, 1),
    erlang:send_after(Interval, self(), force_gc),
    {noreply, State};
handle_info({socket, Socket}, #?MODULE{cfg = Cfg} = State) ->
    ok = inet:setopts(Socket, [{active, 5}]),

    {noreply, State#?MODULE{cfg = Cfg#cfg{socket = Socket}}};
handle_info({tcp, Socket, Bin},
            #?MODULE{cfg = #cfg{socket = Socket,
                                leader_pid = LeaderPid},
                     parse_state = ParseState0,
                     log = Log0,
                     counter = Cnt} = State) ->
    ok = inet:setopts(Socket, [{active, 1}]),
    %% validate chunk
    {ParseState, OffsetChunks} = parse_chunk(Bin, ParseState0, []),
    {Acks, Log} = lists:foldl(
                        fun({FirstOffset, B}, {Aks, Acc0}) ->
                                Acc = osiris_log:accept_chunk(B, Acc0),
                                counters:add(Cnt, ?C_CHUNKS_WRITTEN, 1),
                                {[FirstOffset | Aks], Acc}
                        end, {[], Log0}, OffsetChunks),
    LastOffs = osiris_log:next_offset(Log) - 1,
    counters:put(Cnt, ?C_OFFSET, LastOffs),
    counters:add(Cnt, ?C_PACKETS, 1),
    case Acks of
        [] -> ok;
        _ ->
            ok = osiris_writer:ack(LeaderPid, lists:reverse(Acks))
    end,
    {noreply, State#?MODULE{log = Log,
                            parse_state = ParseState}};
handle_info({tcp_passive, Socket},
            #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
    %% we always top up before processing each packet so no need to do anything
    %% here
    {noreply, State};
handle_info({tcp_closed, Socket},
            #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
    error_logger:info_msg("Socket closed ~n", []),
    {noreply, State};
handle_info({tcp_error, Socket, Error},
            #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
    error_logger:info_msg("Socket error ~p~n", [Error]),
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
terminate(_Reason, #?MODULE{cfg = #cfg{external_ref = ExtRef}}) ->
    ok = osiris_counters:delete({?MODULE, ExtRef}),
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
              _NumEntries:16/unsigned,
              _NumRecords:32/unsigned,
                _Epoch:64/unsigned,
              FirstOffset:64/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              _:Size/binary,
              Rem/binary>> = All, undefined, Acc) ->
    TotalSize = Size + ?HEADER_SIZE,
    <<Chunk:TotalSize/binary, _/binary>> = All,
    parse_chunk(Rem, undefined, [{FirstOffset, Chunk} | Acc]);
parse_chunk(Bin, undefined, Acc)
  when byte_size(Bin) =< ?HEADER_SIZE ->
    {Bin, lists:reverse(Acc)};
parse_chunk(<<?MAGIC:4/unsigned,
              ?VERSION:4/unsigned,
              _NumEntries:16/unsigned,
              _NumRecords:32/unsigned,
                _Epoch:64/unsigned,
              FirstOffset:64/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              Partial/binary>> = All, undefined, Acc) ->
    {{FirstOffset, [All], Size - byte_size(Partial)}, lists:reverse(Acc)};
parse_chunk(Bin, PartialHeaderBin, Acc)
  when is_binary(PartialHeaderBin) ->
    %% TODO: slight inneficiency but partial headers should be relatively
    %% rare - also ensures the header is always intact
    parse_chunk(<<PartialHeaderBin/binary, Bin/binary>>, undefined, Acc);
parse_chunk(Bin, {FirstOffset, IOData, RemSize}, Acc)
  when byte_size(Bin) >= RemSize ->
    <<Final:RemSize/binary, Rem/binary>> = Bin,
    parse_chunk(Rem, undefined,
                [{FirstOffset, lists:reverse([Final | IOData])} | Acc]);
parse_chunk(Bin, {FirstOffset, IOData, RemSize}, Acc) ->
    % error_logger:info_msg("parse_chunk ~b", [FirstOffset]),
    %% there is not enough data to complete the partial chunk
    {{FirstOffset, [Bin | IOData], RemSize - byte_size(Bin)},
     lists:reverse(Acc)}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-endif.
