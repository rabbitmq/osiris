%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_replica_reader).

-behaviour(gen_server).

-include("osiris.hrl").

-define(DEF_SND_BUF, 146988 * 10).
%% replica reader, spawned remotely by replica process, connects back to
%% configured host/port, reads entries from master and uses file:sendfile to
%% replicate read records

%% API functions
-export([start_link/1,
         start/2,
         stop/1]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([formatter/1]).

-record(state,
        {log :: osiris_log:state(),
         name :: osiris:name(),
         transport :: osiris_log:transport(),
         socket :: gen_tcp:socket() | ssl:sslsocket(),
         replica_pid :: pid(),
         leader_pid :: pid(),
         leader_monitor_ref :: reference(),
         counter :: counters:counters_ref(),
         counter_id :: term(),
         committed_offset = -1 :: -1 | osiris:offset(),
         offset_listener :: undefined | osiris:offset()}).

-define(C_OFFSET_LISTENERS, ?C_NUM_LOG_FIELDS + 1).
-define(COUNTER_FIELDS,
        [
         {offset_listeners, ?C_OFFSET_LISTENERS, counter, "Number of offset listeners"}
        ]
       ).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Conf) ->
    gen_server:start_link(?MODULE, Conf, []).

stop(Pid) ->
    gen_server:cast(Pid, stop).

start(Node, ReplicaReaderConf) when is_map(ReplicaReaderConf) ->
    supervisor:start_child({osiris_replica_reader_sup, Node},
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
                             modules => [osiris_replica_reader]}).

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

init(#{hosts := Hosts,
       port := Port,
       transport := Transport,
       name := Name,
       replica_pid := ReplicaPid,
       leader_pid := LeaderPid,
       start_offset := {StartOffset, _} = TailInfo,
       reference := ExtRef,
       connection_token := Token}) ->
    process_flag(trap_exit, true),

    ?DEBUG("~ts: trying to connect to replica at ~0p", [Name, Hosts]),

    case maybe_connect(Name, Transport, Hosts, Port, connect_options())
    of
        {ok, Sock, Host} ->
            ?DEBUG_(Name, "successfully connected to host ~0p port ~b",
                    [Host, Port]),
            CntId = {?MODULE, ExtRef, Host, Port},
            CntSpec = {CntId, ?COUNTER_FIELDS},
            Config = #{counter_spec => CntSpec, transport => Transport},
            %% send token to replica to complete connection setup
            ok = send(Transport, Sock, Token),
            Ret = osiris_writer:init_data_reader(LeaderPid, TailInfo, Config),
            case Ret of
                {ok, Log} ->
                    CntRef = osiris_log:counters_ref(Log),
                    ?INFO_(Name, "starting osiris replica reader at offset ~b",
                          [osiris_log:next_offset(Log)]),

                    %% register data listener with osiris_proc
                    ok = osiris_writer:register_data_listener(LeaderPid, StartOffset),
                    MRef = monitor(process, LeaderPid),
                    State =
                        maybe_register_offset_listener(
                          maybe_send_committed_offset(#state{log = Log,
                                                             name = Name,
                                                             transport = Transport,
                                                             socket = Sock,
                                                             replica_pid = ReplicaPid,
                                                             leader_pid = LeaderPid,
                                                             leader_monitor_ref = MRef,
                                                             counter = CntRef,
                                                             counter_id = CntId})),
                    {ok, State};
                {error, no_process} ->
                    ?WARN_(Name,
                           "osiris writer for ~0p is down, "
                           "replica reader will not start",
                          [ExtRef]),
                    {stop, normal};
                {error, enoent} ->
                    ?WARN_(Name,
                           "data reader for ~0p encountered an 'enonet' error whilst
                           initialising, replica reader will not start",
                          [ExtRef]),
                    {stop, normal};
                {error, {offset_out_of_range, Range}} ->
                    ?WARN_(Name,
                           "data reader requested an offset (~b) that was out
                           of range: ~0p, replica reader will not start",
                          [StartOffset, Range]),
                    {stop, normal};
                {error, {invalid_last_offset_epoch, Epoch, Offset}} ->
                    ?WARN_(Name,
                           "data reader found an invalid last offset epoch:
                           epoch ~0p offset ~0p, replica reader will not start",
                           [Epoch, Offset]),
                    {stop, normal}
            end;
        {error, Reason} ->
            ?WARN_(Name, "could not connect replica reader to replica at ~0p port ~b, Reason: ~0p",
                   [Hosts, Port, Reason]),
            {stop, Reason}
    end.

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
handle_cast({more_data, _LastOffset},
            #state{leader_pid = LeaderPid} = State0) ->
    #state{log = Log} = State = do_sendfile(State0),
    NextOffs = osiris_log:next_offset(Log),
    ok = osiris_writer:register_data_listener(LeaderPid, NextOffs),
    {noreply, maybe_register_offset_listener(State)};
handle_cast(stop, State) ->
    {stop, normal, State}.

maybe_register_offset_listener(#state{leader_pid = LeaderPid,
                                      committed_offset = COffs,
                                      counter = Cnt,
                                      offset_listener = undefined} =
                                   State) ->
    ok = counters:add(Cnt, ?C_OFFSET_LISTENERS, 1),
    ok =
        osiris:register_offset_listener(LeaderPid, COffs + 1,
                                        {?MODULE, formatter, []}),
    State#state{offset_listener = COffs + 1};
maybe_register_offset_listener(State) ->
    State.

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
handle_info({osiris_offset, _, _Offs}, State0) ->
    State1 = maybe_send_committed_offset(State0),
    State =
        maybe_register_offset_listener(State1#state{offset_listener =
                                                        undefined}),
    {noreply, State};
handle_info({'DOWN', Ref, _, _, Info},
            #state{name = Name,
                   transport = Transport,
                   socket = Sock,
                   leader_monitor_ref = Ref} =
                State) ->
    %% leader is down, exit
    ?ERROR_(Name, "detected leader down with ~W - exiting...",
           [Info, 10]),
    %% this should be enough to make the replica shut down
    ok = close(Transport, Sock),
    {stop, Info, State};
handle_info({tcp_closed, Socket},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG_(Name, "Socket closed. Exiting...", []),
    {stop, normal, State};
handle_info({ssl_closed, Socket},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG_(Name, "TLS socket closed. Exiting...", []),
    {stop, normal, State};
handle_info({tcp_error, Socket, Error},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG_(Name, "Socket error ~0p. "
           "Exiting...", [Error]),
    {stop, {tcp_error, Error}, State};
handle_info({ssl_error, Socket, Error},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG_(Name, "TLS socket error ~0p. "
           "Exiting...", [Error]),
    {stop, {ssl_error, Error}, State};
handle_info({'EXIT', Ref, Info}, #state{name = Name} = State) ->
    ?DEBUG_(Name, "EXIT received "
           "~w, Info: ~w",
           [Ref, Info]),
    {stop, normal, State};
handle_info(Info, #state{name = Name} = State) ->
    ?DEBUG_(Name, "'~ts' unhandled message ~W",
           [Info, 10]),
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
terminate(_Reason, #state{log = Log}) ->
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

do_sendfile(#state{socket = Sock,
                   transport = Transport} = State) ->
    ok = setopts(Transport, Sock, [{nopush, true}]),
    do_sendfile0(State).

do_sendfile0(#state{name = Name,
                    socket = Sock,
                    transport = Transport,
                    log = Log0} = State0) ->
    State = maybe_send_committed_offset(State0),
    case osiris_log:send_file(Sock, Log0) of
        {ok, Log} ->
            do_sendfile0(State#state{log = Log});
        {error, _Err} ->
            ok = setopts(Transport, Sock, [{nopush, false}]),
            ?DEBUG_(Name, "sendfile err ~w", [_Err]),
            State;
        {end_of_stream, Log} ->
            ok = setopts(Transport, Sock, [{nopush, false}]),
            State#state{log = Log}
    end.

maybe_send_committed_offset(#state{log = Log,
                                   committed_offset = Last,
                                   replica_pid = RPid} =
                                State) ->
    COffs = osiris_log:committed_offset(Log),
    case COffs of
        COffs when COffs > Last ->
            ok =
                erlang:send(RPid, {'$gen_cast', {committed_offset, COffs}},
                            [noconnect, nosuspend]),
            State#state{committed_offset = COffs};
        _ ->
            State
    end.

formatter(Evt) ->
    Evt.

send(tcp, Socket, Data) ->
    gen_tcp:send(Socket, Data);
send(ssl, Socket, Data) ->
    ssl:send(Socket, Data).

close(tcp, Socket) ->
    gen_tcp:close(Socket);
close(ssl, Socket) ->
    ssl:close(Socket).

connect_options() ->
    SndBuf = application:get_env(osiris, replica_sndbuf, ?DEF_SND_BUF),
    KeepAlive = application:get_env(osiris, replica_keepalive, false),
    [binary,
     {packet, 0},
     {nodelay, true},
     {sndbuf, SndBuf},
     {keepalive, KeepAlive}].

setopts(tcp, Sock, Opts) ->
    ok = inet:setopts(Sock, Opts);
setopts(ssl, Sock, Opts) ->
    ok = ssl:setopts(Sock, Opts).

maybe_connect(_Name, _, [], _Port, _Options) ->
    {error, connection_refused};
maybe_connect(Name, tcp, [H | T], Port, Options) ->
    ?DEBUG_(Name, "trying to connect to ~0p on port ~b", [H, Port]),
    case gen_tcp:connect(H, Port, Options) of
        {ok, Sock} ->
            {ok, Sock, H};
        {error, Reason} ->
            ?DEBUG_(Name, "connection refused, reason: ~w host:~0p - port: ~0p",
                    [Reason, H, Port]),
            maybe_connect(Name, tcp, T, Port, Options)
    end;
maybe_connect(Name, ssl, [H | T], Port, Options) ->
    ?DEBUG_(Name, "trying to establish TLS connection to ~0p using port ~b", [H, Port]),
    Opts = Options ++
        application:get_env(osiris, replication_client_ssl_options, []) ++
        maybe_add_sni_option(H),
    case ssl:connect(H, Port, Opts) of
        {ok, Sock} ->
            {ok, Sock, H};
        {error, {tls_alert, {handshake_failure, _}}} ->
            ?DEBUG_(Name, "TLS connection refused (handshake failure), host:~0p - port: ~0p",
                    [H, Port]),
            maybe_connect(Name, ssl, T, Port, Options);
        {error, E} ->
            ?DEBUG_(Name, "TLS connection refused, host:~0p - port: ~0p", [H, Port]),
            ?DEBUG_(Name, "error while trying to establish TLS connection ~0p", [E]),
            maybe_connect(Name, ssl, T, Port, Options)
    end.

maybe_add_sni_option(H) when is_binary(H) ->
    [{server_name_indication, binary_to_list(H)}];
maybe_add_sni_option(H) when is_list(H) ->
    [{server_name_indication, H}];
maybe_add_sni_option(_) ->
    [].

