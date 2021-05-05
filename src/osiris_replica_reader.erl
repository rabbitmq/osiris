%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_replica_reader).

-behaviour(gen_server).

-include("osiris.hrl").

%% replica reader, spawned remoted by replica process, connects back to
%% configured host/port, reads entries from master and uses file:sendfile to
%% replicate read records

%% API functions
-export([start_link/1,
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
         name :: string(),
         socket :: gen_tcp:socket(),
         replica_pid :: pid(),
         leader_pid :: pid(),
         leader_monitor_ref :: reference(),
         counter :: counters:counters_ref(),
         counter_id :: term(),
         committed_offset = -1 :: -1 | osiris:offset(),
         offset_listener :: undefined | osiris:offset()}).

-define(COUNTER_FIELDS, [offset_listeners]).
-define(C_OFFSET_LISTENERS, ?C_NUM_LOG_FIELDS + 1).

-spec maybe_connect(list(), integer(), list()) ->
                       {ok, term(), term()} | {error, term()}.
maybe_connect([], _Port, _Options) ->
    {error, connection_refused};
maybe_connect([H | T], Port, Options) ->
    case gen_tcp:connect(H, Port, Options) of
        {ok, Sock} ->
            {ok, Sock, H};
        {error, _} ->
            ?WARN("osiris replica connection refused, host:~p ", [H]),
            maybe_connect(T, Port, Options)
    end.

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
       name := Name,
       replica_pid := ReplicaPid,
       leader_pid := LeaderPid,
       start_offset := {StartOffset, _} = TailInfo,
       reference := ExtRef,
       connection_token := Token} =
         Args) ->
    process_flag(trap_exit, true),

    SndBuf = 146988 * 10,

    case maybe_connect(Hosts, Port,
                       [binary, {packet, 0}, {nodelay, true}, {sndbuf, SndBuf}])
    of
        {ok, Sock, Host} ->
            CntId = {?MODULE, ExtRef, Host, Port},
            CntSpec = {CntId, ?COUNTER_FIELDS},
            %% TODO: handle errors
            {ok, Log} =
                osiris_writer:init_data_reader(LeaderPid, TailInfo, CntSpec),
            CntRef = osiris_log:counters_ref(Log),
            ?INFO("starting replica reader ~s at offset ~b Args: ~p",
                  [Name, osiris_log:next_offset(Log), Args]),

            ok = gen_tcp:send(Sock, Token),
            %% register data listener with osiris_proc
            ok = osiris_writer:register_data_listener(LeaderPid, StartOffset),
            MRef = monitor(process, LeaderPid),
            State =
                maybe_send_committed_offset(#state{log = Log,
                                                   name = Name,
                                                   socket = Sock,
                                                   replica_pid = ReplicaPid,
                                                   leader_pid = LeaderPid,
                                                   leader_monitor_ref = MRef,
                                                   counter = CntRef,
                                                   counter_id = CntId}),
            {ok, State};
        {error, Reason} ->
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
    % ?DEBUG("MORE DATA ~b", [_LastOffset]),
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
                   socket = Sock,
                   leader_monitor_ref = Ref} =
                State) ->
    %% leader is down, exit
    ?ERROR("osiris_replica_reader: '~s' detected leader down "
           "with ~W - exiting...",
           [Name, Info, 10]),
    %% this should be enough to make the replica shut down
    ok = gen_tcp:close(Sock),
    {stop, Info, State};
handle_info({tcp_closed, Socket},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG("osiris_replica_reader: '~s' Socket closed. Exiting...",
           [Name]),
    {stop, normal, State};
handle_info({tcp_error, Socket, Error},
            #state{name = Name, socket = Socket} = State) ->
    ?DEBUG("osiris_replica_reader: '~s' Socket error ~p. "
           "Exiting...",
           [Name, Error]),
    {stop, {tcp_error, Error}, State};
handle_info({'EXIT', Ref, Info}, State) ->
    ?DEBUG("osiris_replica_reader:handle_info/2: EXIT received "
           "~w, Info: ~w",
           [Ref, Info]),
    {stop, normal, State};
handle_info(Info, #state{name = Name} = State) ->
    ?DEBUG("osiris_replica_reader: '~s' unhandled message ~W",
           [Name, Info, 10]),
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

do_sendfile(#state{socket = Sock, log = Log0} = State0) ->
    State = maybe_send_committed_offset(State0),
    case osiris_log:send_file(Sock, Log0) of
        {ok, Log} ->
            do_sendfile(State#state{log = Log});
        {error, _Err} ->
            ?DEBUG("osiris_relica_reader: sendfile err ~w", [_Err]),
            State;
        {end_of_stream, Log} ->
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
