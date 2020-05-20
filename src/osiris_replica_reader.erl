-module(osiris_replica_reader).
-behaviour(gen_server).

-include("osiris.hrl").


%% replica reader, spawned remoted by replica process, connects back to
%% configured host/port, reads entries from master and uses file:sendfile to
%% replicate read records

%% API functions
-export([start_link/1, stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([formatter/1]).

-record(state, {log :: osiris_log:state(),
                socket :: gen_tcp:socket(),
                replica_pid :: pid(),
                leader_pid :: pid(),
                counter :: counters:counters_ref(),
                counter_id :: term(),
                committed_offset = -1 :: -1 | osiris:offset()}).

-define(COUNTER_FIELDS,
        [chunks_sent,
         offset,
         offset_listeners
        ]).
-define(C_CHUNKS_SENT, 1).
-define(C_OFFSET, 2).
-define(C_OFFSET_LISTENERS, 3).

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
init(#{host := Host,
       port := Port,
       replica_pid := ReplicaPid,
       leader_pid := LeaderPid,
       start_offset := {StartOffset, _} = TailInfo,
       external_ref := ExtRef} = Args) ->
    CntId = {?MODULE, ExtRef, Host, Port},
    CntRef = osiris_counters:new(CntId, ?COUNTER_FIELDS),
    %% TODO: handle errors
    {ok, Log} = osiris_writer:init_data_reader(LeaderPid, TailInfo),
    ?INFO("starting replica reader with ~w NextOffs ~b~n",
          [Args, osiris_log:next_offset(Log)]),
    SndBuf = 146988 * 10,
    {ok, Sock} = gen_tcp:connect(Host, Port, [binary,
                                              {packet, 0},
                                              {nodelay, true},
                                              {sndbuf, SndBuf}]),
    %% register data listener with osiris_proc
    ok = osiris_writer:register_data_listener(LeaderPid, StartOffset),
    State = maybe_send_committed_offset(#state{log = Log,
                                               socket = Sock,
                                               replica_pid = ReplicaPid,
                                               leader_pid = LeaderPid,
                                               counter = CntRef,
                                               counter_id = CntId}),
    {ok, State}.

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
            #state{log = Log0,
                   leader_pid = LeaderPid,
                   socket = Sock,
                   committed_offset = Last,
                   counter = Cnt} = State0) ->
    % ?DEBUG("MORE DATA ~b", [_LastOffset]),
    #state{log = Log} = State = do_sendfile(Sock, State0, Log0),
    NextOffs = osiris_log:next_offset(Log),
    ok = osiris_writer:register_data_listener(LeaderPid, NextOffs),
    ok = osiris:register_offset_listener(LeaderPid, Last + 1,
                                         {?MODULE, formatter, []}),
    ok = counters:add(Cnt, ?C_OFFSET_LISTENERS, 1),
    {noreply, State};
handle_cast(stop, State) ->
    {stop, normal, State}.


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
handle_info({osiris_offset, _, _Offs},
            #state{leader_pid = LeaderPid,
                   committed_offset = Last} = State0) ->
    State = maybe_send_committed_offset(State0),
    ok = osiris:register_offset_listener(LeaderPid, Last + 1,
                                         {?MODULE, formatter, []}),
    {noreply, State};
handle_info(_Info, State) ->
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
terminate(_Reason, #state{counter_id = CntId}) ->
    ok = osiris_counters:delete(CntId),
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

do_sendfile(Sock, #state{counter = Cnt} = State0, Log0) ->
    case osiris_log:send_file(Sock, Log0) of
        {ok, Log} ->
            Offset = osiris_log:next_offset(Log) - 1,
            ok = counters:add(Cnt, ?C_CHUNKS_SENT, 1),
            ok = counters:put(Cnt, ?C_OFFSET, Offset),
            State = maybe_send_committed_offset(State0),
            do_sendfile(Sock, State, Log);
        {end_of_stream, Log} ->
            maybe_send_committed_offset(State0#state{log = Log})
    end.

maybe_send_committed_offset(#state{log = Log,
                                   committed_offset = Last,
                                   replica_pid = RPid} = State) ->
    COffs = osiris_log:committed_offset(Log),
    case COffs of
        COffs when COffs > Last ->
            ok = erlang:send(RPid, {'$gen_cast', {committed_offset, COffs}},
                             [noconnect, nosuspend]),
            State#state{committed_offset = COffs};
        _ ->
            State
    end.

formatter(Evt) ->
    Evt.
