-module(osiris_replica_reader).

-behaviour(gen_server).


%% replica reader, spawned remoted by replica process, connects back to
%% configured host/port, reads entries from master and uses file:sendfile to
%% replicate read records

%% API functions
-export([start_link/4, stop/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {segment :: osiris_segment:state(),
                socket :: gen_tcp:socket(),
                leader_pid :: pid()}).

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
start_link(Host, Port, LeaderPid, StartOffset) ->
    gen_server:start_link(?MODULE, [Host, Port, LeaderPid, StartOffset], []).

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
init([Host, Port, LeaderPid, StartOffset] = Args) ->
    Segment = osiris_writer:init_reader(LeaderPid, StartOffset),
    error_logger:info_msg("starting replica reader with ~w NextOffs ~b~n",
                          [Args, osiris_segment:next_offset(Segment)]),
    SndBuf = 146988 * 10,
    {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 0},
                                              {nodelay, true},
                                              {sndbuf, SndBuf}]),
    error_logger:info_msg("gen tcp opts snd buf~p",
                          [inet:getopts(Sock, [sndbuf])]),
    %% register data listener with osiris_proc
    ok = osiris_writer:register_data_listener(LeaderPid, StartOffset -1),
    {ok, #state{segment = Segment,
                socket = Sock,
                leader_pid = LeaderPid}}.

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
            #state{segment = Seg0,
                   leader_pid = LeaderPid,
                   socket = Sock} = State) ->
    {ok, Seg} = do_sendfile(Sock, Seg0),
    LastOffset = osiris_segment:next_offset(Seg) - 1,
    % error_logger:info_msg("replicate reader listen for ~b", [LastOffset]),
    ok = osiris_writer:register_data_listener(LeaderPid, LastOffset),
    {noreply, State#state{segment = Seg}};
handle_cast(stop, State) ->
    {stop, normal, State}.

do_sendfile(Sock, Seg0) ->
    case osiris_segment:send_file(Sock, Seg0) of
        {ok, Seg} ->
            % error_logger:info_msg("~w replicate reader next offs ~b",
            %                       [self(), osiris_segment:next_offset(Seg)]),
            do_sendfile(Sock, Seg);
        {end_of_stream, Seg} ->
            {ok, Seg}
    end.


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
terminate(_Reason, _State) ->
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
