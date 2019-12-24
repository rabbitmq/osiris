-module(osiris_replica).

-behaviour(gen_server).


%% osiris replica, spaws remote reader, TCP listener
%% replicates and confirms latest offset back to primary

%% API functions
-export([start/3, start_replica/2, start_link/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {port,
                listening_socket,
                socket}).

%%%===================================================================
%%% API functions
%%%===================================================================

start(Node, Name, PortRange) ->
    %% READERS pumps data on replicas!!! replicas are the gen_tcp listeners - whch is
    %% different from this
    %% master unaware of replicas
    %% TODO if we select from a range, how do we know in the other side
    %% which one have we selected???
    rpc:call(Node, ?MODULE, start_replica, [Name, PortRange]).
 
start_replica(Name, PortRange) ->
    %% TODO What's the Id? How many replicas do we have?
    %% TODO How do we know the name of the segment to write to disk?
    %% TODO another replica for the index?
    Self = self(),
    case supervisor:start_child(osiris_sup, #{id => Name,
                                              start => {?MODULE, start_link, [Self, PortRange]},
                                              restart => transient,
                                              shutdown => 5000,
                                              type => worker,
                                              modules => [?MODULE]}) of
        {error, _} = E ->
            E;
        _ ->
            receive
                {port, Port} ->
                    Port
            end
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Caller, PortRange) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Caller, PortRange], []).

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
init([Caller, _PortRange]) ->
    Port = 5679,
    Self = self(),
    {ok, LSock} = gen_tcp:listen(Port, [binary, {packet, raw}, {active, true}]),
    Caller ! {port, Port},
    spawn_link(fun() -> accept(LSock, Self) end),
    {ok, #state{port = Port,
                listening_socket = LSock}}.

accept(LSock, Process) ->
    %% TODO what if we have more than 1 connection?
    {ok, Sock} = gen_tcp:accept(LSock),
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
handle_info({socket, Socket}, State) ->
    {noreply, State#state{socket = Socket}};
handle_info({tcp, Socket, Bin}, #state{socket = Socket} = State) ->
    ct:pal("Received ~p~n", [Bin]),
    {noreply, State};
handle_info({tcp_closed, Socket}, #state{socket = Socket} = State) ->
    ct:pal("Socket closed ~n", []),
    {noreply, State#state{socket = Socket}};
handle_info({tcp_error, Socket, Error}, #state{socket = Socket} = State) ->
    ct:pal("Socket error ~p~n", [Error]),
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
