-module(osiris_replica).

-behaviour(gen_server).


%% osiris replica, spaws remote reader, TCP listener
%% replicates and confirms latest offset back to primary

%% API functions
-export([start/3, start_link/1]).
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
              socket :: undefined | gen_tcp:socket()
             }).

-record(?MODULE, {cfg :: #cfg{},
                  segment :: osiris_segment:state()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

%%%===================================================================
%%% API functions
%%%===================================================================

start(Node, Name, LeaderPid) ->
    %% READERS pumps data on replicas!!! replicas are the gen_tcp listeners - whch is
    %% different from this
    %% master unaware of replicas
    %% TODO if we select from a range, how do we know in the other side
    %% which one have we selected???
    %% TODO What's the Id? How many replicas do we have?
    %% TODO How do we know the name of the segment to write to disk?
    %%` TODO another replica for the index?
    supervisor:start_child({osiris_replica_sup, Node},
                           #{id => Name,
                             start => {?MODULE, start_link,
                                       [LeaderPid]},
                             restart => transient,
                             shutdown => 5000,
                             type => worker,
                             modules => [?MODULE]}) .

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(LeaderPid) ->
    gen_server:start_link(?MODULE, [LeaderPid], []).

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
init([LeaderPid]) ->
    {ok, {Min, Max}} = application:get_env(port_range),
    %% TODO: use locally configured port range
    {Port, LSock} = open_tcp_port(Min, Max),
    Self = self(),
    spawn_link(fun() -> accept(LSock, Self) end),

    {ok, Dir} = application:get_env(data_dir),
    Segment = osiris_segment:init(Dir, #{}),
    NextOffset = osiris_segment:next_offset(Segment),
    %% spawn reader process on leader node
    {ok, HostName} = inet:gethostname(),
    {ok, Ip} = inet:getaddr(HostName, inet),
    Node = node(LeaderPid),
    case supervisor:start_child({osiris_replica_reader_sup, Node},
                                #{
                                  id => make_ref(),
                                  start => {osiris_replica_reader, start_link,
                                            [Ip, Port, LeaderPid, NextOffset]},
                                  restart => transient,
                                  shutdown => 5000,
                                  type => worker,
                                  modules => [osiris_replica_reader]}) of
        {ok, _} ->
            ok;
        {ok, _, _} ->
            ok
    end,
    %% TODO: monitor leader pid
    {ok, #?MODULE{cfg = #cfg{leader_pid = LeaderPid,
                             directory = Dir,
                             port = Port,
                             listening_socket = LSock},
                  segment = Segment}}.


open_tcp_port(M, M) ->
    throw({error, all_busy});
open_tcp_port(Min, Max) ->
    case gen_tcp:listen(Min, [binary, {packet, raw}, {active, true}]) of
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
handle_info({socket, Socket}, #?MODULE{cfg = Cfg} = State) ->

    {noreply, State#?MODULE{cfg = Cfg#cfg{socket = Socket}}};
handle_info({tcp, Socket, Bin},
            #?MODULE{cfg = #cfg{socket = Socket,
                                leader_pid = LeaderPid},
                     segment = Segment0} = State) ->
    %% validate chunk
    FirstOffset = parse_chunk(Bin),
    Segment = osiris_segment:accept_chunk(Bin, Segment0),
    ok = osiris_writer:ack(LeaderPid, FirstOffset),
    {noreply, State#?MODULE{segment = Segment}};
handle_info({tcp_closed, Socket}, #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
    ct:pal("Socket closed ~n", []),
    {noreply, State};
handle_info({tcp_error, Socket, Error},
            #?MODULE{cfg = #cfg{socket = Socket}} = State) ->
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

parse_chunk(<<"CHNK",
              FirstOffset:64/unsigned,
              _NumRecords:32/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              _Record/binary>> = Chunk) ->
    true = byte_size(Chunk) == (Size + 24),
    FirstOffset.
