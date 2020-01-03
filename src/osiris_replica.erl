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

-type parse_state() :: undefined | binary() | {iolist(), non_neg_integer()}.

-record(?MODULE, {cfg :: #cfg{},
                  parse_state :: parse_state(),
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
                     parse_state = ParseState0,
                     segment = Segment0} = State) ->
    %% validate chunk
    {ParseState, OffsetChunks} = parse_chunk(Bin, ParseState0, []),
    Segment = lists:foldl(
                fun({FirstOffset, B}, Acc0) ->
                        Acc = osiris_segment:accept_chunk(B, Acc0),
                        ok = osiris_writer:ack(LeaderPid, FirstOffset),
                        Acc
                end, Segment0, OffsetChunks),
    {noreply, State#?MODULE{segment = Segment,
                            parse_state = ParseState}};
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

parse_chunk(<<>>, ParseState, Acc) ->
    {ParseState, lists:reverse(Acc)};
parse_chunk(<< "CHNK",
               FirstOffset:64/unsigned,
               _NumRecords:32/unsigned,
               _Crc:32/integer,
               Size:32/unsigned,
               _Record:Size/binary,
               Rem/binary>> = All, undefined, Acc) ->
    % true = byte_size(Chunk) == (Size + 24),
    TotalSize = Size + 24,
    <<Chunk:TotalSize/binary, _/binary>> = All,
    parse_chunk(Rem, undefined, [{FirstOffset, Chunk} | Acc]);
parse_chunk(Bin, undefined, Acc)
  when byte_size(Bin) =< 24 ->
    {Bin, lists:reverse(Acc)};
parse_chunk(<<"CHNK",
              FirstOffset:64/unsigned,
              _NumRecords:32/unsigned,
              _Crc:32/integer,
              Size:32/unsigned,
              Partial/binary>> = All, undefined, Acc) ->
    {{FirstOffset, [All], Size - byte_size(Partial)},
     Acc};
parse_chunk(Bin, PartialHeaderBin, Acc)
  when is_binary( PartialHeaderBin) ->
    %% TODO: slight inneficiency but partial headers should be relatively
    %% rare
    parse_chunk(<<PartialHeaderBin/binary, Bin/binary>>, undefined, Acc);
parse_chunk(Bin, {FirstOffset, IOData, RemSize}, Acc)
  when byte_size(Bin) >= RemSize ->
    <<Final:RemSize/binary, Rem/binary>> = Bin,
    parse_chunk(Rem, undefined,
                [{FirstOffset, lists:reverse([Final | IOData])} | Acc]);
parse_chunk(Bin, {FirstOffset, IOData, RemSize}, Acc) ->
    {{FirstOffset, [Bin | IOData], RemSize - byte_size(Bin)}, Acc}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_chunk_test() ->
    Bin = <<67,72,78,75,0,0,0,0,0,0,0,147,0,0,0,33,0,0,0,0,
            0,0,2,148,0,0,0,8,0,0,0,0,0,0,0,179,0,0,0,0,0,0,
            3,85,0,0,0,8,0,0,0,0,0,0,0,178,0,0,0,0,0,0,3,84,
            0,0,0,8,0,0,0,0,0,0,0,177,0,0,0,0,0,0,3,83,0,0,
            0,8,0,0,0,0,0,0,0,176,0,0,0,0,0,0,3,82,0,0,0,8,
            0,0,0,0,0,0,0,175,0,0,0,0,0,0,3,81,0,0,0,8,0,0,
            0,0,0,0,0,174,0,0,0,0,0,0,3,80,0,0,0,8,0,0,0,0,
            0,0,0,173,0,0,0,0,0,0,3,79,0,0,0,8,0,0,0,0,0,0,
            0,172,0,0,0,0,0,0,3,78,0,0,0,8,0,0,0,0,0,0,0,
            171,0,0,0,0,0,0,3,77,0,0,0,8,0,0,0,0,0,0,0,170,
            0,0,0,0,0,0,3,76,0,0,0,8,0,0,0,0,0,0,0,169,0,0,
            0,0,0,0,3,75,0,0,0,8,0,0,0,0,0,0,0,168,0,0,0,0,
            0,0,3,74,0,0,0,8,0,0,0,0,0,0,0,167,0,0,0,0,0,0,
            3,73,0,0,0,8,0,0,0,0,0,0,0,166,0,0,0,0,0,0,3,72,
            0,0,0,8,0,0,0,0,0,0,0,165,0,0,0,0,0,0,3,71,0,0,
            0,8,0,0,0,0,0,0,0,164,0,0,0,0,0,0,3,70,0,0,0,8,
            0,0,0,0,0,0,0,163,0,0,0,0,0,0,3,69,0,0,0,8,0,0,
            0,0,0,0,0,162,0,0,0,0,0,0,3,68,0,0,0,8,0,0,0,0,
            0,0,0,161,0,0,0,0,0,0,3,67,0,0,0,8,0,0,0,0,0,0,
            0,160,0,0,0,0,0,0,3,66,0,0,0,8,0,0,0,0,0,0,0,
            159,0,0,0,0,0,0,3,65,0,0,0,8,0,0,0,0,0,0,0,158,
            0,0,0,0,0,0,3,64,0,0,0,8,0,0,0,0,0,0,0,157,0,0,
            0,0,0,0,3,63,0,0,0,8,0,0,0,0,0,0,0,156,0,0,0,0,
            0,0,3,62,0,0,0,8,0,0,0,0,0,0,0,155,0,0,0,0,0,0,
            3,61,0,0,0,8,0,0,0,0,0,0,0,154,0,0,0,0,0,0,3,60,
            0,0,0,8,0,0,0,0,0,0,0,153,0,0,0,0,0,0,3,59,0,0,
            0,8,0,0,0,0,0,0,0,152,0,0,0,0,0,0,3,58,0,0,0,8,
            0,0,0,0,0,0,0,151,0,0,0,0,0,0,3,57,0,0,0,8,0,0,
            0,0,0,0,0,150,0,0,0,0,0,0,3,56,0,0,0,8,0,0,0,0,
            0,0,0,149,0,0,0,0,0,0,3,55,0,0,0,8,0,0,0,0,0,0,
            0,148,0,0,0,0,0,0,3,54,0,0,0,8,0,0,0,0,0,0,0,
            147,0,0,0,0,0,0,3,53,67,72,78,75,0,0,0,0,0,0,0,
            180,0,0,0,38,0,0,0,0,0,0,2,248,0,0,0,8,0,0,0,0,
            0,0,0,217,0,0,0,0,0,0,3,52,0,0,0,8,0,0,0,0,0,0,
            0,216,0,0,0,0,0,0,3,51,0,0,0,8,0,0,0,0,0,0,0,
            215,0,0,0,0,0,0,3,50,0,0,0,8,0,0,0,0,0,0,0,214,
            0,0,0,0,0,0,3,49,0,0,0,8,0,0,0,0,0,0,0,213,0,0,
            0,0,0,0,3,48,0,0,0,8,0,0,0,0,0,0,0,212,0,0,0,0,
            0,0,3,47,0,0,0,8,0,0,0,0,0,0,0,211,0,0,0,0,0,0,
            3,46,0,0,0,8,0,0,0,0,0,0,0,210,0,0,0,0,0,0,3,45,
            0,0,0,8,0,0,0,0,0,0,0,209,0,0,0,0,0,0,3,44,0,0,
            0,8,0,0,0,0,0,0,0,208,0,0,0,0,0,0,3,43,0,0,0,8,
            0,0,0,0,0,0,0,207,0,0,0,0,0,0,3,42,0,0,0,8,0,0,
            0,0,0,0,0,206,0,0,0,0,0,0,3,41,0,0,0,8,0,0,0,0,
            0,0,0,205,0,0,0,0,0,0,3,40,0,0,0,8,0,0,0,0,0,0,
            0,204,0,0,0,0,0,0,3,39,0,0,0,8,0,0,0,0,0,0,0,
            203,0,0,0,0,0,0,3,38,0,0,0,8,0,0,0,0,0,0,0,202,
            0,0,0,0,0,0,3,37,0,0,0,8,0,0,0,0>>,

    {P, _} = parse_chunk(Bin, undefined, []),
    Next = <<0,0,0,201,0,0,0,0,0,0,3,36,0,0,0,8,0,0,0,0,0,0,0,200,
             0,0,0,0,0,0,3,35,0,0,0,8,0,0,0,0,0,0,0,199,0,0,0,0,0,
             0,3,34,0,0,0,8,0,0,0,0,0,0,0,198,0,0,0,0,0,0,3,33,0,
             0,0,8,0,0,0,0,0,0,0,197,0,0,0,0,0,0,3,32,0,0,0,8,0,0,
             0,0,0,0,0,196,0,0,0,0,0,0,3,31,0,0,0,8,0,0,0,0,0,0,0,
             195,0,0,0,0,0,0,3,30,0,0,0,8,0,0,0,0,0,0,0,194,0,0,0,
             0,0,0,3,29,0,0,0,8,0,0,0,0,0,0,0,193,0,0,0,0,0,0,3,
             28,0,0,0,8,0,0,0,0,0,0,0,192,0,0,0,0,0,0,3,27,0,0,0,
             8,0,0,0,0,0,0,0,191,0,0,0,0,0,0,3,26,0,0,0,8,0,0,0,0,
             0,0,0,190,0,0,0,0,0,0,3,25,0,0,0,8,0,0,0,0,0,0,0,189,
             0,0,0,0,0,0,3,24,0,0,0,8,0,0,0,0,0,0,0,188,0,0,0,0,0,
             0,3,23,0,0,0,8,0,0,0,0,0,0,0,187,0,0,0,0,0,0,3,22,0,
             0,0,8,0,0,0,0,0,0,0,186,0,0,0,0,0,0,3,21,0,0,0,8,0,0,
             0,0,0,0,0,185,0,0,0,0,0,0,3,20,0,0,0,8,0,0,0,0,0,0,0,
             184,0,0,0,0,0,0,3,19,0,0,0,8,0,0,0,0,0,0,0,183,0,0,0,
             0,0,0,3,18,0,0,0,8,0,0,0,0,0,0,0,182,0,0,0,0,0,0,3,
             17,0,0,0,8,0,0,0,0,0,0,0,181,0,0,0,0,0,0,3,16,0,0,0,
             8,0,0,0,0,0,0,0,180,0,0,0,0,0,0,3,15>>,
    Res = parse_chunk(Next, P, []),
    ?debugFmt("~p", [Res]),
    ok.

-endif.
