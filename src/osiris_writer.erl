%% @hidden
-module(osiris_writer).
-behaviour(gen_batch_server).

-export([start_link/1,
         start/2,
         init_reader/2,
         register_data_listener/2,
         ack/2,
         write/4,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

%% primary osiris process
%% batch writes incoming data
%% notifies replicator and reader processes of the new max index
%% manages incoming max index

-record(?MODULE, {name :: string(),
                  replicas :: [node()],
                  directory :: file:filename(),
                  segment = osiris_segment:state(),
                  pending_writes = #{} :: #{osiris_segment:offset() =>
                                            {[node()], #{pid() => [term()]}}},
                  data_listeners = [] :: [{pid(), osiris_segment:offset()}]}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

start(Name, Config0) ->
    Config = Config0#{name => Name},
    supervisor:start_child(osiris_writer_sup,
                           #{id => Name,
                             start => {?MODULE, start_link, [Config]},
                             restart => transient,
                             shutdown => 5000,
                             type => worker}).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link(?MODULE, Config).


init_reader(Pid, StartOffset) when node(Pid) == node() ->
    Dir = gen_batch_server:call(Pid, get_directory),
    osiris_segment:init_reader(StartOffset, Dir, #{}).

register_data_listener(Pid, Offset) ->
    ok = gen_batch_server:cast(Pid, {register_data_listener, self(), Offset}).

ack(LeaderPid, Offset) ->
    gen_batch_server:cast(LeaderPid, {ack, node(), Offset}).

write(Pid, Sender, Corr, Data) ->
    gen_batch_server:cast(Pid, {write, Sender, Corr, Data}).

-spec init(map()) -> {ok, state()}.
init(#{name := Name,
       replica_nodes := Replicas}) ->
    {ok, DataDir} = application:get_env(data_dir),
    Dir = filename:join(DataDir, Name),
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> ok;
        E -> throw(E)
    end,
    Segment = osiris_segment:init(Dir, #{}),
    {ok, #?MODULE{name = Name,
                  replicas = Replicas,
                  directory  = Dir,
                  segment = Segment}}.

handle_batch(Commands, #?MODULE{segment = Seg0} = State0) ->

    %% filter write commands
    {Records, Replies, Corrs, State1} = handle_commands(Commands, State0,
                                                        {[], [], #{}}),
    %% TODO handle empty replicas
    Next = osiris_segment:next_offset(Seg0),
    Seg = osiris_segment:write(Records, Seg0),
    State2 = update_pending(Next, Corrs, State1),
    %% write to log and index files
    State = notify_listeners(State2#?MODULE{segment = Seg}),
    {ok, Replies, State}.

terminate(_, #?MODULE{}) ->
    ok.

format_status(State) ->
    State.

%% Internal


update_pending(_Next, Corrs, #?MODULE{name = Name,
                                      replicas = []} = State) ->
    _ = notify_writers(Name, Corrs),
    State;
update_pending(Next, Corrs, #?MODULE{replicas = Replicas,
                                     pending_writes = Pending0} = State) ->
    case Corrs of
        _  when map_size(Corrs) == 0 ->
            State;
        _ ->
            State#?MODULE{pending_writes =
                          Pending0#{Next => {Replicas, Corrs}}}
    end.

notify_writers(Name, Corrs) ->
    maps:map(
      fun (P, V) ->
              P ! {osiris_written, Name, lists:reverse(V)}
      end, Corrs).

handle_commands([], State, {Records, Replies, Corrs}) ->
    {lists:reverse(Records), Replies, Corrs, State};
handle_commands([{cast, {write, Pid, Corr, R}} | Rem], State,
                {Records, Replies, Corrs0}) ->
    Corrs = maps:update_with(Pid, fun (C) -> [Corr |  C] end,
                             [Corr], Corrs0),
    handle_commands(Rem, State, {[R | Records], Replies, Corrs});
handle_commands([{cast, {register_data_listener, Pid, Offset}} | Rem],
                #?MODULE{data_listeners = Listeners} = State0, Acc) ->
    State = State0#?MODULE{data_listeners = [{Pid, Offset} | Listeners]},
    handle_commands(Rem, State, Acc);
handle_commands([{cast, {ack, ReplicaNode, Offset}} | Rem],
                #?MODULE{name = Name,
                         pending_writes = Pending0} = State0, Acc) ->
    Pending = case maps:get(Offset, Pending0) of
                  {[ReplicaNode], Corrs} ->
                      ct:pal("last ack ~w", [Offset]),
                      _ = notify_writers(Name, Corrs),
                      maps:remove(Offset, Pending0);
                  {Reps, Corrs} ->
                      Reps1 = lists:delete(ReplicaNode, Reps),
                      maps:update(Offset,
                                  {Reps1, Corrs},
                                  Pending0)
              end,
    ct:pal("ack ~w ~w ~w ~w", [self(), ReplicaNode, Offset, Pending]),
    State = State0#?MODULE{pending_writes = Pending},
    handle_commands(Rem, State, Acc);
handle_commands([{call, From, get_directory} | Rem],
                #?MODULE{directory = Dir} = State, {Records, Replies, Corrs}) ->
    Reply = {reply, From, Dir},
    handle_commands(Rem, State, {Records, [Reply | Replies], Corrs}).


notify_listeners(#?MODULE{segment = Seg,
                          data_listeners = L0} = State) ->
    LastOffset = osiris_segment:next_offset(Seg) - 1,
    {Notify, L} = lists:splitwith(fun ({_Pid, O}) ->
                                          O < LastOffset
                                  end, L0),
    [gen_server:cast(P, {more_data, LastOffset})
     || {P, _} <- Notify],
    State#?MODULE{data_listeners = L}.

