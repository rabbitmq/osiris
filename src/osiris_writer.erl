%% @hidden
-module(osiris_writer).
-behaviour(gen_batch_server).

-export([start_link/1,
         start/2,
         init_reader/2,
         register_data_listener/2,
         register_offset_listener/2,
         ack/2,
         write/4,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1,
         stop/1
        ]).

%% primary osiris process
%% batch writes incoming data
%% notifies replicator and reader processes of the new max index
%% manages incoming max index

-record(?MODULE, {name :: string(),
                  reference :: term(),
                  offset_ref :: atomics:atomics_ref(),
                  replicas = [] :: [node()],
                  directory :: file:filename(),
                  segment = osiris_segment:state(),
                  pending_writes = #{} :: #{osiris_segment:offset() =>
                                            {[node()], #{pid() => [term()]}}},
                  data_listeners = [] :: [{pid(), osiris_segment:offset()}],
                  offset_listeners = [] :: [{pid(), osiris_segment:offset()}],
                  committed_offset = -1 :: osiris_segment:offset()
                 }).

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

stop(Name) ->
    ok = supervisor:terminate_child(osiris_writer_sup, Name),
    ok = supervisor:delete_child(osiris_writer_sup, Name).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link(?MODULE, Config).


init_reader(Pid, StartOffset) when node(Pid) == node() ->
    Ctx = gen_batch_server:call(Pid, get_reader_context),
    osiris_segment:init_reader(StartOffset, Ctx).

register_data_listener(Pid, Offset) ->
    ok = gen_batch_server:cast(Pid, {register_data_listener, self(), Offset}).

register_offset_listener(Pid, Offset) ->
    ok = gen_batch_server:cast(Pid, {register_offset_listener, self(), Offset}).

ack(LeaderPid, Offset) ->
    gen_batch_server:cast(LeaderPid, {ack, node(), Offset}).

write(Pid, Sender, Corr, Data) ->
    gen_batch_server:cast(Pid, {write, Sender, Corr, Data}).

-spec init(map()) -> {ok, state()}.
init(#{name := Name,
       replica_nodes := Replicas} = Config) ->
    Dir0 = case Config of
              #{dir := D} -> D;
              _ ->
                  {ok, D} = application:get_env(data_dir),
                  D
          end,
    Dir = filename:join(Dir0, Name),
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> ok;
        E -> throw(E)
    end,
    ORef = atomics:new(1, []),
    Segment = osiris_segment:init(Dir, Config),
    {ok, #?MODULE{name = Name,
                  %% reference used for notification
                  %% if not provided use the name
                  reference = maps:get(reference, Config, Name),
                  offset_ref = ORef,
                  replicas = Replicas,
                  directory  = Dir,
                  segment = Segment}}.

handle_batch(Commands, #?MODULE{segment = Seg0} = State0) ->

    %% filter write commands
    {Records, Replies, Corrs, State1} = handle_commands(Commands, State0,
                                                        {[], [], #{}}),
    %% TODO handle empty replicas
    State2 = case Records of
                 [] ->
                     State1;
                 _ ->
                     Next = osiris_segment:next_offset(Seg0),
                     Seg = osiris_segment:write(Records, Seg0),
                     update_pending(Next, Corrs, State1#?MODULE{segment = Seg})
             end,
    %% write to log and index files
    State = notify_offset_listeners(
              notify_data_listeners(State2)),
    {ok, Replies, State}.

terminate(_, #?MODULE{data_listeners = Listeners}) ->
    [osiris_replica_reader:stop(Pid) || {Pid, _} <- Listeners],
    ok.

format_status(State) ->
    State.

%% Internal

update_pending(Next, Corrs, #?MODULE{reference = Ref,
                                     offset_ref = OffsRef,
                                     replicas = []} = State) ->
    _ = notify_writers(Ref, Corrs),
    atomics:put(OffsRef, 1, Next),
    State#?MODULE{committed_offset = Next};
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
handle_commands([{cast, {register_offset_listener, Pid, Offset}} | Rem],
                #?MODULE{offset_listeners = Listeners} = State0, Acc) ->
    State = State0#?MODULE{offset_listeners = [{Pid, Offset} | Listeners]},
    handle_commands(Rem, State, Acc);
handle_commands([{cast, {ack, ReplicaNode, Offset}} | Rem],
                #?MODULE{reference = Ref,
                         committed_offset = COffs0,
                         offset_ref = ORef,
                         pending_writes = Pending0} = State0, Acc) ->
    {COffs, Pending} = case maps:get(Offset, Pending0) of
                           {[ReplicaNode], Corrs} ->
                               _ = notify_writers(Ref, Corrs),
                               atomics:put(ORef, 1, Offset),
                               {Offset, maps:remove(Offset, Pending0)};
                           {Reps, Corrs} ->
                               Reps1 = lists:delete(ReplicaNode, Reps),
                               {COffs0, maps:update(Offset,
                                                    {Reps1, Corrs},
                                                    Pending0)}
                       end,
    State = State0#?MODULE{pending_writes = Pending,
                           committed_offset = COffs},
    handle_commands(Rem, State, Acc);
handle_commands([{call, From, get_reader_context} | Rem],
                #?MODULE{offset_ref = ORef,
                         committed_offset = COffs,
                         directory = Dir} = State, {Records, Replies, Corrs}) ->
    Reply = {reply, From, #{dir => Dir,
                            committed_offset => max(0, COffs),
                            offset_ref => ORef}},
    handle_commands(Rem, State, {Records, [Reply | Replies], Corrs});
handle_commands([_Unk | Rem], State, Acc) ->
    error_logger:info_msg("osiris_writer unknown command ~w", [_Unk]),
    handle_commands(Rem, State, Acc).


notify_data_listeners(#?MODULE{segment = Seg,
                               data_listeners = L0} = State) ->
    LastOffset = osiris_segment:next_offset(Seg) - 1,
    {Notify, L} = lists:splitwith(fun ({_Pid, O}) ->
                                          O < LastOffset
                                  end, L0),
    [gen_server:cast(P, {more_data, LastOffset})
     || {P, _} <- Notify],
    State#?MODULE{data_listeners = L}.

notify_offset_listeners(#?MODULE{reference = Ref,
                                 committed_offset = COffs,
                                 segment = Seg,
                                 offset_listeners = L0} = State) ->
    {Notify, L} = lists:splitwith(fun ({_Pid, O}) ->
                                          O =< COffs
                                  end, L0),
    [begin
         Next = osiris_segment:next_offset(Seg),
         error_logger:info_msg("osiris_writer offset listner ~w CO: ~w Next ~w LO: ~w",
                               [P, COffs, Next, O]),
         P ! {osiris_offset, Ref, COffs}
     end || {P, O} <- Notify],
    State#?MODULE{offset_listeners = L}.
