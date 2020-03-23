%% @hidden
-module(osiris_writer).
-behaviour(gen_batch_server).

-export([start_link/1,
         start/1,
         init_data_reader/2,
         init_offset_reader/2,
         register_data_listener/2,
         register_offset_listener/2,
         ack/2,
         write/4,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1,
         stop/1,
         delete/1]).

-define(SUP, osiris_server_sup).

%% primary osiris process
%% batch writes incoming data
%% notifies replicator and reader processes of the new max index
%% manages incoming max index

-record(cfg, {name :: string(),
              ext_reference :: term(),
              offset_ref :: atomics:atomics_ref(),
              replicas = [] :: [node()],
              directory :: file:filename(),
              counter :: counters:counters_ref(),
              event_formatter :: undefined | {module(), atom(), list()}
             }).

-record(?MODULE, {cfg :: #cfg{},
                  log = osiris_log:state(),
                  pending_writes = #{} :: #{osiris:offset() =>
                                            {[node()], #{pid() => [term()]}}},
                  data_listeners = [] :: [{pid(), osiris:offset()}],
                  offset_listeners = [] :: [{pid(), osiris:offset()}],
                  committed_offset = -1 :: osiris:offset()
                 }).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

start(Config = #{name := Name,
                 leader_node := Leader}) ->
    supervisor:start_child({?SUP, Leader},
                           #{id => Name,
                             start => {?MODULE, start_link, [Config]},
                             restart => transient,
                             shutdown => 5000,
                             type => worker}).

stop(#{name := Name,
       leader_node := Leader}) ->
    _ = supervisor:terminate_child({?SUP, Leader}, Name),
    _ = supervisor:delete_child({?SUP, Leader}, Name),
    ok.

delete(#{leader_node := Leader} = Config) ->
    stop(Config),
    rpc:call(Leader, osiris_log, delete_directory, [Config]).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link(?MODULE, Config).

init_data_reader(Pid, TailInfo) when node(Pid) == node() ->
    Ctx = gen_batch_server:call(Pid, get_reader_context),
    osiris_log:init_data_reader(TailInfo, Ctx).

init_offset_reader(Pid, OffsetSpec) when node(Pid) == node() ->
    Ctx = gen_batch_server:call(Pid, get_reader_context),
    osiris_log:init_offset_reader(OffsetSpec, Ctx).

register_data_listener(Pid, Offset) ->
    ok = gen_batch_server:cast(Pid, {register_data_listener, self(), Offset}).

register_offset_listener(Pid, Offset) ->
    ok = gen_batch_server:cast(Pid, {register_offset_listener, self(), Offset}).

ack(LeaderPid, Offsets) ->
    gen_batch_server:cast(LeaderPid, {ack, node(), Offsets}).

write(Pid, Sender, Corr, Data) ->
    gen_batch_server:cast(Pid, {write, Sender, Corr, Data}).

-define(COUNTER_FIELDS,
        [chunks_written,
         offset,
         committed_offset]).
-define(C_CHUNKS_WRITTEN, 1).
-define(C_OFFSET, 2).
-define(C_COMMITTED_OFFSET, 3).

-spec init(osiris:config()) -> {ok, state()}.
init(#{name := Name,
       external_ref := ExtRef,
       replica_nodes := Replicas} = Config)
  when is_list(Name) ->
    Formatter = maps:get(event_formatter, Config, undefined),
    Dir = osiris_log:directory(Config),
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    ORef = atomics:new(1, [{signed, true}]),
    Log = osiris_log:init(Dir, Config),
    CntRef = osiris_counters:new({?MODULE, ExtRef}, ?COUNTER_FIELDS),
    LastOffs = osiris_log:next_offset(Log) -1,
    atomics:put(ORef, 1, LastOffs),
    counters:put(CntRef, ?C_OFFSET, LastOffs),
    counters:put(CntRef, ?C_COMMITTED_OFFSET, LastOffs),
    {ok, #?MODULE{cfg = #cfg{name = Name,
                             %% reference used for notification
                             %% if not provided use the name
                             ext_reference = ExtRef,

                             offset_ref = ORef,
                             replicas = Replicas,
                             directory = Dir,
                             %% TODO: there is no GC of counter registrations
                             counter = CntRef,
                             event_formatter = Formatter},

                  committed_offset = LastOffs,
                  log = Log}}.

handle_batch(Commands, #?MODULE{cfg = #cfg{counter = Cnt},
                                log = Seg0} = State0) ->

    %% filter write commands
    case handle_commands(Commands, State0, {[], [], #{}}) of
        {Entries, Replies, Corrs, State1} ->
            %% incr chunk counter
            %% TODO handle empty replicas
            State2 = case Entries of
                         [] ->
                             State1;
                         _ ->
                             ThisBatchOffs = osiris_log:next_offset(Seg0),
                             Seg = osiris_log:write(Entries, Seg0),
                             LastOffs = osiris_log:next_offset(Seg) - 1,
                             %% update written
                             counters:add(Cnt, ?C_CHUNKS_WRITTEN, 1),
                             counters:put(Cnt, ?C_OFFSET, LastOffs),
                             update_pending(ThisBatchOffs, Corrs,
                                            State1#?MODULE{log = Seg})
                     end,
            %% write to log and index files
            State = notify_data_listeners(State2),
            {ok, [garbage_collect | Replies], State};
        {stop, normal} ->
            {stop, normal}
    end.

terminate(_, #?MODULE{data_listeners = Listeners,
                      cfg = #cfg{ext_reference = ExtRef}}) ->
    ok = osiris_counters:delete({?MODULE, ExtRef}),
    [osiris_replica_reader:stop(Pid) || {Pid, _} <- Listeners],
    ok.

format_status(State) ->
    State.

%% Internal

update_pending(BatchOffs, Corrs,
               #?MODULE{cfg = #cfg{ext_reference = Ref,
                                   counter = Cnt,
                                   offset_ref = OffsRef,
                                   replicas = [],
                                   event_formatter = EventFormatter}} = State0) ->
    _ = notify_writers(Ref, Corrs, EventFormatter),
    atomics:put(OffsRef, 1, BatchOffs),
    counters:put(Cnt, ?C_COMMITTED_OFFSET, BatchOffs),
    State = State0#?MODULE{committed_offset = BatchOffs},
    notify_offset_listeners(State);
update_pending(BatchOffs, Corrs,
               #?MODULE{cfg = #cfg{replicas = Replicas},
                        pending_writes = Pending0} = State) ->
    case Corrs of
        _  when map_size(Corrs) == 0 ->
            State;
        _ ->
            State#?MODULE{pending_writes =
                          Pending0#{BatchOffs => {Replicas, Corrs}}}
    end.

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
    State1 = State0#?MODULE{offset_listeners = [{Pid, Offset} | Listeners]},
    State = notify_offset_listeners(State1),
    handle_commands(Rem, State, Acc);
handle_commands([{cast, {ack, ReplicaNode, Offsets}} | Rem],
                #?MODULE{cfg = #cfg{ext_reference = Ref,
                                    counter = Cnt,
                                    offset_ref = ORef,
                                    event_formatter = EventFormatter},
                         committed_offset = COffs0,
                         pending_writes = Pending0} = State0, Acc) ->
    % ct:pal("acks ~w", [Offsets]),

    {COffs, Pending} =
    lists:foldl(fun (Offset, {C0, P0}) ->
                        case maps:get(Offset, P0) of
                            {[ReplicaNode], Corrs} ->
                                _ = notify_writers(Ref, Corrs, EventFormatter),
                                atomics:put(ORef, 1, Offset),
                                {Offset, maps:remove(Offset, P0)};
                            {Reps, Corrs} ->
                                Reps1 = lists:delete(ReplicaNode, Reps),
                                {C0, maps:update(Offset,
                                                 {Reps1, Corrs},
                                                 P0)}
                        end
                end, {COffs0, Pending0}, Offsets),
    % ct:pal("acks after ~w ~W", [COffs, Pending, 5]),
    counters:put(Cnt, ?C_COMMITTED_OFFSET, COffs),
    State1 = State0#?MODULE{pending_writes = Pending,
                           committed_offset = COffs},
    %% if committed offset has incresed - update 
    State = case COffs > COffs0 of
                true ->
                    notify_offset_listeners(State1);
                false ->
                    State1
            end,
    handle_commands(Rem, State, Acc);
handle_commands([{call, From, get_reader_context} | Rem],
                #?MODULE{cfg = #cfg{offset_ref = ORef,
                                    directory = Dir},
                         committed_offset = COffs} = State,
                {Records, Replies, Corrs}) ->
    Reply = {reply, From, #{dir => Dir,
                            committed_offset => max(0, COffs),
                            offset_ref => ORef}},
    handle_commands(Rem, State, {Records, [Reply | Replies], Corrs});
handle_commands([osiris_stop | _Rem], _State, _Acc) ->
    {stop, normal};
handle_commands([_Unk | Rem], State, Acc) ->
    error_logger:info_msg("osiris_writer unknown command ~w", [_Unk]),
    handle_commands(Rem, State, Acc).


notify_data_listeners(#?MODULE{log = Seg,
                               data_listeners = L0} = State) ->
    NextOffset = osiris_log:next_offset(Seg),
    {Notify, L} = lists:splitwith(fun ({_Pid, O}) -> O < NextOffset end, L0),
    [gen_server:cast(P, {more_data, NextOffset}) || {P, _} <- Notify],
    State#?MODULE{data_listeners = L}.

notify_offset_listeners(#?MODULE{cfg = #cfg{ext_reference = Ref},
                                 committed_offset = COffs,
                                 offset_listeners = L0} = State) ->
    {Notify, L} = lists:splitwith(fun ({_Pid, O}) -> O =< COffs end, L0),
    [P ! {osiris_offset, Ref, COffs} || {P, _} <- Notify],
    State#?MODULE{offset_listeners = L}.

% notify_offset_listeners(#?MODULE{cfg = #cfg{ext_reference = Ref},
%                                  committed_offset = COffs,
%                                  % log = Seg,
%                                  offset_listeners = L0}) ->
%     [begin
%          % Next = osiris_log:next_offset(Seg),
%          % error_logger:info_msg("osiris_writer offset listener ~w CO: ~w Next ~w LO: ~w",
%          %                       [P, COffs, Next, O]),
%          P ! {osiris_offset, Ref, COffs}
%      end || P <- L0],
%     ok.

notify_writers(Name, Corrs, EventFormatter) ->
    maps:map(
      fun (P, V) ->
              P ! wrap_osiris_event(EventFormatter, Name, lists:reverse(V))
      end, Corrs).

wrap_osiris_event(undefined, Name, Value) ->
    {osiris_written, Name, Value};
wrap_osiris_event({M, F, A}, Name, Value) ->
    apply(M, F, [Name, Value | A]).
