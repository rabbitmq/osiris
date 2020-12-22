%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @hidden
-module(osiris_writer).

-behaviour(gen_batch_server).

-include("osiris.hrl").

-export([start_link/1,
         start/1,
         overview/1,
         init_data_reader/2,
         register_data_listener/2,
         ack/2,
         write/5,
         write_tracking/3,
         read_tracking/2,
         query_writers/2,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1,
         stop/1,
         delete/1]).

-define(SUP, osiris_server_sup).
-define(ADD_COUNTER_FIELDS, [committed_offset]).
-define(C_COMMITTED_OFFSET, ?C_NUM_LOG_FIELDS + 1).

%% primary osiris process
%% batch writes incoming data
%% notifies replicator and reader processes of the new max index
%% manages incoming max index

-record(cfg,
        {name :: string(),
         ext_reference :: term(),
         offset_ref :: atomics:atomics_ref(),
         replicas = [] :: [node()],
         directory :: file:filename(),
         counter :: counters:counters_ref(),
         event_formatter :: undefined | mfa()}).
-record(?MODULE,
        {cfg :: #cfg{},
         log = osiris_log:state(),
         replica_state = #{} :: #{node() => osiris:offset()},
         pending_corrs = queue:new() :: queue:queue(),
         duplicates = [] ::
             [{osiris:offset(), pid(), osiris:writer_id(), non_neg_integer()}],
         data_listeners = [] :: [{pid(), osiris:offset()}],
         offset_listeners = [] ::
             [{pid(), osiris:offset(), mfa() | undefined}],
         committed_offset = -1 :: osiris:offset()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

start(Config = #{name := Name, leader_node := Leader}) ->
    supervisor:start_child({?SUP, Leader},
                           #{id => child_name(Name),
                             start => {?MODULE, start_link, [Config]},
                             restart => temporary,
                             shutdown => 5000,
                             type => worker}).

stop(#{name := Name, leader_node := Leader}) ->
    CName = child_name(Name),
    try
        _ = supervisor:terminate_child({?SUP, Leader}, CName),
        _ = supervisor:delete_child({?SUP, Leader}, CName),
        ok
    catch
        _:{noproc, _} ->
            %% Whole supervisor or app is already down - i.e. stop_app
            ok
    end.

delete(#{leader_node := Leader, name := Name} = Config) ->
    try
        case supervisor:get_childspec({?SUP, Leader}, child_name(Name)) of
            {ok, _} ->
                stop(Config),
                rpc:call(Leader, osiris_log, delete_directory, [Config]);
            {error, not_found} ->
                ok
        end
    catch
        _:{noproc, _} ->
            %% Whole supervisor or app is already down - i.e. stop_app
            ok
    end.

-spec start_link(Config :: map()) ->
                    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    Mod = ?MODULE,
    Opts = [{reversed_batch, true}],
    gen_batch_server:start_link(undefined, Mod, Config, Opts).

overview(Pid) when node(Pid) == node() ->
    #{dir := Dir} = gen_batch_server:call(Pid, get_reader_context),
    {ok, osiris_log:overview(Dir)}.

init_data_reader(Pid, TailInfo) when node(Pid) == node() ->
    Ctx = gen_batch_server:call(Pid, get_reader_context),
    osiris_log:init_data_reader(TailInfo, Ctx).

register_data_listener(Pid, Offset) ->
    ok =
        gen_batch_server:cast(Pid, {register_data_listener, self(), Offset}).

-spec ack(identifier(), osiris:offset()) -> ok.
ack(LeaderPid, Offset) when is_integer(Offset) andalso Offset >= 0 ->
    gen_batch_server:cast(LeaderPid, {ack, node(), Offset}).

-spec write(Pid :: pid(),
            Sender :: pid(),
            WriterId :: binary() | undefined,
            CorrOrSeq :: non_neg_integer() | term(),
            Data :: osiris:data()) ->
               ok.
write(Pid, Sender, WriterId, Corr, Data)
    when is_pid(Pid) andalso is_pid(Sender) ->
    gen_batch_server:cast(Pid, {write, Sender, WriterId, Corr, Data}).

-spec write_tracking(pid(), binary(), osiris:offset()) -> ok.
write_tracking(Pid, TrackingId, Offset)
    when is_pid(Pid)
         andalso is_binary(TrackingId)
         andalso byte_size(TrackingId) =< 255
         andalso is_integer(Offset) ->
    gen_batch_server:cast(Pid, {write_tracking, TrackingId, Offset}).

read_tracking(Pid, TrackingId) ->
    gen_batch_server:call(Pid, {read_tracking, TrackingId}).

query_writers(Pid, QueryFun) ->
    gen_batch_server:call(Pid, {query_writers, QueryFun}).

-spec init(osiris:config()) -> {ok, state()}.
init(#{name := Name,
       external_ref := ExtRef,
       replica_nodes := Replicas} =
         Config)
    when is_list(Name) ->
    Dir = osiris_log:directory(Config),
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    ORef = atomics:new(1, [{signed, true}]),
    CntName = {?MODULE, ExtRef},
    Log = osiris_log:init(Config#{dir => Dir,
                                  counter_spec =>
                                      {CntName, ?ADD_COUNTER_FIELDS}}),
    CntRef = osiris_log:counters_ref(Log),
    %% should this not be be chunk id rather than last offset?
    LastOffs = osiris_log:next_offset(Log) - 1,
    CommittedOffset =
        case osiris_log:tail_info(Log) of
            {_, {_, BatchOffs}} when Replicas == [] ->
                %% only when there are no replicas can we
                %% recover the committed offset from the last
                %% batch offset in the log
                BatchOffs;
            _ ->
                -1
        end,
    atomics:put(ORef, 1, CommittedOffset),
    counters:put(CntRef, ?C_COMMITTED_OFFSET, CommittedOffset),
    EvtFmt = maps:get(event_formatter, Config, undefined),
    ?INFO("osiris_writer:init/1: name: ~s last offset: ~b "
          "committed chunk id: ~b",
          [Name, LastOffs, CommittedOffset]),
    {ok,
     #?MODULE{cfg =
                  #cfg{name = Name,
                       %% reference used for notification
                       %% if not provided use the name
                       ext_reference = ExtRef,
                       event_formatter = EvtFmt,
                       offset_ref = ORef,
                       replicas = Replicas,
                       directory = Dir,
                       counter = CntRef},
              committed_offset = CommittedOffset,
              replica_state = maps:from_list([{R, -1} || R <- Replicas]),
              log = Log}}.

handle_batch(Commands,
             #?MODULE{cfg = #cfg{counter = Cnt, offset_ref = ORef} = Cfg,
                      duplicates = Dupes0,
                      committed_offset = COffs0,
                      log = Log0} =
                 State0) ->
    %% process commands in reverse order
    case catch lists:foldr(fun handle_command/2,
                           {State0, [], [], #{}, #{}, #{}, []}, Commands)
    of
        {State1, Entries, Replies, Corrs, Trk, Wrt, Dupes} ->
            ThisBatchOffs = osiris_log:next_offset(Log0),
            Log1 =
                osiris_log:write(Entries,
                                 ?CHNK_USER,
                                 erlang:system_time(millisecond),
                                 Wrt,
                                 Log0),
            Log = osiris_log:write_tracking(Trk, delta, Log1),
            State2 =
                update_pending(ThisBatchOffs, Corrs, State1#?MODULE{log = Log}),

            LastChId =
                case osiris_log:tail_info(State2#?MODULE.log) of
                    {_, {_, BatchOffs}} ->
                        BatchOffs;
                    _ ->
                        -1
                end,
            COffs =
                agreed_commit([LastChId
                               | maps:values(State2#?MODULE.replica_state)]),

            RemDupes = handle_duplicates(COffs, Dupes ++ Dupes0, Cfg),
            %% if committed offset has increased - update
            State =
                case COffs > COffs0 of
                    true ->
                        P = State2#?MODULE.pending_corrs,
                        atomics:put(ORef, 1, COffs),
                        counters:put(Cnt, ?C_COMMITTED_OFFSET, COffs),
                        Pending = notify_writers(P, COffs, Cfg),
                        State2#?MODULE{committed_offset = COffs,
                                       duplicates = RemDupes,
                                       pending_corrs = Pending};
                    false ->
                        State2#?MODULE{duplicates = RemDupes}
                end,
            {ok, [garbage_collect | Replies],
             notify_offset_listeners(notify_data_listeners(State))};
        {stop, normal} ->
            {stop, normal}
    end.

terminate(Reason,
          #?MODULE{log = Log,
                   data_listeners = Listeners,
                   cfg = #cfg{name = Name}}) ->
    ?INFO("osiris_writer:terminate/2: name ~s reason: ~w",
          [Name, Reason]),
    ok = osiris_log:close(Log),
    [osiris_replica_reader:stop(Pid) || {Pid, _} <- Listeners],
    ok.

format_status(State) ->
    State.

%% Internal

update_pending(BatchOffs, Corrs,
               #?MODULE{cfg = #cfg{}, pending_corrs = Pending0} = State) ->
    case Corrs of
        _ when map_size(Corrs) == 0 ->
            State;
        _ ->
            State#?MODULE{pending_corrs =
                              queue:in({BatchOffs, Corrs}, Pending0)}
    end.

put_writer(undefined, _Corr, Wrt) ->
    Wrt;
put_writer(WriterId, Corr, Wrt) when is_binary(WriterId) ->
    Wrt#{WriterId => Corr}.

handle_duplicates(CommittedOffset, Dupes, #cfg{} = Cfg)
    when is_list(Dupes) ->
    %% turn list of dupes into  corr map
    {Rem, Corrs} =
        lists:foldl(fun ({ChId, Pid, WriterId, Seq}, {Rem, Corrs0})
                            when ChId =< CommittedOffset ->
                            Corrs =
                                maps:update_with({Pid, WriterId},
                                                 fun(C) -> [Seq | C] end,
                                                 [Seq],
                                                 Corrs0),
                            {Rem, Corrs};
                        (Dupe, {Rem, Corrs}) ->
                            {[Dupe | Rem], Corrs}
                    end,
                    {[], #{}}, Dupes),
    send_written_events(Cfg, Corrs),
    Rem.

handle_command({cast, {write, Pid, WriterId, Corr, R}},
               {State, Records, Replies, Corrs0, Trk, Wrt, Dupes}) ->
    case is_duplicate(WriterId, Corr, State, Wrt) of
        {false, _} ->
            Corrs =
                maps:update_with({Pid, WriterId},
                                 fun(C) -> [Corr | C] end,
                                 [Corr],
                                 Corrs0),
            {State,
             [R | Records],
             Replies,
             Corrs,
             Trk,
             put_writer(WriterId, Corr, Wrt),
             Dupes};
        {true, ChId} ->
            %% add write to duplications list
            {State,
             Records,
             Replies,
             Corrs0,
             Trk,
             Wrt,
             [{ChId, Pid, WriterId, Corr} | Dupes]}
    end;
handle_command({cast, {write_tracking, TrackingId, Offset}},
               {State, Records, Replies, Corrs, Trk0, Wrt, Dupes}) ->
    Trk = Trk0#{TrackingId => Offset},
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({call, From, {read_tracking, TrackingId}},
               {State, Records, Replies0, Corrs, Trk, Wrt, Dupes}) ->
    %% need to merge pending tracking entries before read
    Tracking = osiris_log:tracking(State#?MODULE.log),
    Replies =
        [{reply, From, maps:get(TrackingId, Tracking, undefined)} | Replies0],
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({cast, {register_data_listener, Pid, Offset}},
               {#?MODULE{data_listeners = Listeners} = State0,
                Records,
                Replies,
                Corrs,
                Trk,
                Wrt,
                Dupes}) ->
    State = State0#?MODULE{data_listeners = [{Pid, Offset} | Listeners]},
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({cast,
                {register_offset_listener, Pid, EvtFormatter, Offset}},
               {#?MODULE{offset_listeners = Listeners} = State0,
                Records,
                Replies,
                Corrs,
                Trk,
                Wrt,
                Dupes}) ->
    State =
        State0#?MODULE{offset_listeners =
                           [{Pid, Offset, EvtFormatter} | Listeners]},
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({cast, {ack, ReplicaNode, Offset}},
               {#?MODULE{replica_state = ReplicaState0} = State0,
                Records,
                Replies,
                Corrs,
                Trk,
                Wrt,
                Dupes}) ->
    % ?DEBUG("osiris_writer ack from ~w at ~b", [ReplicaNode, Offset]),
    ReplicaState =
        maps:update_with(ReplicaNode,
                         fun(O) -> max(O, Offset) end,
                         Offset,
                         ReplicaState0),
    State = State0#?MODULE{replica_state = ReplicaState},
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({call, From, get_reader_context},
               {#?MODULE{cfg =
                             #cfg{offset_ref = ORef,
                                  name = Name,
                                  directory = Dir},
                         committed_offset = COffs} =
                    State,
                Records,
                Replies,
                Corrs,
                Trk,
                Wrt,
                Dupes}) ->
    Reply =
        {reply, From,
         #{dir => Dir,
           name => Name,
           committed_offset => max(0, COffs),
           offset_ref => ORef}},
    {State, Records, [Reply | Replies], Corrs, Trk, Wrt, Dupes};
handle_command({call, From, {query_writers, QueryFun}},
               {State, Records, Replies0, Corrs, Trk, Wrt, Dupes}) ->
    %% need to merge pending tracking entries before read
    Writers = osiris_log:writers(State#?MODULE.log),
    Result =
        try QueryFun(Writers) of
            R ->
                R
        catch
            Err ->
                Err
        end,
    Replies = [{reply, From, Result} | Replies0],
    {State, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command({call, From, {update_retention, Retention}},
               {#?MODULE{log = Log0} = State,
                Records,
                Replies0,
                Corrs,
                Trk,
                Wrt,
                Dupes}) ->
    Log = osiris_log:update_retention(Retention, Log0),
    Replies = [{reply, From, ok} | Replies0],
    {State#?MODULE{log = Log}, Records, Replies, Corrs, Trk, Wrt, Dupes};
handle_command(osiris_stop, _Acc) ->
    throw({stop, normal});
handle_command(_Unk, Acc) ->
    ?DEBUG("osiris_writer: unknown command ~w", [_Unk]),
    Acc.

notify_data_listeners(#?MODULE{log = Seg, data_listeners = L0} =
                          State) ->
    NextOffset = osiris_log:next_offset(Seg),
    {Notify, L} =
        lists:partition(fun({_Pid, O}) -> O < NextOffset end, L0),
    [gen_server:cast(P, {more_data, NextOffset}) || {P, _} <- Notify],
    State#?MODULE{data_listeners = L}.

notify_offset_listeners(#?MODULE{cfg =
                                     #cfg{ext_reference = Ref,
                                          event_formatter = EvtFmt},
                                 committed_offset = COffs,
                                 offset_listeners = L0} =
                            State) ->
    {Notify, L} =
        lists:partition(fun({_Pid, O, _}) -> O =< COffs end, L0),
    [begin
         Evt =
             wrap_osiris_event(%% the per offset listener event formatter takes precedence of
                               %% the process scoped one
                               select_formatter(Fmt, EvtFmt),
                               {osiris_offset, Ref, COffs}),
         P ! Evt
     end
     || {P, _, Fmt} <- Notify],
    State#?MODULE{offset_listeners = L}.

select_formatter(undefined, Fmt) ->
    Fmt;
select_formatter(Fmt, _) ->
    Fmt.

notify_writers(Q0, COffs, Cfg) ->
    case queue:peek(Q0) of
        {value, {O, Corrs}} when O =< COffs ->
            send_written_events(Cfg, Corrs),
            {_, Q} = queue:out(Q0),
            notify_writers(Q, COffs, Cfg);
        _ ->
            Q0
    end.

send_written_events(#cfg{ext_reference = ExtRef,
                         event_formatter = Fmt},
                    Corrs) ->
    %% TODO: minor optimisation: use maps:iterator here to avoid building a new
    %% result map
    maps:map(fun({P, WId}, V) ->
                %% TODO: if the writer is on a remote node this could block
                %% which is bad but we'd have to consider the downsides of using
                %% send with noconnect and nosuspend here
                % ?DEBUG("send_written_events ~s ~w", [ExtRef, V]),
                P
                ! wrap_osiris_event(Fmt,
                                    {osiris_written,
                                     ExtRef,
                                     WId,
                                     lists:reverse(V)})
             end,
             Corrs),
    ok.

wrap_osiris_event(undefined, Evt) ->
    Evt;
wrap_osiris_event({M, F, A}, Evt) ->
    apply(M, F, [Evt | A]).

-spec agreed_commit([osiris:offset()]) -> osiris:offset().
agreed_commit(Indexes) ->
    SortedIdxs = lists:sort(fun erlang:'>'/2, Indexes),
    Nth = length(SortedIdxs) div 2 + 1,
    lists:nth(Nth, SortedIdxs).

child_name(Name) ->
    lists:flatten(
        io_lib:format("~s_writer", [Name])).

is_duplicate(undefined, _, _, _) ->
    {false, 0};
is_duplicate(WriterId, Corr, #?MODULE{log = Log}, Wrt) ->
    case Wrt of
        #{WriterId := Seq} ->
            ChunkId = osiris_log:next_offset(Log),
            {Corr =< Seq, ChunkId};
        _ ->
            Writers = osiris_log:writers(Log),
            case Writers of
                #{WriterId := {ChunkId, _, Seq}} ->
                    {Corr =< Seq, ChunkId};
                _ ->
                    {false, 0}
            end
    end.
