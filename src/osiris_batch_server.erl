%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% Based on gen_batch_server. Extended with optional byte-size-aware batching.
%%
-module(osiris_batch_server).
-export([start_link/2,
         start_link/3,
         start_link/4,
         init_it/6,
         stop/1,
         stop/3,
         cast/2,
         cast_batch/2,
         call/2,
         call/3,
         system_continue/3,
         system_terminate/4,
         system_get_state/1,
         write_debug/3,
         format_status/2,
         %% needed for hibernation
         loop_wait/2
        ]).

-define(MIN_MAX_BATCH_SIZE, 32).
-define(MAX_MAX_BATCH_SIZE, 8192).

-type server_ref() :: pid() |
                      (LocalName :: atom()) |
                      {Name :: atom(), Node :: atom()} |
                      {'global', term()} |
                      {'via', Module :: module(), Name :: term()}.

-type from() :: {Pid :: pid(), Tag :: reference()}.

-type op() :: {cast, pid(), UserOp :: term()} |
              {call, from(), UserOp :: term()} |
              {info, UserOp :: term()}.

-record(config, {batch_size :: non_neg_integer(),
                 min_batch_size :: non_neg_integer(),
                 max_batch_size :: non_neg_integer(),
                 max_batch_bytes = infinity :: non_neg_integer() | infinity,
                 parent :: pid(),
                 name :: atom(),
                 module :: module(),
                 hibernate_after = infinity :: non_neg_integer(),
                 reversed_batch = false :: boolean(),
                 has_batch_item_size = false :: boolean()}).

-record(state, {batch = [] :: [op()],
                batch_count = 0 :: non_neg_integer(),
                batch_bytes = 0 :: non_neg_integer(),
                config = #config{} :: #config{},
                state :: term(),
                needs_gc = false :: boolean(),
                debug :: list()}).

% -type state() :: #state{}.

-export_type([from/0, op/0,
              action/0, server_ref/0]).

%%% Behaviour

-type action() ::
    {reply, from(), Msg :: term()} |
    garbage_collect.
%% an action that can be returned from handle_batch/2

-callback init(Args :: term()) ->
    {ok, State} |
    {ok, State, {continue, term()}} |
    {stop, Reason :: term()}
      when State :: term().

-callback handle_batch([op()], State) ->
    {ok, State} |
    {ok, State, {continue, term()}} |
    {ok, [action()], State} |
    {ok, [action()], State, {continue, term()}} |
    {stop, Reason :: term()}
      when State :: term().

-callback handle_continue(Continue :: term(), State :: term()) -> term().

-callback terminate(Reason :: term(), State :: term()) -> term().

-callback format_status(State :: term()) -> term().

-callback batch_item_size(op()) -> non_neg_integer().

%% TODO: code_change

-optional_callbacks([handle_continue/2,
                     format_status/1,
                     terminate/2,
                     batch_item_size/1]).


%%%
%%% API
%%%

-spec start_link(Mod, Args) -> Result when
     Mod :: module(),
     Args :: term(),
     Result ::  {ok,pid()} | {error, Reason :: term()}.
start_link(Mod, Args) ->
    gen:start(?MODULE, link, Mod, {[], Args}, []).

-spec start_link(Name, Mod, Args) -> Result when
     Name :: {local, atom()} |
             {global, term()} |
             {via, atom(), term()} |
             undefined,
     Mod :: module(),
     Args :: term(),
     Result ::  {ok,pid()} | {error, Reason :: term()}.
start_link(Name, Mod, Args) ->
    gen_start(Name, Mod, Args, []).

-spec start_link(Name, Mod, Args, Options) -> Result when
     Name :: {local, atom()} |
             {global, term()} |
             {via, atom(), term()} |
             undefined,
     Mod :: module(),
     Args :: term(),
     Options :: list(),
     Result ::  {ok,pid()} | {error, Reason :: term()}.
start_link(Name, Mod, Args, Opts0) ->
    gen_start(Name, Mod, Args, Opts0).

%% pretty much copied wholesale from gen_server
init_it(Starter, self, Name, Mod, Args, Options) ->
    init_it(Starter, self(), Name, Mod, Args, Options);
init_it(Starter, Parent, Name0, Mod, {GBOpts, Args}, Options) ->
    Name = gen:name(Name0),
    Debug = gen:debug_options(Name, Options),
    HibernateAfter = gen:hibernate_after(Options),
    MaxBatchSize = proplists:get_value(max_batch_size, GBOpts,
                                       ?MAX_MAX_BATCH_SIZE),
    MinBatchSize = proplists:get_value(min_batch_size, GBOpts,
                                       ?MIN_MAX_BATCH_SIZE),
    ReverseBatch = proplists:get_value(reversed_batch, GBOpts, false),
    MaxBatchBytes = proplists:get_value(max_batch_bytes, GBOpts, infinity),
    HasBatchItemSize = erlang:function_exported(Mod, batch_item_size, 1),
    Conf = #config{module = Mod,
                   parent = Parent,
                   name = Name,
                   batch_size = MinBatchSize,
                   min_batch_size = MinBatchSize,
                   max_batch_size = MaxBatchSize,
                   max_batch_bytes = MaxBatchBytes,
                   hibernate_after = HibernateAfter,
                   reversed_batch = ReverseBatch,
                   has_batch_item_size = HasBatchItemSize},
    case catch Mod:init(Args) of
        {ok, Inner0} ->
            proc_lib:init_ack(Starter, {ok, self()}),
            State = #state{config = Conf,
                           state = Inner0,
                           debug = Debug},
            loop_wait(State, Parent);
        {ok, Inner0, {continue, Continue}} ->
            proc_lib:init_ack(Starter, {ok, self()}),
            State0 = #state{config = Conf,
                            state = Inner0,
                            debug = Debug},
            State = handle_continue(Continue, State0),
            loop_wait(State, Parent);
        {stop, Reason} ->
            %% For consistency, we must make sure that the
            %% registered name (if any) is unregistered before
            %% the parent process is notified about the failure.
            %% (Otherwise, the parent process could get
            %% an 'already_started' error if it immediately
            %% tried starting the process again.)
            gen:unregister_name(Name0),
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        % ignore ->
        %     gen:unregister_name(Name0),
        %     proc_lib:init_ack(Starter, ignore),
        %     exit(normal);
        {'EXIT', Reason} ->
            gen:unregister_name(Name0),
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        Else ->
            Error = {bad_return_value, Else},
            proc_lib:init_ack(Starter, {error, Error}),
            exit(Error)
    end.

stop(Name) ->
    gen:stop(Name).

stop(Name, Reason, Timeout) ->
    gen:stop(Name, Reason, Timeout).

-spec cast(server_ref(), term()) -> ok.
cast({global,Name}, Request) ->
    catch global:send(Name, cast_msg(Request)),
    ok;
cast({via, Mod, Name}, Request) ->
    catch Mod:send(Name, cast_msg(Request)),
    ok;
cast({Name,Node}=Dest, Request) when is_atom(Name), is_atom(Node) ->
    do_cast(Dest, Request);
cast(Dest, Request) when is_atom(Dest) ->
    do_cast(Dest, Request);
cast(Dest, Request) when is_pid(Dest) ->
    do_cast(Dest, Request).


do_cast(Dest, Request) ->
  do_send(Dest, cast_msg(Request)),
  ok.

cast_msg(Request) ->
  {'$gen_cast', Request}.

-spec cast_batch(server_ref(), [term()]) -> ok.
cast_batch({global,Name}, Batch) ->
    catch global:send(Name, cast_batch_msg(Batch)),
    ok;
cast_batch({via, Mod, Name}, Batch) ->
    catch Mod:send(Name, cast_batch_msg(Batch)),
    ok;
cast_batch({Name,Node}=Dest, Batch) when is_atom(Name), is_atom(Node) ->
    do_cast_batch(Dest, Batch);
cast_batch(Dest, Batch) when is_atom(Dest) ->
    do_cast_batch(Dest, Batch);
cast_batch(Dest, Batch) when is_pid(Dest) ->
    do_cast_batch(Dest, Batch).

do_cast_batch(Dest, Batch) ->
    do_send(Dest, cast_batch_msg(Batch)),
    ok.

cast_batch_msg(Msgs0) when is_list(Msgs0) ->
    Msgs = lists:foldl(fun (Msg, Acc) ->
                               [{cast, Msg} | Acc]
                       end, [], Msgs0),
    {'$gen_cast_batch', Msgs, length(Msgs)};
cast_batch_msg(_) ->
    erlang:error(badarg).

-spec call(server_ref(), Request :: term()) -> term().
call(Name, Request) ->
    call(Name, Request, 5000).

-spec call(pid() | atom(), term(), non_neg_integer()) -> term().
call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$gen_call', Request, Timeout) of
        {ok, Res} ->
            Res;
        {'EXIT', Reason} ->
            exit({Reason, {?MODULE, call, [Name, Request, Timeout]}})
    end.


%% Internal

loop_wait(#state{config = #config{hibernate_after = Hib}} = State00, Parent) ->
    %% batches can accumulate a lot of garbage, collect it here
    %% evaluate gc state
    State0 = case State00 of
                 #state{needs_gc = true} ->
                     _ = garbage_collect(),
                     State00#state{needs_gc = false};
                 _ ->
                     State00
             end,
    receive
        Msg ->
            case Msg of
                {system, From, Request} ->
                    sys:handle_system_msg(Request, From, Parent,
                                          ?MODULE, State0#state.debug, State0);
                {'EXIT', Parent, Reason} ->
                    terminate(Reason, State0),
                    exit(Reason);
                _ ->
                    enter_loop_batched(Msg, Parent, State0)
            end
    after Hib ->
              proc_lib:hibernate(?MODULE, ?FUNCTION_NAME, [State0, Parent])
    end.

append_msg({'$gen_cast', Msg},
           #state{batch = Batch,
                  batch_count = BatchCount,
                  batch_bytes = BatchBytes,
                  config = Config} = State0) ->
    Op = {cast, Msg},
    Size = item_size(Config, Op),
    State0#state{batch = [Op | Batch],
                 batch_count = BatchCount + 1,
                 batch_bytes = BatchBytes + Size};
append_msg({'$gen_cast_batch', Msgs, Count},
           #state{batch = Batch,
                  batch_count = BatchCount,
                  batch_bytes = BatchBytes,
                  config = Config} = State0) ->
    Size = batch_size(Config, Msgs),
    State0#state{batch = Msgs ++ Batch,
                 batch_count = BatchCount + Count,
                 batch_bytes = BatchBytes + Size};
append_msg({'$gen_call', From, Msg},
           #state{batch = Batch,
                  batch_count = BatchCount} = State0) ->
    State0#state{batch = [{call, From, Msg} | Batch],
                 batch_count = BatchCount + 1};
append_msg(Msg, #state{batch = Batch,
                       batch_count = BatchCount} = State0) ->
    State0#state{batch = [{info, Msg} | Batch],
                 batch_count = BatchCount + 1}.

enter_loop_batched(Msg, Parent, #state{debug = []} = State0) ->
    loop_batched(append_msg(Msg, State0), Parent);
enter_loop_batched(Msg, Parent, State0) ->
    State = handle_debug_in(State0, Msg),
    %% append to batch
    loop_batched(append_msg(Msg, State), Parent).

loop_batched(#state{config = #config{batch_size = BatchSize,
                                     max_batch_size = Max,
                                     max_batch_bytes = MaxBytes} = Config,
                    batch_count = BatchCount,
                    batch_bytes = BatchBytes} = State0,
             Parent) when BatchCount >= BatchSize orelse
                          BatchBytes >= MaxBytes ->
    % complete batch after seeing batch_size writes or max_batch_bytes reached
    State = complete_batch(State0),
    % grow max batch size
    NewBatchSize = min(Max, BatchSize * 2),
    loop_wait(State#state{config = Config#config{batch_size = NewBatchSize}},
              Parent);
loop_batched(#state{debug = Debug} = State0, Parent) ->
    receive
        Msg ->
            case Msg of
                {system, From, Request} ->
                    sys:handle_system_msg(Request, From, Parent,
                                          ?MODULE, Debug, State0);
                {'EXIT', Parent, Reason} ->
                    terminate(Reason, State0),
                    exit(Reason);
                _ ->
                    enter_loop_batched(Msg, Parent, State0)
            end
    after 0 ->
              State = complete_batch(State0),
              Config = State#state.config,
              NewBatchSize = max(Config#config.min_batch_size,
                                 Config#config.batch_size / 2),
              loop_wait(State#state{config =
                                    Config#config{batch_size = NewBatchSize}},
                        Parent)
    end.

terminate(Reason, #state{config = #config{module = Mod}, state = Inner}) ->
    catch Mod:terminate(Reason, Inner),
    ok.


complete_batch(#state{batch = []} = State) ->
    State;
complete_batch(#state{batch = Batch0,
                      config = #config{module = Mod,
                                       reversed_batch = ReverseBatch},
                      state = Inner0,
                      debug = Debug0} = State0) ->
    Batch = case ReverseBatch of
                false ->
                    %% reversing restores the received order
                    lists:reverse(Batch0);
                true ->
                    %% accepting the batch in reverse order means we can avoid
                    %% reversing the list
                    Batch0
            end,

    case catch Mod:handle_batch(Batch, Inner0) of
        {ok, Inner} ->
            State0#state{batch = [],
                         state = Inner,
                         batch_count = 0,
                         batch_bytes = 0};
        {ok, Inner, {continue, Continue}} ->
            handle_continue(Continue,
                            State0#state{batch = [],
                                         state = Inner,
                                         batch_count = 0,
                                         batch_bytes = 0});
        {ok, Actions, Inner} when is_list(Actions) ->
            {ShouldGc, Debug} = handle_actions(Actions, Debug0),
            State0#state{batch = [],
                         batch_count = 0,
                         batch_bytes = 0,
                         state = Inner,
                         needs_gc = ShouldGc,
                         debug = Debug};
        {ok, Actions, Inner, {continue, Continue}} when is_list(Actions) ->
            {ShouldGc, Debug} = handle_actions(Actions, Debug0),
            handle_continue(Continue,
                            State0#state{batch = [],
                                         batch_count = 0,
                                         batch_bytes = 0,
                                         state = Inner,
                                         needs_gc = ShouldGc,
                                         debug = Debug});
        {stop, Reason} ->
            terminate(Reason, State0),
            exit(Reason);
        {'EXIT', Reason} ->
            terminate(Reason, State0),
            exit(Reason)
    end.

handle_actions(Actions, Debug0) ->
    lists:foldl(fun ({reply, {Pid, Tag}, Msg},
                     {ShouldGc, Dbg}) ->
                        Pid ! {Tag, Msg},
                        {ShouldGc,
                         handle_debug_out(Pid, Msg, Dbg)};
                    (garbage_collect, {_, Dbg}) ->
                        {true, Dbg}
                end, {false, Debug0}, Actions).


handle_continue(Continue, #state{config = #config{module = Mod},
                                 state = Inner0} = State0) ->
    case catch Mod:handle_continue(Continue, Inner0) of
        {ok, Inner} ->
            State0#state{state = Inner};
        {ok, Inner, {continue, Continue}} ->
            handle_continue(Continue, State0#state{state = Inner});
        {stop, Reason} ->
            terminate(Reason, State0),
            exit(Reason);
        {'EXIT', Reason} ->
            terminate(Reason, State0),
            exit(Reason)
    end.

handle_debug_in(#state{debug = Dbg0} = State, Msg) ->
    Dbg = sys:handle_debug(Dbg0, fun write_debug/3, ?MODULE, {in, Msg}),
    State#state{debug = Dbg}.

handle_debug_out(_, _, []) ->
    [];
handle_debug_out(Pid, Msg, Dbg) ->
    Evt = {out, {self(), Msg}, Pid},
    sys:handle_debug(Dbg, fun write_debug/3, ?MODULE, Evt).

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the current batch or not
    loop_batched(State#state{debug = Debug}, Parent).

-spec system_terminate(term(), pid(), list(), term()) -> no_return().
system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State),
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

format_status(_Reason, [_PDict, SysState, Parent, Debug,
                        #state{config = #config{name = Name,
                                                module = Mod},
                               state = State }]) ->
    Header = gen:format_status_header("Status for batching server", Name),
    Log = sys_get_log(Debug),

    [{header, Header},
     {data,
      [
       {"Status", SysState},
       {"Parent", Parent},
       {"Logged Events", Log}]} |
     case catch Mod:format_status(State) of
         L when is_list(L) -> L;
         {'EXIT', {undef, _}} ->
             %% not implemented just return the state
             [State];
         T -> [T]
     end].

sys_get_log(Debug) ->
    sys:get_log(Debug).

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).

%% Send function

do_send(Dest, Msg) ->
    try erlang:send(Dest, Msg)
    catch
        error:_ -> ok
    end,
    ok.

gen_start(undefined, Mod, Args, Opts0) ->
    %% filter out gen batch server specific options as the options type in gen
    %% is closed and dialyzer would complain.
    {GBOpts, Opts} = lists:splitwith(fun ({Key, _}) ->
                                             Key == max_batch_size orelse
                                             Key == min_batch_size orelse
                                             Key == reversed_batch orelse
                                             Key == max_batch_bytes
                                     end, Opts0),
    gen:start(?MODULE, link, Mod, {GBOpts, Args}, Opts);
gen_start(Name, Mod, Args, Opts0) ->
    %% filter out gen batch server specific options as the options type in gen
    %% is closed and dialyzer would complain.
    {GBOpts, Opts} = lists:splitwith(fun ({Key, _}) ->
                                             Key == max_batch_size orelse
                                             Key == min_batch_size orelse
                                             Key == reversed_batch orelse
                                             Key == max_batch_bytes
                                     end, Opts0),
    gen:start(?MODULE, link, Name, Mod, {GBOpts, Args}, Opts).

%% Internal helpers for byte-size tracking

item_size(#config{has_batch_item_size = true, module = Mod}, Op) ->
    Mod:batch_item_size(Op);
item_size(#config{has_batch_item_size = false}, _Op) ->
    0.

batch_size(#config{has_batch_item_size = true, module = Mod}, Ops) ->
    lists:foldl(fun(Op, Acc) -> Acc + Mod:batch_item_size(Op) end, 0, Ops);
batch_size(#config{has_batch_item_size = false}, _Ops) ->
    0.
