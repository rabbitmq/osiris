%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_retention).

-behaviour(gen_batch_server).

-include("osiris.hrl").
%% API functions
-export([start_link/0,
         eval/4]).
%% gen_batch_server callbacks
-export([init/1,
         handle_batch/2,
         terminate/2]).

-define(DEFAULT_SCHEDULED_EVAL_TIME, 1000 * 60 * 60). %% 1HR

-record(state, {scheduled = #{} :: #{osiris:name() => timer:tref()}}).

%%%===================================================================
%%% API functions
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec eval(osiris:name(), file:name_all(), [osiris:retention_spec()],
           fun((osiris_log:range()) -> ok)) ->
    ok.
eval(_Name, _Dir, [], _Fun) ->
    ok;
eval(Name, Dir, Specs, Fun) ->
    gen_batch_server:cast(?MODULE, {eval, self(), Name, Dir, Specs, Fun}).

%%%===================================================================
%%% gen_batch_server callbacks
%%%===================================================================

-spec init([]) -> {ok, #state{}}.
init([]) ->
    {ok, #state{}}.

-spec handle_batch([gen_batch_server:op()], #state{}) -> {ok, #state{}}.
handle_batch(Ops, State0) ->
    Evals = deduplicate_evals(Ops),
    State1 = lists:foldl(fun evaluate_retention/2, State0, Evals),
    {ok, State1}.

-spec terminate(term(), #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Deduplicate by Name as multiple requests from the same stream might
%% have arrived while processing the previous batch.
%% Keeping last occurrence for each Name, as retention config could
%% have changed.
deduplicate_evals(Ops) ->
    Map = lists:foldl(
            fun({cast, {eval, _Pid, Name, _Dir, _Specs, _Fun} = Eval}, Acc) ->
                    maps:put(Name, Eval, Acc);
               ({call, From, _}, Acc) ->
                    gen:reply(From, ok),
                    Acc;
               (_, Acc) ->
                    Acc
            end, #{}, Ops),
    maps:values(Map).

evaluate_retention({eval, Pid, Name, Dir, Specs, Fun} = Eval, State) ->
    %% only do retention evaluation for stream processes that are
    %% alive as the callback Fun passed in would update a shared atomic
    %% value and this atomic is new per process incarnation
    case is_process_alive(Pid) of
        true ->
            try osiris_log:evaluate_retention(Dir, Specs) of
                Result ->
                    _ = Fun(Result),
                    schedule(Eval, Result, State)
            catch _:Err ->
                    ?DEBUG_(Name, "retention evaluation failed with ~w", [Err]),
                    State
            end;
        false ->
            State
    end.

schedule({eval, _Pid, Name, _Dir, Specs, _Fun} = Eval,
         {_, _, NumSegmentRemaining},
         #state{scheduled = Scheduled0} = State) ->
    %% we need to check the scheduled map even if the current specs do not
    %% include max_age as the retention config could have changed
    Scheduled = case maps:take(Name, Scheduled0) of
                    {OldRef, Scheduled1} ->
                        _ = erlang:cancel_timer(OldRef),
                        Scheduled1;
                    error ->
                        Scheduled0
                end,
    case lists:any(fun ({T, _}) -> T == max_age end, Specs) andalso
         NumSegmentRemaining > 1 of
        true ->
            EvalInterval = application:get_env(osiris, retention_eval_interval,
                                               ?DEFAULT_SCHEDULED_EVAL_TIME),
            Ref = erlang:send_after(EvalInterval, self(), {'$gen_cast', Eval}),
            State#state{scheduled = Scheduled#{Name => Ref}};
        false ->
            State#state{scheduled = Scheduled}
    end.
