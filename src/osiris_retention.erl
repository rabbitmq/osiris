%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_retention).

-behaviour(gen_server).

%% API functions
-export([start_link/0,
         eval/4]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(DEFAULT_SHEDULED_EVAL_TIME, 1000 * 60 * 60). %% 1HR

-record(state, {scheduled = #{} :: #{osiris:name() => timer:tref()}}).

%%%===================================================================
%%% API functions
%%%===================================================================

%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec eval(osiris:name(), file:name_all(), [osiris:retention_spec()],
           fun((osiris_log:range()) -> ok)) ->
    ok.
eval(_Name, _Dir, [], _Fun) ->
    ok;
eval(Name, Dir, Specs, Fun) ->
    gen_server:cast(?MODULE, {eval, Name, Dir, Specs, Fun}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
init([]) ->
    {ok, #state{}}.

%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
handle_cast({eval, Name, Dir, Specs, Fun}, State) ->
    Result = osiris_log:evaluate_retention(Dir, Specs),
    _ = Fun(Result),
    {noreply, schedule(osiris_util:normalise_name(Name), Specs, Result, State)}.

%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
handle_info(_Info, State) ->
    {noreply, State}.

%% @spec terminate(Reason, State) -> void()
terminate(_Reason, _State) ->
    ok.

%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
schedule(_Name, _Specs, _Result, State) ->
    State.

