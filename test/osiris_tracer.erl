%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_tracer).

-record(?MODULE,
        {
         calls = [] :: list() 
        }).

-export([start/1, loop/1, calls/1, stop/1]).

-spec start(erlang:trace_pattern_mfa()) -> pid().
start(MFA) ->
    P = spawn(?MODULE, loop, [#?MODULE{}]),
    erlang:trace(self(), true, [call, {tracer, P}]),
    erlang:trace_pattern(MFA, true, [global]),
    P.

loop(#?MODULE{calls = Calls} = S) ->
    receive
        {trace, _Pid, call, {Module, Function, Arguments}} ->
            loop(#?MODULE{calls = [{Module, Function, Arguments} | Calls]});
        {P, calls} ->
            P ! lists:reverse(Calls),
            loop(S);
        stop ->
            ok;
        _ ->
            loop(S)
  end.

calls(P) ->
    P ! {self(), calls},
    receive
        Calls -> Calls
    end.

stop(P) ->
    erlang:trace(self(), false, [call]),
    P ! stop.
