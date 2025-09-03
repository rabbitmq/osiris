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

-export([start/1,
         loop/1,
         calls/1,
         calls/3,
         call_count/1,
         call_count/3,
         stop/1]).

start(MFAS) ->
    P = spawn(?MODULE, loop, [#?MODULE{}]),
    erlang:trace(self(), true, [call, {tracer, P}]),
    [erlang:trace_pattern(MFA, true, [global]) || MFA <- MFAS],
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

calls(P, M, F) ->
    Calls = calls(P),
    lists:filter(fun({M1, F1, _A}) when M1 == M andalso F1 == F ->
                         true;
                    (_) -> false
                 end, Calls).

call_count(P) ->
    length(calls(P)).

call_count(P, M, F) ->
    length(calls(P, M, F)).

stop(P) ->
    erlang:trace(self(), false, [call]),
    P ! stop.
