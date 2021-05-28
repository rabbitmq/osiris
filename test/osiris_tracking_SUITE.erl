%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_tracking_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TRK_TYPE_SEQUENCE, 0).
-define(TRK_TYPE_OFFSET, 1).
%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() -> [basics,
               recover].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

basics(_Config) ->
    T0 = osiris_tracking:init(undefined),
    Ts1 = ?LINE,
    T1 = osiris_tracking:add(<<"w1">>, sequence, 55, Ts1, T0),
    ?assert(osiris_tracking:needs_flush(T1)),
    ?assertEqual({ok, 55}, osiris_tracking:query(<<"w1">>, sequence, T1)),
    ?assertEqual({error, not_found}, osiris_tracking:query(<<"w2">>, sequence, T1)),
    {Trailer1, T2} = osiris_tracking:flush(T1),
    ?assert(false == osiris_tracking:needs_flush(T2)),
    ?assertMatch(<<?TRK_TYPE_SEQUENCE:8,
                   2:8/unsigned,
                   "w1", 55:64/unsigned>>,
                 iolist_to_binary(Trailer1)),

    Ts2 = ?LINE,
    T3 = osiris_tracking:add(<<"t1">>, offset, 99, Ts2, T2),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T3)),
    {Trailer2, T4} = osiris_tracking:flush(T3),
    ?assertMatch(<<?TRK_TYPE_OFFSET:8,
                   2:8/unsigned,
                   "t1", 99:64/unsigned>>,
                 iolist_to_binary(Trailer2)),

    {Snap1, _T5} = osiris_tracking:snapshot(99, T4),
    ?assertMatch(<<?TRK_TYPE_OFFSET:8/unsigned,
                   2:8/unsigned,
                   "t1",
                   99:64/unsigned,
                   ?TRK_TYPE_SEQUENCE:8/unsigned,
                   2:8/unsigned,
                   "w1",
                   Ts1:64/unsigned,
                   55:64/unsigned>>, iolist_to_binary(Snap1)),
    %% passing a first offset lower than the tracking id should discard it
    {Snap2, _T6} = osiris_tracking:snapshot(100, T4),
    ?assertMatch(<<?TRK_TYPE_SEQUENCE:8/unsigned,
                   2:8/unsigned,
                   "w1",
                   Ts1:64/unsigned,
                   55:64/unsigned>>, iolist_to_binary(Snap2)),
    ok.


recover(_Config) ->
    Ts1 = ?LINE,
    SnapBin = <<?TRK_TYPE_OFFSET:8/unsigned,
                2:8/unsigned,
                "t1",
                99:64/unsigned,
                ?TRK_TYPE_SEQUENCE:8/unsigned,
                2:8/unsigned,
                "w1",
                Ts1:64/unsigned,
                55:64/unsigned>>,

    T0 = osiris_tracking:init(SnapBin),
    ?assertEqual({ok, 55}, osiris_tracking:query(<<"w1">>, sequence, T0)),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T0)),

    Trailer = <<?TRK_TYPE_OFFSET:8/unsigned,
                2:8/unsigned,
                "t2",
                103:64/unsigned,
                ?TRK_TYPE_SEQUENCE:8/unsigned,
                2:8/unsigned,
                "w2",
                77:64/unsigned>>,

    Ts = ?LINE,
    T1 = osiris_tracking:append_trailer(Ts, Trailer, T0),
    ?assertEqual({ok, 55}, osiris_tracking:query(<<"w1">>, sequence, T1)),
    ?assertEqual({ok, 77}, osiris_tracking:query(<<"w2">>, sequence, T1)),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T1)),
    ?assertEqual({ok, 103}, osiris_tracking:query(<<"t2">>, offset, T1)),
    ok.
