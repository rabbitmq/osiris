%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_tracking_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TRK_TYPE_SEQUENCE, 0).
-define(TRK_TYPE_OFFSET, 1).
-define(TRK_TYPE_TIMESTAMP, 2).
%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() -> [basics,
                max_writers,
                snapshot_excludes_unwritten,
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
    T0 = osiris_tracking:init(undefined, #{}),
    ChId1 = ?LINE,
    T1 = osiris_tracking:add(<<"w1">>, sequence, 55, ChId1, T0),
    ?assert(osiris_tracking:needs_flush(T1)),
    ?assertEqual({ok, {ChId1, 55}}, osiris_tracking:query(<<"w1">>, sequence, T1)),
    ?assertEqual({error, not_found}, osiris_tracking:query(<<"w2">>, sequence, T1)),
    {Trailer1, T2} = osiris_tracking:flush(T1),
    ?assert(false == osiris_tracking:needs_flush(T2)),
    ?assertMatch(<<?TRK_TYPE_SEQUENCE:8,
                   2:8/unsigned,
                   "w1", 55:64/unsigned>>,
                 iolist_to_binary(Trailer1)),

    ChId2 = ?LINE,
    T3 = osiris_tracking:add(<<"t1">>, offset, 99, ChId2, T2),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T3)),
    {Trailer2, T4} = osiris_tracking:flush(T3),
    ?assertMatch(<<?TRK_TYPE_OFFSET:8,
                   2:8/unsigned,
                   "t1", 99:64/unsigned>>,
                 iolist_to_binary(Trailer2)),

    ChId3 = ?LINE,
    Now = erlang:system_time(millisecond),
    T5 = osiris_tracking:add(<<"t2">>, timestamp, Now, ChId3, T4),
    ?assertEqual({ok, Now}, osiris_tracking:query(<<"t2">>, timestamp, T5)),
    {Trailer3, T6} = osiris_tracking:flush(T5),
    ?assertMatch(<<?TRK_TYPE_TIMESTAMP:8,
                   2:8/unsigned,
                   "t2", Now:64/signed>>,
                 iolist_to_binary(Trailer3)),

    %% ensure negative timestamps work (although they shouldn't be used in practice)
    ChId4 = ?LINE,
    NegativeTs = -9,
    T7 = osiris_tracking:add(<<"t3">>, timestamp, NegativeTs, ChId4, T6),
    ?assertEqual({ok, NegativeTs}, osiris_tracking:query(<<"t3">>, timestamp, T7)),
    {Trailer4, T8} = osiris_tracking:flush(T7),
    ?assertMatch(<<?TRK_TYPE_TIMESTAMP:8,
                   2:8/unsigned,
                   "t3", NegativeTs:64/signed>>,
                 iolist_to_binary(Trailer4)),

    {Snap1, _T9} = osiris_tracking:snapshot(99, NegativeTs, T8),
    ?assertMatch(<<?TRK_TYPE_TIMESTAMP:8/unsigned,
                   2:8/unsigned,
                   "t3",
                   NegativeTs:64/signed,
                   ?TRK_TYPE_TIMESTAMP:8/unsigned,
                   2:8/unsigned,
                   "t2",
                   Now:64/signed,
                   ?TRK_TYPE_OFFSET:8/unsigned,
                   2:8/unsigned,
                   "t1",
                   99:64/unsigned,
                   ?TRK_TYPE_SEQUENCE:8/unsigned,
                   2:8/unsigned,
                   "w1",
                   ChId1:64/unsigned,
                   55:64/unsigned>>, iolist_to_binary(Snap1)),
    %% tracking offsets lower than first offset in stream should be discarded
    %% tracking timestamps lower than first timestamp in stream should be discarded
    {Snap2, _T10} = osiris_tracking:snapshot(100, Now+1, T8),
    ?assertMatch(<<?TRK_TYPE_SEQUENCE:8/unsigned,
                   2:8/unsigned,
                   "w1",
                   ChId1:64/unsigned,
                   55:64/unsigned>>, iolist_to_binary(Snap2)),
    ok.

max_writers(_Config) ->
    Trk0 = osiris_tracking:init(undefined, #{max_sequences => 4}),
    Trk1 = lists:foldl(
      fun(I, T0) ->
        osiris_tracking:add(integer_to_binary(I), sequence, I, I, T0)
      end, Trk0, lists:seq(1, 8)),
    [?assertEqual({ok, {I, I}}, osiris_tracking:query(integer_to_binary(I), sequence, Trk1))
    || I <- lists:seq(1, 8)],

    {_, Trk} = osiris_tracking:snapshot(1, 1, Trk1),
    [?assertEqual({error, not_found}, osiris_tracking:query(integer_to_binary(I), sequence, Trk))
    || I <- lists:seq(1, 4)],

    [?assertEqual({ok, {I, I}}, osiris_tracking:query(integer_to_binary(I), sequence, Trk))
    || I <- lists:seq(5, 8)],
    ok.

snapshot_excludes_unwritten(_Config) ->
    %% A snapshot is written to the log before the chunk that carries the
    %% pending tracking deltas, so it must only ever contain tracking that is
    %% already in the log - otherwise a crash in between the two chunks would
    %% recover tracking that is ahead of the log data.
    T0 = osiris_tracking:init(undefined, #{}),
    ChId1 = 1,
    T1 = osiris_tracking:add(<<"w1">>, sequence, 55, ChId1, T0),
    T2 = osiris_tracking:add(<<"t1">>, offset, 99, ChId1, T1),
    T3 = osiris_tracking:add(<<"t2">>, timestamp, 12345, ChId1, T2),
    %% queries must still see the pending values, deduplication relies on it
    ?assertEqual({ok, {ChId1, 55}}, osiris_tracking:query(<<"w1">>, sequence, T3)),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T3)),
    ?assertEqual({ok, 12345}, osiris_tracking:query(<<"t2">>, timestamp, T3)),
    %% nothing is in the log yet, so there is nothing to snapshot
    {Snap0, T4} = osiris_tracking:snapshot(0, 0, T3),
    ?assertEqual(<<>>, iolist_to_binary(Snap0)),
    %% taking a snapshot must not swallow the pending deltas, they still have to
    %% be written into the chunk that follows the snapshot
    ?assert(osiris_tracking:needs_flush(T4)),
    {Trailer0, T5} = osiris_tracking:flush(T4),
    ?assertEqual(#{sequences => #{<<"w1">> => {ChId1, 55}},
                   offsets => #{<<"t1">> => 99},
                   timestamps => #{<<"t2">> => 12345}},
                 trailer_overview(ChId1, Trailer0)),

    %% now that they are in the log they have to appear in a snapshot
    {Snap1, T6} = osiris_tracking:snapshot(0, 0, T5),
    ?assertEqual(#{sequences => #{<<"w1">> => {ChId1, 55}},
                   offsets => #{<<"t1">> => 99},
                   timestamps => #{<<"t2">> => 12345}},
                 snapshot_overview(Snap1)),

    %% an unflushed update on top of a written value falls back to the written
    %% value rather than being left out
    ChId2 = 2,
    T7 = osiris_tracking:add(<<"w1">>, sequence, 56, ChId2, T6),
    T8 = osiris_tracking:add(<<"t1">>, offset, 100, ChId2, T7),
    ?assertEqual({ok, {ChId2, 56}}, osiris_tracking:query(<<"w1">>, sequence, T8)),
    {Snap2, T9} = osiris_tracking:snapshot(0, 0, T8),
    ?assertEqual(#{sequences => #{<<"w1">> => {ChId1, 55}},
                   offsets => #{<<"t1">> => 99},
                   timestamps => #{<<"t2">> => 12345}},
                 snapshot_overview(Snap2)),
    %% and the pending update is still flushed into its own chunk
    {Trailer1, T10} = osiris_tracking:flush(T9),
    ?assertEqual(#{sequences => #{<<"w1">> => {ChId2, 56}},
                   offsets => #{<<"t1">> => 100},
                   timestamps => #{}},
                 trailer_overview(ChId2, Trailer1)),
    {Snap3, _} = osiris_tracking:snapshot(0, 0, T10),
    ?assertEqual(#{sequences => #{<<"w1">> => {ChId2, 56}},
                   offsets => #{<<"t1">> => 100},
                   timestamps => #{<<"t2">> => 12345}},
                 snapshot_overview(Snap3)),
    ok.

%% round trip through the module's own parsers rather than asserting on the
%% binaries, which are not written in a defined order
snapshot_overview(Snap) ->
    osiris_tracking:overview(
      osiris_tracking:init(iolist_to_binary(Snap), #{})).

trailer_overview(ChId, Trailer) ->
    osiris_tracking:overview(
      osiris_tracking:append_trailer(ChId, iolist_to_binary(Trailer),
                                     osiris_tracking:init(undefined, #{}))).

recover(_Config) ->
    ChId1 = ?LINE,
    Now = erlang:system_time(millisecond),
    SnapBin = <<?TRK_TYPE_OFFSET:8/unsigned,
                2:8/unsigned,
                "t1",
                99:64/unsigned,
                ?TRK_TYPE_TIMESTAMP:8/unsigned,
                2:8/unsigned,
                "t2",
                Now:64/signed,
                ?TRK_TYPE_SEQUENCE:8/unsigned,
                2:8/unsigned,
                "w1",
                ChId1 :64/unsigned,
                55:64/unsigned>>,

    T0 = osiris_tracking:init(SnapBin, #{}),
    ?assertEqual({ok, {ChId1, 55}}, osiris_tracking:query(<<"w1">>, sequence, T0)),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T0)),
    ?assertEqual({ok, Now}, osiris_tracking:query(<<"t2">>, timestamp, T0)),

    Trailer = <<?TRK_TYPE_OFFSET:8/unsigned,
                2:8/unsigned,
                "t3",
                103:64/unsigned,
                ?TRK_TYPE_TIMESTAMP:8/unsigned,
                2:8/unsigned,
                "t4",
                11:64/signed,
                ?TRK_TYPE_SEQUENCE:8/unsigned,
                2:8/unsigned,
                "w2",
                77:64/unsigned>>,

    ChId2 = ?LINE,
    T1 = osiris_tracking:append_trailer(ChId2, Trailer, T0),
    ?assertEqual({ok, {ChId1, 55}}, osiris_tracking:query(<<"w1">>, sequence, T1)),
    ?assertEqual({ok, {ChId2, 77}}, osiris_tracking:query(<<"w2">>, sequence, T1)),
    ?assertEqual({ok, 99}, osiris_tracking:query(<<"t1">>, offset, T1)),
    ?assertEqual({ok, Now}, osiris_tracking:query(<<"t2">>, timestamp, T1)),
    ?assertEqual({ok, 103}, osiris_tracking:query(<<"t3">>, offset, T1)),
    ?assertEqual({ok, 11}, osiris_tracking:query(<<"t4">>, timestamp, T1)),
    ok.
