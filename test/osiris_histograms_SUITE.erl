%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_histograms_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("src/osiris.hrl").

all() ->
    [{group, tests}].

all_tests() ->
    [observe_entry_records_simple_entry,
     observe_entry_records_batch,
     observe_entry_unwraps_filter_value,
     observe_buckets_by_size,
     overview_reports_every_histogram,
     init_is_idempotent,
     observe_without_init_is_a_noop,
     make_chunk_reports_its_size,
     write_observes_chunk,
     accept_chunk_does_not_observe_chunk].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    _ = application:ensure_all_started(logger),
    osiris:configure_logger(logger),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    {ok, _} = application:ensure_all_started(seshat),
    %% creates the 'osiris' seshat group that the histograms register in
    ok = osiris_counters:init(),
    ok = osiris_histograms:init(),
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    [{test_case, TestCase},
     {osiris_conf,
      #{dir => Dir,
        name => atom_to_list(TestCase),
        epoch => 1,
        readers_counter_fun => fun(_) -> ok end,
        shared => osiris_log_shared:new(),
        options => #{}}},
     {dir, Dir}
     | Config].

end_per_testcase(_TestCase, _Config) ->
    ok = osiris_histograms:cleanup(),
    ok = application:stop(seshat),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

observe_entry_records_simple_entry(_Config) ->
    ok = osiris_histograms:observe_entry(<<"hello">>),
    ?assertMatch(#{count := 1, sum := 5}, overview(simple_entry_bytes)),
    ?assertMatch(#{count := 0}, overview(batch_bytes)),
    %% iodata, not just binaries
    ok = osiris_histograms:observe_entry([<<"he">>, [<<"llo">>]]),
    ?assertMatch(#{count := 2, sum := 10}, overview(simple_entry_bytes)),
    ok.

observe_entry_records_batch(_Config) ->
    %% the uncompressed size is recorded, not the size of the body
    Batch = {batch, 3, 0, 4242, <<"compressed">>},
    ok = osiris_histograms:observe_entry(Batch),
    ?assertMatch(#{count := 1, sum := 4242}, overview(batch_bytes)),
    ?assertMatch(#{count := 0}, overview(simple_entry_bytes)),
    ok.

observe_entry_unwraps_filter_value(_Config) ->
    ok = osiris_histograms:observe_entry({<<"filter">>, <<"hello">>}),
    ?assertMatch(#{count := 1, sum := 5}, overview(simple_entry_bytes)),
    ok = osiris_histograms:observe_entry(
           {<<"filter">>, {batch, 1, 0, 99, <<"body">>}}),
    ?assertMatch(#{count := 1, sum := 99}, overview(batch_bytes)),
    ok.

observe_buckets_by_size(_Config) ->
    %% one observation either side of the 1_000 bound
    ok = osiris_histograms:observe_entry(binary:copy(<<"a">>, 500)),
    ok = osiris_histograms:observe_entry(binary:copy(<<"a">>, 5_000)),
    #{buckets := Buckets} = overview(simple_entry_bytes),
    ?assertEqual(1, bucket(1_000, Buckets)),
    ?assertEqual(1, bucket(10_000, Buckets)),
    ?assertEqual(0, bucket(100, Buckets)),
    ?assertEqual(0, bucket(infinity, Buckets)),
    ok.

overview_reports_every_histogram(_Config) ->
    Overview = osiris_histograms:overview(),
    ?assertEqual([batch_bytes, chunk_bytes, simple_entry_bytes],
                 lists:sort(maps:keys(Overview))),
    ok.

%% osiris_sup may restart, re-running init/0. That must not throw away
%% what has been observed so far.
init_is_idempotent(_Config) ->
    ok = osiris_histograms:observe_entry(<<"hello">>),
    ok = osiris_histograms:init(),
    ?assertMatch(#{count := 1, sum := 5}, overview(simple_entry_bytes)),
    ok = osiris_histograms:observe_entry(<<"hello">>),
    ?assertMatch(#{count := 2, sum := 10}, overview(simple_entry_bytes)),
    ok.

%% Instrumentation runs on the write path and must never take a writer
%% down, so observing before init/0 has run is a no-op rather than a crash.
observe_without_init_is_a_noop(_Config) ->
    ok = osiris_histograms:cleanup(),
    ?assertEqual(ok, osiris_histograms:observe_entry(<<"hello">>)),
    ?assertEqual(ok, osiris_histograms:observe_entry({batch, 1, 0, 9, <<>>})),
    ?assertEqual(ok, osiris_histograms:observe_chunk(1234)),
    ?assertEqual(#{}, osiris_histograms:overview()),
    %% re-init so end_per_testcase's cleanup/0 is symmetrical
    ok = osiris_histograms:init(),
    ok.

%% make_chunk/7 reports the size of the iodata it built, so that neither
%% write/5 nor write_chunk/7 has to traverse the chunk to find it. The
%% value also drives segment size accounting, so it has to be exact.
make_chunk_reports_its_size(_Config) ->
    Cases = [{[<<"hi">>], <<>>},
             {[<<"hi">>, <<"there">>], <<>>},
             {[{<<"filter">>, <<"blob">>}], <<>>},
             {[<<"hi">>], <<"trailer-data">>},
             {[iolist_to_binary(lists:duplicate(1000, $x))], <<>>}],
    [begin
         {Chunk, _NumRecords, Size} =
             osiris_log:make_chunk(Entries, Trailer, 0, ?LINE, 1, 0,
                                   ?DEFAULT_FILTER_SIZE),
         ?assertEqual(iolist_size(Chunk), Size)
     end || {Entries, Trailer} <- Cases],
    ok.

write_observes_chunk(Config) ->
    S0 = osiris_log:init(?config(osiris_conf, Config)),
    S1 = osiris_log:write([<<"hi">>], S0),
    osiris_log:close(S1),
    {_Chunk, _NumRecords, ExpectedSize} =
        osiris_log:make_chunk([<<"hi">>], <<>>, 0, ?LINE, 1, 0,
                              ?DEFAULT_FILTER_SIZE),
    #{count := Count, sum := Sum} = overview(chunk_bytes),
    ?assertEqual(1, Count),
    ?assertEqual(ExpectedSize, Sum),
    ok.

%% Chunks accepted from a leader are written by a replica, not produced by
%% a writer on this node, so they are deliberately not counted.
accept_chunk_does_not_observe_chunk(Config) ->
    Conf = ?config(osiris_conf, Config),
    Chunk = iolist_to_binary(
              element(1, osiris_log:make_chunk([<<"hi">>], <<>>, 0, ?LINE, 1,
                                               100, ?DEFAULT_FILTER_SIZE))),
    F0 = osiris_log:init(Conf#{initial_offset => 100}, acceptor),
    F1 = osiris_log:accept_chunk(Chunk, F0),
    ?assertEqual(101, osiris_log:next_offset(F1)),
    osiris_log:close(F1),
    ?assertMatch(#{count := 0, sum := 0}, overview(chunk_bytes)),
    ok.

%%%===================================================================
%%% Helpers
%%%===================================================================

overview(Histogram) ->
    maps:get(Histogram, osiris_histograms:overview()).

bucket(UpperBound, Buckets) ->
    {UpperBound, Count} = lists:keyfind(UpperBound, 1, Buckets),
    Count.
