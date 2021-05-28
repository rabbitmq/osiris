%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_log_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("src/osiris.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() ->
    [init_empty,
     init_twice,
     init_recover,
     init_recover_with_writers,
     init_with_lower_epoch,
     write_batch,
     subbatch,
     read_chunk_parsed,
     read_chunk_parsed_multiple_chunks,
     read_header,
     write_multi_log,
     tail_info_empty,
     tail_info,
     init_offset_reader_empty,
     init_offset_reader,
     init_offset_reader_timestamp,
     init_offset_reader_truncated,
     init_data_reader_empty_log,
     init_data_reader_truncated,
     init_epoch_offsets_empty,
     init_epoch_offsets_empty_writer,
     init_epoch_offsets_truncated_writer,
     init_epoch_offsets,
     init_epoch_offsets_multi_segment,
     init_epoch_offsets_multi_segment2,
     % truncate,
     % truncate_multi_segment,
     accept_chunk,
     accept_chunk_truncates_tail,
     accept_chunk_does_not_truncate_tail_in_same_epoch,
     accept_chunk_in_other_epoch,
     overview,
     evaluate_retention_max_bytes,
     evaluate_retention_max_age,
     offset_tracking,
     offset_tracking_snapshot,
     offset_tracking_empty].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    osiris:configure_logger(logger),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    osiris:configure_logger(logger),
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    LeaderDir = filename:join(Dir, "leader"),
    Follower1Dir = filename:join(Dir, "follower1"),
    ORef = atomics:new(2, [{signed, true}]),
    [{test_case, TestCase},
     {leader_dir, LeaderDir},
     {follower1_dir, Follower1Dir},
     {osiris_conf,
      #{dir => Dir,
        name => atom_to_list(TestCase),
        epoch => 1,
        readers_counter_fun => fun(_) -> ok end,
        offset_ref => ORef}},
     {dir, Dir}
     | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_empty(Config) ->
    S0 = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    ok.

init_twice(Config) ->
    _S0 = osiris_log:init(?config(osiris_conf, Config)),
    S1 = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(0, osiris_log:next_offset(S1)),
    ok.

init_recover(Config) ->
    S0 = osiris_log:init(?config(osiris_conf, Config)),
    S1 = osiris_log:write([<<"hi">>], S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok = osiris_log:close(S1),
    S2 = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(1, osiris_log:next_offset(S2)),
    ok.

init_recover_with_writers(Config) ->
    S0 = osiris_log:init(?config(osiris_conf, Config)),
    Now = erlang:system_time(millisecond),
    Writers = #{<<"wid1">> => 1},
    S1 = osiris_log:write([<<"hi">>], ?CHNK_USER, Now, Writers, S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok = osiris_log:close(S1),
    S2 = osiris_log:init(?config(osiris_conf, Config)),
    ?assertMatch(#{<<"wid1">> := {0, Now, 1}}, osiris_log:writers(S2)),
    ?assertEqual(1, osiris_log:next_offset(S2)),
    ok.

init_with_lower_epoch(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf#{max_segment_size_bytes => 10 * 1000 * 1000}),
    S1 = osiris_log:write([<<"hi">>, <<"hi-there">>], S0),
    %% same is ok
    osiris_log:close(S1),
    _ = osiris_log:close(
            osiris_log:init(Conf)),
    %% higher is always ok
    _ = osiris_log:close(
            osiris_log:init(Conf#{epoch => 2})),
    %% lower is not ok
    ?assertException(exit, {invalid_epoch, _, _},
                     osiris_log:init(Conf#{epoch => 0})),
    ok.

write_batch(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    S1 = osiris_log:write([<<"hi">>], S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok.

subbatch(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    IOData = [<<0:1, 2:31/unsigned, "hi">>, <<0:1, 2:31/unsigned, "h0">>],
    CompType = 0, %% no compression
    Batch = {batch, 2, CompType, IOData},
    %% osiris_writer passes entries in reverse order
    S1 = osiris_log:write(
             lists:reverse([Batch, <<"simple">>]), S0),
    ?assertEqual(3, osiris_log:next_offset(S1)),
    OffRef = atomics:new(1, []),
    atomics:put(OffRef, 1, -1), %% the initial value
    {ok, R0} =
        osiris_log:init_offset_reader(0, Conf#{offset_ref => OffRef}),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    atomics:put(OffRef, 1, 0), %% first chunk index

    ?assertMatch({[{0, <<"hi">>}, {1, <<"h0">>}, {2, <<"simple">>}], _},
                 osiris_log:read_chunk_parsed(R1)),

    osiris_log:close(S1),
    osiris_log:close(R1),
    ok.

read_chunk_parsed(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    {ok, R0} = osiris_log:init_data_reader({0, empty}, Conf),
    ct:pal("before"),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    ct:pal("empty"),
    _S1 = osiris_log:write([<<"hi">>], S0),
    ?assertMatch({[{0, <<"hi">>}], _}, osiris_log:read_chunk_parsed(R1)),
    ok.

read_chunk_parsed_multiple_chunks(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    Entries = [<<"hi">>, <<"hi-there">>],
    %% osiris_writer passes entries in reversed order
    S1 = osiris_log:write(
             lists:reverse(Entries), S0),
    _S2 = osiris_log:write([<<"hi-again">>], S1),
    {ok, R0} = osiris_log:init_data_reader({0, empty}, Conf),
    {[{0, <<"hi">>}, {1, <<"hi-there">>}], R1} =
        osiris_log:read_chunk_parsed(R0),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R1)),
    %% open another reader at a later index
    {ok, R2} = osiris_log:init_data_reader({2, {1, 0, 0}}, Conf),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R2)),
    ok.

read_header(Config) ->
    Conf = ?config(osiris_conf, Config),
    W0 = osiris_log:init(Conf),
    OffRef = atomics:new(2, []),
    {ok, R0} =
        osiris_log:init_offset_reader(first, Conf#{offset_ref => OffRef}),
    {end_of_stream, R1} = osiris_log:read_header(R0),
    W1 = osiris_log:write([<<"hi">>, <<"ho">>], W0),
    _W = osiris_log:write([<<"hum">>], W1),
    atomics:put(OffRef, 1, 3),
    {ok, H1, R2} = osiris_log:read_header(R1),
    ?assertMatch(#{chunk_id := 0,
                   epoch := 1,
                   type := 0,
                   num_records := 2,
                   num_entries := 2,
                   timestamp := _,
                   data_size := _,
                   trailer_size := 0},
                 H1),
    {ok, H2, R3} = osiris_log:read_header(R2),
    ?assertMatch(#{chunk_id := 2,
                   epoch := 1,
                   type := 0,
                   num_records := 1,
                   num_entries := 1,
                   timestamp := _,
                   data_size := _,
                   trailer_size := 0},
                 H2),
    {end_of_stream, _R} = osiris_log:read_header(R3),
    ok.

write_multi_log(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf#{max_segment_size_bytes => 10 * 1000 * 1000}),
    Data = crypto:strong_rand_bytes(10000),
    BatchOf10 = [Data || _ <- lists:seq(1, 10)],
    _S1 = lists:foldl(fun(_, Acc) -> osiris_log:write(BatchOf10, Acc) end,
                      S0, lists:seq(1, 101)),
    Segments =
        filelib:wildcard(
            filename:join(?config(dir, Config), "*.segment")),
    ?assertEqual(2, length(Segments)),

    OffRef = atomics:new(2, []),
    atomics:put(OffRef, 1,
                1011), %% takes a single offset tracking data into account
    %% ensure all records can be read
    {ok, R0} =
        osiris_log:init_offset_reader(first, Conf#{offset_ref => OffRef}),

    R1 = lists:foldl(fun(_, Acc0) ->
                        {Records = [_ | _], Acc} =
                            osiris_log:read_chunk_parsed(Acc0),

                        ?assert(is_list(Records)),
                        % ct:pal("offsets ~w", [element(1, lists:unzip(Records))]),
                        ?assertEqual(10, length(Records)),
                        Acc
                     end,
                     R0, lists:seq(1, 101)),
    ?assertEqual(1010, osiris_log:next_offset(R1)),
    ok.

tail_info_empty(Config) ->
    Conf = ?config(osiris_conf, Config),
    Log = osiris_log:init(Conf),
    %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ?assertEqual({0, empty}, osiris_log:tail_info(Log)),
    osiris_log:close(Log),
    ok.

tail_info(Config) ->
    EChunks =
        [{1, [<<"one">>]}, {2, [<<"two">>]}, {4, [<<"three">>, <<"four">>]}],
    Log = seed_log(?config(dir, Config), EChunks, Config),
    %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ?assertMatch({4, {4, 2, _}}, osiris_log:tail_info(Log)),
    osiris_log:close(Log),
    ok.

init_offset_reader_empty(Config) ->
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, [], Config),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir, offset_ref => ?FUNCTION_NAME},
    %% first and last falls back to next
    {ok, L1} = osiris_log:init_offset_reader(first, RConf),
    {ok, L2} = osiris_log:init_offset_reader(last, RConf),
    {ok, L3} = osiris_log:init_offset_reader(next, RConf),
    %% "larger" offset should fall back to next
    {ok, L4} = osiris_log:init_offset_reader(0, RConf),
    %% all 4 should have a next index of 0
    ?assertEqual(0, osiris_log:next_offset(L1)),
    ?assertEqual(0, osiris_log:next_offset(L2)),
    ?assertEqual(0, osiris_log:next_offset(L3)),
    ?assertEqual(0, osiris_log:next_offset(L4)),
    osiris_log:close(L1),
    osiris_log:close(L2),
    osiris_log:close(L3),
    osiris_log:close(L4),

    {error, {offset_out_of_range, empty}} =
        osiris_log:init_offset_reader({abs, 1}, RConf),
    ok.

init_offset_reader(Config) ->
    EpochChunks =
        [{1, [<<"one">>]}, {2, [<<"two">>]}, {3, [<<"three">>, <<"four">>]}],
    LDir = ?config(leader_dir, Config),
    Conf = ?config(osiris_conf, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir, offset_ref => ?FUNCTION_NAME},

    {ok, L1} = osiris_log:init_offset_reader(first, RConf),
    ?assertEqual(0, osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    {ok, L2} = osiris_log:init_offset_reader(last, RConf),
    %% 3 is the actual last ones but we can only attach to the chunk offset
    %% containing the requested offset
    ?assertEqual(2, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    {ok, L3} = osiris_log:init_offset_reader(next, RConf),
    ?assertEqual(4, osiris_log:next_offset(L3)),
    osiris_log:close(L3),

    %% "larger" offset should fall back to next
    {ok, L4} = osiris_log:init_offset_reader(0, RConf),
    ?assertEqual(0, osiris_log:next_offset(L4)),
    osiris_log:close(L4),

    %% 2 is the chunk offset containin offset 2 and 3,
    {ok, L5} = osiris_log:init_offset_reader({abs, 3}, RConf),
    ?assertEqual(2, osiris_log:next_offset(L5)),
    osiris_log:close(L5),

    {ok, L6} = osiris_log:init_offset_reader({abs, 2}, RConf),
    ?assertEqual(2, osiris_log:next_offset(L6)),
    osiris_log:close(L6),

    {error, {offset_out_of_range, {0, 3}}} =
        osiris_log:init_offset_reader({abs, 4}, RConf),
    {error, {offset_out_of_range, {0, 3}}} =
        osiris_log:init_offset_reader({abs, 6}, RConf),
    ok.

init_offset_reader_timestamp(Config) ->
    ok = logger:set_primary_config(level, all),
    Now = now_ms(),
    EpochChunks =
        [{1, Now - 10000, [<<"one">>]}, % 0
         {1, Now - 8000, [<<"two">>]},  % 1
         {1, Now - 5000, [<<"three">>, <<"four">>]}], % 2
    LDir = ?config(leader_dir, Config),
    Conf = ?config(osiris_conf, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir, offset_ref => ?FUNCTION_NAME},

    {ok, L1} =
        osiris_log:init_offset_reader({timestamp, Now - 8000}, RConf),
    %% next offset is expected to be offset 1
    ?assertEqual(1, osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    %% future case
    {ok, L2} = osiris_log:init_offset_reader({timestamp, Now}, RConf),
    ?assertEqual(4, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    %% past case
    {ok, L3} =
        osiris_log:init_offset_reader({timestamp, Now - 10000}, RConf),
    ?assertEqual(0, osiris_log:next_offset(L3)),
    osiris_log:close(L3),
    ok.

init_offset_reader_truncated(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    RConf = Conf#{dir => LDir, offset_ref => ?FUNCTION_NAME},
    osiris_log:close(LLog0),

    %% "Truncate" log by deleting first segment
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.index")),
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.segment")),

    {ok, L1} = osiris_log:init_offset_reader(first, RConf),
    %% we can only check reliably that it is larger than 0
    ?assert(0 < osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    {ok, L2} = osiris_log:init_offset_reader(last, RConf),
    %% the last batch offset should be 949 given 50 records per batch
    ?assertEqual(950, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    {ok, L3} = osiris_log:init_offset_reader(next, RConf),
    %% the last offset should be 999 + 1
    ?assertEqual(1000, osiris_log:next_offset(L3)),
    osiris_log:close(L3),

    {ok, L4} = osiris_log:init_offset_reader(1000, RConf),
    %% higher = next
    ?assertEqual(1000, osiris_log:next_offset(L4)),
    osiris_log:close(L4),

    {ok, L5} = osiris_log:init_offset_reader(5, RConf),
    %% lower = first
    ?assert(5 < osiris_log:next_offset(L5)),
    osiris_log:close(L5),
    ok.

init_data_reader_empty_log(Config) ->
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, [], Config),
    %% an empty log
    FLog0 = seed_log(?config(follower1_dir, Config), [], Config),
    RRConf = Conf#{dir => ?config(leader_dir, Config)},
    % FTail = osiris_log:tail_info(FLog0),
    % ct:pal("Tail info ~w", [FTail]),
    %% the next offset, i.e. offset 0
    {ok, RLog0} =
        osiris_log:init_data_reader(
            osiris_log:tail_info(FLog0), RRConf),
    osiris_log:close(RLog0),
    %% too large
    {error, {offset_out_of_range, empty}} =
        osiris_log:init_data_reader({1, {0, 0, 0}}, RRConf),

    LLog = osiris_log:write([<<"hi">>], LLog0),

    %% init after write should also work
    {ok, RLog1} =
        osiris_log:init_data_reader(
            osiris_log:tail_info(FLog0), RRConf),
    ?assertEqual(0, osiris_log:next_offset(RLog1)),
    osiris_log:close(RLog1),

    ok = osiris_log:close(LLog),
    ok.

init_data_reader_truncated(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    RConf = Conf#{dir => LDir, offset_ref => ?FUNCTION_NAME},
    osiris_log:close(LLog0),

    %% "Truncate" log by deleting first segment
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.index")),
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.segment")),

    %% when requesting a lower offset than the start of the log
    %% it should automatically attach at the first available offset
    {ok, L1} = osiris_log:init_data_reader({0, empty}, RConf),
    ?assert(0 < osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    %% attaching inside the log should be ok too
    {ok, L2} = osiris_log:init_data_reader({750, {1, 700, ?LINE}}, RConf),
    ?assertEqual(750, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    % %% however attaching with a different epoch should be disallowed
    ?assertEqual({error, {invalid_last_offset_epoch, 2, 1}},
                 osiris_log:init_data_reader({750, {2, 700, ?LINE}}, RConf)),
    osiris_log:close(L2),
    ok.

init_epoch_offsets_empty(Config) ->
    EpochChunks =
        [{1, [<<"one">>]}, {1, [<<"two">>]}, {1, [<<"three">>, <<"four">>]}],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    FDir = ?config(follower1_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    EOffs = [{1, 0}],
    Log0 =
        osiris_log:init_acceptor(EOffs, Conf#{dir => FDir, epoch => 1}),
    {0, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_empty_writer(Config) ->
    EpochChunks =
        [{1, [<<"one">>]}, {1, [<<"two">>]}, {1, [<<"three">>, <<"four">>]}],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    EOffs = [],
    Log0 =
        osiris_log:init_acceptor(EOffs, Conf#{dir => LDir, epoch => 2}),
    {0, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_truncated_writer(Config) ->
    %% test acceptor initialisation where the acceptor has no log and the writer
    %% has had retention remove the head of it's log
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    EOffs = [{3, 100}],
    Log0 =
        osiris_log:init_acceptor(EOffs, Conf#{dir => LDir,
                                              epoch => 2,
                                              initial_offset => 100}),
    {100, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),

    ?assert(filelib:is_file(filename:join(LDir, "00000000000000000100.index"))),
    ok.

init_epoch_offsets(Config) ->
    EpochChunks =
        [{1, [<<"one">>]}, {1, [<<"two">>]}, {1, [<<"three">>, <<"four">>]}],
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    EOffs = [{1, 1}],
    Log0 =
        osiris_log:init_acceptor(EOffs,
                                 #{dir => LDir,
                                   name => ?config(test_case, Config),
                                   epoch => 2}),
    {2, {1, 1, _}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_multi_segment(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    osiris_log:close(seed_log(LDir, EpochChunks, Config)),
    ct:pal("~p", [osiris_log:overview(LDir)]),
    EOffs = [{1, 650}],
    Log0 =
        osiris_log:init_acceptor(EOffs,
                                 #{dir => LDir,
                                   name => ?config(test_case, Config),
                                   epoch => 2}),
    {700, {1, 650, _}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_multi_segment2(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [{1, [Data || _ <- lists:seq(1, 50)]} || _ <- lists:seq(1, 15)]
        ++ [{2, [Data || _ <- lists:seq(1, 50)]} || _ <- lists:seq(1, 5)],
    LDir = ?config(leader_dir, Config),
    osiris_log:close(seed_log(LDir, EpochChunks, Config)),
    ct:pal("~p", [osiris_log:overview(LDir)]),
    EOffs = [{3, 750}, {1, 650}],
    Log0 =
        osiris_log:init_acceptor(EOffs,
                                 #{dir => LDir,
                                   name => ?config(test_case, Config),
                                   epoch => 2}),
    {700, {1, 650, _}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

accept_chunk(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(osiris_conf, Config),
    LConf = Conf#{dir => ?config(leader_dir, Config)},
    FConf = Conf#{dir => ?config(follower1_dir, Config)},
    L0 = osiris_log:init(LConf),
    %% write an entry with just tracking
    L1 = osiris_log:write([<<"hi">>], ?CHNK_USER, ?LINE, <<>>, L0),
    % L1 = osiris_log:write_tracking(#{<<"id1">> => {offset, 1}}, delta, L0),
    timer:sleep(100),

    Now = ?LINE,
    L2 = osiris_log:write([<<"hi">>], ?CHNK_USER, Now, <<>>, L1),
    % ?assertMatch(#{<<"w1">> := {_, Now, 1}}, osiris_log:writers(L2)),

    F0 = osiris_log:init(FConf),

    {ok, R0} =
        osiris_log:init_data_reader(
            osiris_log:tail_info(F0), LConf),
    {Chunk1, R1} = read_chunk(R0),
    % ct:pal("Chunk1 ~w", [Chunk1]),
    F1 = osiris_log:accept_chunk(Chunk1, F0),
    {Chunk2, R2} = read_chunk(R1),
    F2 = osiris_log:accept_chunk(Chunk2, F1),

    osiris_log:close(L2),
    osiris_log:close(R2),
    osiris_log:close(F2),
    FL0 = osiris_log:init(FConf),
    % ?assertMatch(#{<<"id1">> := {offset, 1}}, osiris_log:tracking(FL0)),
    % ?assertMatch(#{<<"w1">> := {_, Now, 1}}, osiris_log:writers(FL0)),
    osiris_log:close(FL0),
    ok.

read_chunk(S0) ->
    {ok, {_, _, _, Hd, Ch, Tr}, S1} = osiris_log:read_chunk(S0),
    {[Hd, Ch, Tr], S1}.

accept_chunk_truncates_tail(Config) ->
    ok = logger:set_primary_config(level, all),
    EpochChunks =
        [{1, [<<"one">>]}, {2, [<<"two">>]}, {3, [<<"three">>, <<"four">>]}],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertMatch({4, {3, 2, _}}, LTail), %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ok = osiris_log:close(LLog),

    FollowerEpochChunks =
        [{1, [<<"one">>]}, {2, [<<"two">>]},
         {2, [<<"three">>]}], %% should be truncated next accept
    FDir = ?config(follower1_dir, Config),
    FLog0 = seed_log(FDir, FollowerEpochChunks, Config),
    osiris_log:close(FLog0),

    {LO, EOffs} = osiris_log:overview(LDir),
    ALog0 =
        osiris_log:init_acceptor(EOffs, Conf#{dir => FDir, epoch => 2}),
    {ok, RLog0} =
        osiris_log:init_data_reader(
            osiris_log:tail_info(ALog0), Conf#{dir => LDir}),
    {ok, {_, _, _, Hd, Ch, Tr}, _RLog} = osiris_log:read_chunk(RLog0),
    ALog = osiris_log:accept_chunk([Hd, Ch, Tr], ALog0),
    osiris_log:close(ALog),
    % validate equal
    ?assertMatch({LO, EOffs}, osiris_log:overview(FDir)),
    ok.

accept_chunk_does_not_truncate_tail_in_same_epoch(Config) ->
    ok = logger:set_primary_config(level, all),
    EpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>, <<"two">>]},
         {1, [<<"three">>]},
         {1, [<<"four">>]}],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertMatch({5, {1, 4, _}}, LTail), %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ok = osiris_log:close(LLog),
    FollowerEpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>, <<"two">>]}],
    FDir = ?config(follower1_dir, Config),
    FLog0 = seed_log(FDir, FollowerEpochChunks, Config),
    osiris_log:close(FLog0),

    {_LO, EOffs} = osiris_log:overview(LDir),
    ALog0 = osiris_log:init_acceptor(EOffs, Conf#{dir => FDir, epoch => 2}),
    ATail = osiris_log:tail_info(ALog0),
    osiris_log:close(ALog0),
    %% ensure we don't truncate too much
    ?assertMatch({3, {1, 1, _}}, ATail),
    ok.

accept_chunk_in_other_epoch(Config) ->
    ok = logger:set_primary_config(level, all),
    EpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>, <<"two">>]},
         {1, [<<"three">>]},
         {2, [<<"four">>]}],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertMatch({5, {2, 4, _}}, LTail), %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ok = osiris_log:close(LLog),
    FollowerEpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>, <<"two">>]}],
    FDir = ?config(follower1_dir, Config),
    FLog0 = seed_log(FDir, FollowerEpochChunks, Config),
    osiris_log:close(FLog0),

    {_LO, EOffs} = osiris_log:overview(LDir),
    ALog0 = osiris_log:init_acceptor(EOffs, Conf#{dir => FDir, epoch => 2}),
    ATail = osiris_log:tail_info(ALog0),
    osiris_log:close(ALog0),
    %% ensure we don't truncate too much
    ?assertMatch({3, {1, 1, _}}, ATail),
    ok.

overview(Config) ->
    EpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>]},
         {2, [<<"three">>, <<"four">>]},
         {2, [<<"five">>]}],
    LDir = ?config(leader_dir, Config),
    Log0 = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log0),
    {{0, 4}, [{1, 1}, {2, 4}]} = osiris_log:overview(LDir),
    %% non existant dir should return empty
    {empty, []} = osiris_log:overview("/tmp/blahblah"),
    ok.

evaluate_retention_max_bytes(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    Log = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log),
    %% this should delete at least one segment
    Spec = {max_bytes, 1500 * 100},
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    %% idempotency check
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    SegFiles =
        filelib:wildcard(
            filename:join(LDir, "*.segment")),
    ?assertEqual(1, length(SegFiles)),
    ok.

evaluate_retention_max_age(Config) ->
    Conf = ?config(osiris_conf, Config),
    Data = crypto:strong_rand_bytes(1500),
    %% all chunks are at least 2000ms old
    Ts = now_ms() - 2000,
    EpochChunks =
        [begin {1, Ts, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    LDir = ?config(dir, Config),
    %% this should create at least two segments
    Log = seed_log(Conf#{max_segment_size_bytes => 1000 * 1000}, EpochChunks,
                   Config),
    osiris_log:close(Log),
    SegFilesPre =
        filelib:wildcard(
            filename:join(LDir, "*.segment")),
    ?assertEqual(2, length(SegFilesPre)),
    %% this should delete at least one segment as all chunks should be older
    %% than the retention of 1000ms
    Spec = {max_age, 1000},
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    %% idempotency
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    SegFiles =
        filelib:wildcard(
            filename:join(LDir, "*.segment")),
    ?assertEqual(1, length(SegFiles)),
    ok.

offset_tracking(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    S1 = osiris_log:write_tracking(#{<<"id1">> => {offset, 0}}, delta,
                                   osiris_log:write([<<"hi">>], S0)),
    ?assertEqual(2, osiris_log:next_offset(S1)),
    ?assertMatch(#{<<"id1">> := {offset, 0}}, osiris_log:tracking(S1)),
    S2 = osiris_log:write_tracking(#{<<"id1">> => {offset, 1}}, delta, S1),
    %% test recovery
    osiris_log:close(S2),
    S3 = osiris_log:init(Conf),
    ?assertMatch(#{<<"id1">> := {offset, 1}}, osiris_log:tracking(S3)),
    osiris_log:close(S3),
    ok.

offset_tracking_snapshot(Config) ->
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{max_segment_size_bytes => 1000 * 1000},
    Data = crypto:strong_rand_bytes(1500),
    %% all chunks are at least 2000ms old
    Ts = now_ms() - 2000,
    EpochChunks =
        [begin {1, Ts, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Now = erlang:system_time(millisecond),
    S00 = osiris_log:init(Conf),
    S0 = osiris_log:write([<<"hi">>],
                          ?CHNK_USER,
                          Now,
                          #{<<"wid1">> => 2},
                          S00),
    ?assertMatch(#{<<"wid1">> := {_, Now, 2}}, osiris_log:writers(S0)),
    %% write a tracking entry
    S1 = osiris_log:write_tracking(#{<<"id1">> => {offset, 1}}, delta, S0),
    %% this should create at least two segments
    S2 = seed_log(S1, EpochChunks, Config),
    osiris_log:close(S2),
    S3 = osiris_log:init(Conf),
    ?assertMatch(#{<<"id1">> := {offset, 1}}, osiris_log:tracking(S3)),
    ?assertMatch(#{<<"wid1">> := {_, Now, 2}}, osiris_log:writers(S3)),
    osiris_log:close(S3),
    ok.

offset_tracking_empty(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    S1 = osiris_log:write_tracking(#{}, snapshot, S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    S2 = osiris_log:write([<<"hi">>], S1),
    ?assertEqual(2, osiris_log:next_offset(S2)),
    osiris_log:close(S2),
    ok.

%% Utility

seed_log(Conf, EpochChunks, Config) when is_map(Conf) ->
    Log0 = osiris_log:init(Conf),
    seed_log(Log0, EpochChunks, Config);
seed_log(Dir, EpochChunks, Config) when is_list(Dir) ->
    seed_log(#{dir => Dir,
               epoch => 1,
               max_segment_size_bytes => 1000 * 1000,
               name => ?config(test_case, Config)},
             EpochChunks, Config);
seed_log(Log, EpochChunks, _Config) ->
    lists:foldl(fun ({Epoch, Records}, Acc0) ->
                        write_chunk(Epoch, now_ms(), Records, Acc0);
                    ({Epoch, Ts, Records}, Acc0) ->
                        write_chunk(Epoch, Ts, Records, Acc0)
                end,
                Log, EpochChunks).

write_chunk(Epoch, Now, Records, Log0) ->
    case osiris_log:get_current_epoch(Log0) of
        Epoch ->
            osiris_log:write(Records, Now, Log0);
        _ ->
            %% need to re=init
            Dir = osiris_log:get_directory(Log0),
            Name = osiris_log:get_name(Log0),
            osiris_log:close(Log0),
            Log = osiris_log:init(#{dir => Dir,
                                    epoch => Epoch,
                                    name => Name}),
            osiris_log:write(Records, Log)
    end.

now_ms() ->
    erlang:system_time(millisecond).
