-module(osiris_log_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     init_empty,
     init_recover,
     init_with_lower_epoch,
     write_batch,
     subbatch,
     read_chunk_parsed,
     read_chunk_parsed_multiple_chunks,
     write_multi_log,
     tail_info_empty,
     tail_info,
     init_offset_reader_empty,
     init_offset_reader,
     init_offset_reader_truncated,
     init_data_reader_empty_log,
     init_data_reader_truncated,
     init_epoch_offsets_empty,
     init_epoch_offsets,
     init_epoch_offsets_multi_segment,
     init_epoch_offsets_multi_segment2,
     % truncate,
     % truncate_multi_segment,
     accept_chunk_truncates_tail,
     overview,
     evaluate_retention
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    LeaderDir = filename:join(Dir, "leader"),
    Follower1Dir = filename:join(Dir, "follower1"),
    [{test_case, TestCase},
     {leader_dir, LeaderDir},
     {follower1_dir, Follower1Dir},
     {dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_empty(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    ok.

init_recover(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    S1 = osiris_log:write([<<"hi">>], S0),
    % ?assertEqual([{0, <<"hi">>}], S2S1 = osiris_log:write([<<"hi">>]. S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok = osiris_log:close(S1),
    S2 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    ?assertEqual(1, osiris_log:next_offset(S2)),
    ok.

init_with_lower_epoch(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config),
                           max_segment_size => 10 * 1000 * 1000,
                           epoch => 1}),
    S1 = osiris_log:write([<<"hi">>, <<"hi-there">>], S0),
    %% same is ok
    osiris_log:close(S1),
    _ = osiris_log:close(
          osiris_log:init(#{dir => ?config(dir, Config),
                            max_segment_size => 10 * 1000 * 1000,
                            epoch => 1})),
    %% higher is always ok
    _ = osiris_log:close(
          osiris_log:init(#{dir => ?config(dir, Config),
                            max_segment_size => 10 * 1000 * 1000,
                            epoch => 2})),
    %% lower is not ok
    ?assertException(exit, invalid_epoch,
                     osiris_log:init(#{dir => ?config(dir, Config),
                                       max_segment_size => 10 * 1000 * 1000,
                                       epoch => 0})),
    ok.

write_batch(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    S1 = osiris_log:write([<<"hi">>], S0),
    % ?assertEqual([{0, <<"hi">>}], S2S1 = osiris_log:write([<<"hi">>]. S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok.

subbatch(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    IOData = [
              <<0:1, 2:31/unsigned, "hi">>,
              <<0:1, 2:31/unsigned, "h0">>
              ],
    CompType = 0, %% no compression
    Batch = {batch, 2, CompType, IOData},
    S1 = osiris_log:write([Batch, <<"simple">>], S0),
    % ?assertEqual([{0, <<"hi">>}], S2S1 = osiris_log:write([<<"hi">>]. S0),
    ?assertEqual(3, osiris_log:next_offset(S1)),
    OffRef = atomics:new(1, []),
    atomics:put(OffRef, 1, -1), %% the initial value
    {ok, R0} = osiris_log:init_offset_reader(0, #{dir => ?config(dir, Config),
                                                  offset_ref => OffRef}),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    atomics:put(OffRef, 1, 0), %% first chunk index

    ?assertMatch({[{0, <<"hi">>},
                   {1, <<"h0">>},
                   {2, <<"simple">>}], _},
                 osiris_log:read_chunk_parsed(R1)),

    osiris_log:close(S1),
    osiris_log:close(R1),
    ok.

read_chunk_parsed(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    _S1 = osiris_log:write([<<"hi">>], S0),
    {ok, R0} = osiris_log:init_data_reader({0, empty},
                                     #{dir => ?config(dir, Config)}),
    ?assertMatch({[{0, <<"hi">>}], _},
                 osiris_log:read_chunk_parsed(R0)),
    ok.

read_chunk_parsed_multiple_chunks(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config), epoch => 1}),
    S1 = osiris_log:write([<<"hi">>, <<"hi-there">>], S0),
    _S2 = osiris_log:write([<<"hi-again">>], S1),
    {ok, R0} = osiris_log:init_data_reader({0, empty},
                                           #{dir => ?config(dir, Config)}),
    {[{0, <<"hi">>}, {1, <<"hi-there">>}], R1} =
        osiris_log:read_chunk_parsed(R0),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R1)),
    %% open another reader at a later index
    {ok, R2} = osiris_log:init_data_reader({2, {1, 0}},
                                           #{dir => ?config(dir, Config)}),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R2)),
    ok.

write_multi_log(Config) ->
    S0 = osiris_log:init(#{dir => ?config(dir, Config),
                           epoch => 1,
                           max_segment_size => 10 * 1000 * 1000}),
    Data = crypto:strong_rand_bytes(10000),
    BatchOf10 = [Data || _ <- lists:seq(1, 10)],
    _S1 = lists:foldl(
           fun (_, Acc) ->
                   osiris_log:write(BatchOf10, Acc)
           end, S0, lists:seq(1, 101)),
    Segments = filelib:wildcard(filename:join(?config(dir, Config), "*.segment")),
    ?assertEqual(2, length(Segments)),

    %% ensure all records can be read
    {ok, R0} = osiris_log:init_data_reader({0, empty},
                                           #{dir => ?config(dir, Config)}),

    R1 = lists:foldl(
                fun (_, Acc0) ->
                        {Records = [_|_], Acc} = osiris_log:read_chunk_parsed(Acc0),

                        ?assert(is_list(Records)),
                        % ct:pal("offsets ~w", [element(1, lists:unzip(Records))]),
                        ?assertEqual(10, length(Records)),
                        Acc
                end, R0, lists:seq(1, 101)),
    ?assertEqual(1010, osiris_log:next_offset(R1)),
    ok.

tail_info_empty(Config) ->
    Log = osiris_log:init(#{dir => ?config(dir, Config), epoch => 0}),
    %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ?assertEqual({0, empty}, osiris_log:tail_info(Log)),
    osiris_log:close(Log),
    ok.

tail_info(Config) ->
    EChunks = [{1, [<<"one">>]},
               {2, [<<"two">>]},
               {4, [<<"three">>, <<"four">>]}
              ],
    Log  = seed_log(?config(dir, Config), EChunks, Config),
    %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ?assertEqual({4, {4, 2}}, osiris_log:tail_info(Log)),
    osiris_log:close(Log),
    ok.

init_offset_reader_empty(Config) ->
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, [], Config),
    osiris_log:close(LLog0),
    RConf = #{dir => LDir, offset_ref => ?FUNCTION_NAME},
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
    EpochChunks = [{1, [<<"one">>]},
                   {2, [<<"two">>]},
                   {3, [<<"three">>, <<"four">>]}
                  ],
    LDir = ?config(leader_dir, Config),
    LLog0  = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LLog0),
    RConf = #{dir => LDir, offset_ref => ?FUNCTION_NAME},

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

init_offset_reader_truncated(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks = [begin
                       {1, [Data || _ <- lists:seq(1, 50)]}
                   end || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    LLog0  = seed_log(LDir, EpochChunks, Config),
    RConf = #{dir => LDir,
              offset_ref => ?FUNCTION_NAME},
    osiris_log:close(LLog0),

    %% "Truncate" log by deleting first segment
    ok = file:delete(filename:join(LDir, "00000000000000000000.index")),
    ok = file:delete(filename:join(LDir, "00000000000000000000.segment")),

    {ok, L1} = osiris_log:init_offset_reader(first, RConf),
    %% we can only check reliably that it is larger than 0
    ?assert(0 < osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    {ok, L2} = osiris_log:init_offset_reader(last, RConf),
    %% the last batch offset should be 949 given 50 records per batch
    ?assertEqual(950, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    {ok, L3} = osiris_log:init_offset_reader(next, RConf),
    %% the last offset should be 999
    ?assertEqual(1000, osiris_log:next_offset(L3)),
    osiris_log:close(L3),

    {ok, L4} = osiris_log:init_offset_reader(1001, RConf),
    %% higher = next
    ?assertEqual(1000, osiris_log:next_offset(L4)),
    osiris_log:close(L4),

    {ok, L5} = osiris_log:init_offset_reader(5, RConf),
    %% lower = first
    ?assert(5 < osiris_log:next_offset(L5)),
    osiris_log:close(L5),
    ok.

init_data_reader_empty_log(Config) ->
    LDir = ?config(leader_dir, Config),
    LLog0  = seed_log(LDir, [], Config),
    %% an empty log
    FLog0 = seed_log(?config(follower1_dir, Config), [], Config),
    RRConf = #{dir => ?config(leader_dir, Config)},
    % FTail = osiris_log:tail_info(FLog0),
    % ct:pal("Tail info ~w", [FTail]),
    %% the next offset, i.e. offset 0
    {ok, RLog0} = osiris_log:init_data_reader(
                    osiris_log:tail_info(FLog0), RRConf),
    osiris_log:close(RLog0),
    %% too large
    {error, {offset_out_of_range, empty}} =
        osiris_log:init_data_reader({1, {0, 0}}, RRConf),

    LLog = osiris_log:write([<<"hi">>], LLog0),

    %% init after write should also work
    {ok, RLog1} = osiris_log:init_data_reader(
                    osiris_log:tail_info(FLog0), RRConf),
    ?assertEqual(0, osiris_log:next_offset(RLog1)),
    osiris_log:close(RLog1),

    ok = osiris_log:close(LLog),
    ok.

init_data_reader_truncated(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks = [begin
                       {1, [Data || _ <- lists:seq(1, 50)]}
                   end || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    LLog0  = seed_log(LDir, EpochChunks, Config),
    RConf = #{dir => LDir,
              offset_ref => ?FUNCTION_NAME},
    osiris_log:close(LLog0),

    %% "Truncate" log by deleting first segment
    ok = file:delete(filename:join(LDir, "00000000000000000000.index")),
    ok = file:delete(filename:join(LDir, "00000000000000000000.segment")),

    %% when requesting a lower offset than the start of the log
    %% it should automatically attach at the first available offset
    {ok, L1} = osiris_log:init_data_reader({0, empty}, RConf),
    ?assert(0 < osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    %% attaching inside the log should be ok too
    {ok, L2} = osiris_log:init_data_reader({750, {1, 700}}, RConf),
    ?assertEqual(750, osiris_log:next_offset(L2)),
    osiris_log:close(L2),

    % %% however attaching with a different epoch should be disallowed
    ?assertEqual({error, {invalid_last_offset_epoch, 2, 1}},
                 osiris_log:init_data_reader({750, {2, 700}}, RConf)),
    osiris_log:close(L2),
    ok.

init_epoch_offsets_empty(Config) ->
    EpochChunks = [{1, [<<"one">>]},
                   {1, [<<"two">>]},
                   {1, [<<"three">>, <<"four">>]}
                  ],
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    EOffs = [],
    Log0 = osiris_log:init_acceptor(EOffs, #{dir => LDir, epoch =>2}),
    {0, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets(Config) ->
    EpochChunks = [{1, [<<"one">>]},
                   {1, [<<"two">>]},
                   {1, [<<"three">>, <<"four">>]}
                  ],
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    EOffs = [{1, 1}],
    Log0 = osiris_log:init_acceptor(EOffs, #{dir => LDir, epoch =>2}),
    {2, {1, 1}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_multi_segment(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks = [begin
                       {1, [Data || _ <- lists:seq(1, 50)]}
                   end || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    osiris_log:close(seed_log(LDir, EpochChunks, Config)),
    ct:pal("~p", [osiris_log:overview(LDir)]),
    EOffs = [{1, 650}],
    Log0 = osiris_log:init_acceptor(EOffs, #{dir => LDir, epoch =>2}),
    {700, {1, 650}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_multi_segment2(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
    [{1, [Data || _ <- lists:seq(1, 50)]} || _ <- lists:seq(1, 15)] ++
    [{2, [Data || _ <- lists:seq(1, 50)]} || _ <- lists:seq(1, 5)],
    LDir = ?config(leader_dir, Config),
    osiris_log:close(seed_log(LDir, EpochChunks, Config)),
    ct:pal("~p", [osiris_log:overview(LDir)]),
    EOffs = [{3, 750}, {1, 650}],
    Log0 = osiris_log:init_acceptor(EOffs, #{dir => LDir, epoch =>2}),
    {700, {1, 650}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

% truncate(Config) ->
%     EpochChunks = [{1, [<<"one">>]},
%                    {1, [<<"two">>]},
%                    {1, [<<"three">>, <<"four">>]}
%                   ],
%     LDir = ?config(leader_dir, Config),
%     Log0  = seed_log(LDir, EpochChunks, Config),
%     {4, {1, 2}} = osiris_log:tail_info(Log0),
%     Log1 = osiris_log:truncate(last_chunk, Log0),
%     {2, {1, 1}} = osiris_log:tail_info(Log1),
%     Log2 = osiris_log:truncate(last_chunk, Log1),
%     {1, {1, 0}} = osiris_log:tail_info(Log2),
%     Log3 = osiris_log:truncate(last_chunk, Log2),
%     {0, empty} = osiris_log:tail_info(Log3),
%     Log = osiris_log:write([<<"on2e">>], Log3),
%     ok = osiris_log:close(Log),
%     ok.


accept_chunk_truncates_tail(Config) ->
    EpochChunks = [{1, [<<"one">>]},
                   {2, [<<"two">>]},
                   {3, [<<"three">>, <<"four">>]}
                  ],
    LDir = ?config(leader_dir, Config),
    LLog  = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertEqual({4, {3, 2}}, LTail), %% {NextOffs, {LastEpoch, LastChunkOffset}}
    ok = osiris_log:close(LLog),

    FollowerEpochChunks =
        [{1, [<<"one">>]},
         {2, [<<"two">>]},
         {2, [<<"three">>]} %% should be truncated next accept
        ],
    FDir = ?config(follower1_dir, Config),
    FLog0 = seed_log(FDir, FollowerEpochChunks, Config),
    osiris_log:close(FLog0),

    {LO, EOffs} = osiris_log:overview(LDir),
    ALog0 = osiris_log:init_acceptor(EOffs, #{dir => FDir, epoch => 2}),
    {ok, RLog0} = osiris_log:init_data_reader(osiris_log:tail_info(ALog0),
                                              #{dir => LDir}),
    {ok, {_, _, Hd, Ch}, _RLog} = osiris_log:read_chunk(RLog0),
    ALog = osiris_log:accept_chunk([Hd, Ch], ALog0),
    osiris_log:close(ALog),
    % validate equal
    ?assertMatch({LO, EOffs}, osiris_log:overview(FDir)),

    ok.

overview(Config) ->
    EpochChunks = [{1, [<<"one">>]},
                   {1, [<<"two">>]},
                   {2, [<<"three">>, <<"four">>]},
                   {2, [<<"five">>]}
                  ],
    LDir = ?config(leader_dir, Config),
    Log0 = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log0),
    {{0, 4}, [{1, 1}, {2, 4}]} = osiris_log:overview(LDir),
    ok.

evaluate_retention(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks = [begin
                       {1, [Data || _ <- lists:seq(1, 50)]}
                   end || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    Log  = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log),
    %% this should delete at least one segment
    Spec = {max_bytes, 1500 * 100},
    ok = osiris_log:evaluate_retention(LDir, Spec),
    SegFiles = filelib:wildcard(filename:join(LDir, "*.segment")),
    ?assertEqual(1, length(SegFiles)),
    ok.

%% Utility


seed_log(Dir, EpochChunks, Config) ->
    Log0 = osiris_log:init(#{dir => Dir,
                             epoch => 1,
                             max_segment_size => 1000 * 1000,
                             name => ?config(test_case, Config)}),
    lists:foldl(
      fun ({Epoch, Records}, Acc0) ->
              case osiris_log:get_current_epoch(Acc0) of
                  Epoch ->
                      osiris_log:write(Records, Acc0);
                  _ ->
                      %% need to re=init
                      osiris_log:close(Acc0),
                      Acc = osiris_log:init(#{dir => Dir,
                                              epoch => Epoch,
                                              name => ?config(test_case, Config)}),
                      osiris_log:write(Records, Acc)
              end
      end, Log0, EpochChunks).



