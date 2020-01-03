-module(osiris_segment_SUITE).

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
     write_batch,
     read_chunk_parsed,
     read_chunk_parsed_multiple_chunks,
     write_multi_segment
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
    [{dir, Dir} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_empty(Config) ->
    S0 = osiris_segment:init(?config(dir, Config), #{}),
    ?assertEqual(0, osiris_segment:next_offset(S0)),
    ok.

init_recover(Config) ->
    S0 = osiris_segment:init(?config(dir, Config), #{}),
    S1 = osiris_segment:write([<<"hi">>], S0),
    % ?assertEqual([{0, <<"hi">>}], S2S1 = osiris_segment:write([<<"hi">>]. S0),
    ?assertEqual(1, osiris_segment:next_offset(S1)),
    ok = osiris_segment:close(S1),
    S2 = osiris_segment:init(?config(dir, Config), #{}),
    ?assertEqual(1, osiris_segment:next_offset(S2)),
    ok.

write_batch(Config) ->
    S0 = osiris_segment:init(?config(dir, Config), #{}),
    S1 = osiris_segment:write([<<"hi">>], S0),
    % ?assertEqual([{0, <<"hi">>}], S2S1 = osiris_segment:write([<<"hi">>]. S0),
    ?assertEqual(1, osiris_segment:next_offset(S1)),
    ok.

read_chunk_parsed(Config) ->
    S0 = osiris_segment:init(?config(dir, Config), #{}),
    _S1 = osiris_segment:write([<<"hi">>], S0),
    R0 = osiris_segment:init_reader(0, #{dir => ?config(dir, Config)}),
    ?assertMatch({[{0, <<"hi">>}], _},
                 osiris_segment:read_chunk_parsed(R0)),
    ok.

read_chunk_parsed_multiple_chunks(Config) ->
    S0 = osiris_segment:init(?config(dir, Config), #{}),
    S1 = osiris_segment:write([<<"hi">>, <<"hi-there">>], S0),
    _S2 = osiris_segment:write([<<"hi-again">>], S1),
    R0 = osiris_segment:init_reader(0, #{dir => ?config(dir, Config)}),
    {[{0, <<"hi">>}, {1, <<"hi-there">>}], R1} =
        osiris_segment:read_chunk_parsed(R0),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_segment:read_chunk_parsed(R1)),
    %% open another reader at a later index
    R2 = osiris_segment:init_reader(2, #{dir => ?config(dir, Config)}),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_segment:read_chunk_parsed(R2)),
    ok.

write_multi_segment(Config) ->
    S0 = osiris_segment:init(?config(dir, Config),
                             #{max_segment_size => 10 * 1000 * 1000}),
    Data = crypto:strong_rand_bytes(10000),
    BatchOf10 = [Data || _ <- lists:seq(1, 10)],
    _S1 = lists:foldl(
           fun (_, Acc) ->
                   osiris_segment:write(BatchOf10, Acc)
           end, S0, lists:seq(1, 101)),
    Segs = filelib:wildcard(filename:join(?config(dir, Config), "*.segment")),
    ?assertEqual(2, length(Segs)),

    %% ensure all records can be read
    R0 = osiris_segment:init_reader(0, #{dir => ?config(dir, Config)}),

    R1 = lists:foldl(
                fun (_, Acc0) ->
                        {Records = [_|_], Acc} = osiris_segment:read_chunk_parsed(Acc0),

                        ?assert(is_list(Records)),
                        % ct:pal("offsets ~w", [element(1, lists:unzip(Records))]),
                        ?assertEqual(10, length(Records)),
                        Acc
                end, R0, lists:seq(1, 101)),
    ?assertEqual(1010, osiris_segment:next_offset(R1)),
    ok.

%% Utility
