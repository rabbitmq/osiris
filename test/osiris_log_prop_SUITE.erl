-module(osiris_log_prop_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("proper/include/proper.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("src/osiris.hrl").

all() -> [
    prop_block_binary_search_offset_matches_skip_search,
    prop_block_binary_search_timestamp_matches_skip_search
].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC, Config) ->
    PrivDir = ?config(priv_dir, Config),
    TcDir = filename:join(PrivDir, atom_to_list(TC)),
    ok = filelib:ensure_dir(filename:join(TcDir, "dummy")),
    persistent_term:put({?MODULE, test_dir}, TcDir),
    Config.

end_per_testcase(_TC, _Config) ->
    persistent_term:erase({?MODULE, test_dir}),
    ok.

%% --- Properties ---

-dialyzer([{nowarn_function, prop_block_binary_search_offset_matches_skip_search/1},
           {nowarn_function, prop_block_binary_search_timestamp_matches_skip_search/1}]).

prop_block_binary_search_offset_matches_skip_search(_Config) ->
    true = proper:quickcheck(
             prop_offset_search(),
             [{numtests, 500}, {to_file, user}]).

prop_block_binary_search_timestamp_matches_skip_search(_Config) ->
    true = proper:quickcheck(
             prop_timestamp_search(),
             [{numtests, 500}, {to_file, user}]).

%% --- Property definitions ---

prop_offset_search() ->
    ?FORALL({Deltas, StartOffset, TargetFraction},
            {non_empty(list(range(1, 10))), range(1, 100), float(0.0, 1.0)},
            begin
                Offsets = prefix_sum(Deltas, StartOffset),
                NumChunks = length(Offsets),
                MaxOffset = lists:last(Offsets),
                Target = trunc(MaxOffset * TargetFraction),
                IdxFile = write_index_file(Offsets),
                try
                    compare_offset_search(IdxFile, Target, NumChunks)
                after
                    file:delete(IdxFile)
                end
            end).

prop_timestamp_search() ->
    ?FORALL({Deltas, StartTs, TargetFraction},
            {non_empty(list(range(1, 5000))), range(1, 1000000000000), float(0.0, 1.0)},
            begin
                Timestamps = prefix_sum(Deltas, StartTs),
                NumChunks = length(Timestamps),
                MaxTs = lists:last(Timestamps),
                MinTs = hd(Timestamps),
                Target = MinTs + trunc((MaxTs - MinTs) * TargetFraction),
                IdxFile = write_index_file_with_timestamps(NumChunks, Timestamps),
                try
                    compare_timestamp_search(IdxFile, Target, NumChunks)
                after
                    file:delete(IdxFile)
                end
            end).

%% --- Comparison functions ---

compare_offset_search(IdxFile, Target, NumRecords) ->
    {ok, Fd} = file:open(IdxFile, [read, raw, binary]),
    SkipResult = osiris_log:idx_skip_search(
                   Fd, ?IDX_HEADER_SIZE,
                   fun osiris_log:offset_search_fun/3,
                   {Target, not_found}),
    _ = file:position(Fd, bof),
    BlockResult = osiris_log:idx_block_binary_search(
                    Fd, NumRecords,
                    fun osiris_log:offset_search_fun/3,
                    {Target, not_found}),
    ok = file:close(Fd),
    case SkipResult =:= BlockResult of
        true ->
            true;
        false ->
            ct:pal("MISMATCH offset=~p skip=~p block=~p",
                   [Target, SkipResult, BlockResult]),
            false
    end.

compare_timestamp_search(IdxFile, Target, NumRecords) ->
    {ok, Fd} = file:open(IdxFile, [read, raw, binary]),
    SkipResult = osiris_log:idx_skip_search(
                   Fd, ?IDX_HEADER_SIZE,
                   fun osiris_log:timestamp_search_fun/3,
                   {Target, not_found}),
    _ = file:position(Fd, bof),
    BlockResult = osiris_log:idx_block_binary_search(
                    Fd, NumRecords,
                    fun osiris_log:timestamp_search_fun/3,
                    {Target, not_found}),
    ok = file:close(Fd),
    case SkipResult =:= BlockResult of
        true ->
            true;
        false ->
            ct:pal("MISMATCH ts=~p skip=~p block=~p",
                   [Target, SkipResult, BlockResult]),
            false
    end.

%% --- Generators / helpers ---

prefix_sum(Deltas, Start) ->
    {Sums, _} = lists:mapfoldl(fun(D, Acc) -> {Acc, Acc + D} end, Start, Deltas),
    Sums.

write_index_file(Offsets) ->
    File = tmp_idx_file(),
    Header = <<"OSII", 1:32/unsigned>>,
    Records = [<<Offset:64/unsigned,
                 Offset:64/signed,
                 1:64/unsigned,
                 0:32/unsigned,
                 0:8/unsigned>> || Offset <- Offsets],
    ok = file:write_file(File, [Header | Records]),
    File.

write_index_file_with_timestamps(N, Timestamps) ->
    File = tmp_idx_file(),
    Header = <<"OSII", 1:32/unsigned>>,
    Offsets = lists:seq(0, N - 1),
    Records = lists:zipwith(
                fun(Offset, Ts) ->
                    <<Offset:64/unsigned,
                      Ts:64/signed,
                      1:64/unsigned,
                      0:32/unsigned,
                      0:8/unsigned>>
                end, Offsets, Timestamps),
    ok = file:write_file(File, [Header | Records]),
    File.

tmp_idx_file() ->
    Dir = persistent_term:get({?MODULE, test_dir}),
    filename:join(Dir, "prop_idx_" ++ integer_to_list(erlang:unique_integer([positive])) ++ ".index").
