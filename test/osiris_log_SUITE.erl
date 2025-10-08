%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include("src/osiris.hrl").
% -include("src/osiris_peer_shim.hrl").

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
     write_with_filter_attach_next,
     write_batch_with_filter,
     write_batch_with_filters_variable_size,
     subbatch,
     subbatch_compressed,
     iterator_read_chunk,
     iterator_read_chunk_with_read_ahead,
     iterator_read_chunk_with_read_ahead_2,
     iterator_read_chunk_mixed_sizes_with_credit,
     read_chunk_parsed,
     read_chunk_parsed_2,
     read_chunk_parsed_multiple_chunks,
     read_chunk_parsed_2_multiple_chunks,
     read_header,
     write_multi_log,
     tail_info_empty,
     tail_info,
     init_offset_reader_empty,
     init_offset_reader_empty_directory,
     init_offset_reader,
     init_offset_reader_last_chunk_is_not_user_chunk,
     init_offset_reader_no_user_chunk_in_last_segment,
     init_offset_reader_no_user_chunk_in_segments,
     init_offset_reader_timestamp_empty,
     init_offset_reader_timestamp,
     init_offset_reader_timestamp_multi_segment,
     init_offset_reader_validate_single_msg_chunks,
     init_offset_reader_truncated,
     init_data_reader_next,
     init_data_reader_empty_log,
     init_data_reader_truncated,
     init_epoch_offsets_empty,
     init_epoch_offsets_empty_writer,
     init_epoch_offsets_truncated_writer,
     init_epoch_offsets,
     init_epoch_offsets_multi_segment,
     init_epoch_offsets_multi_segment2,
     init_raw_offset_reader,
     % truncate,
     % truncate_multi_segment,
     accept_chunk,
     accept_chunk_inital_offset,
     accept_chunk_iolist_header_in_first_element,
     init_acceptor_truncates_tail,
     accept_chunk_truncates_tail,
     accept_chunk_does_not_truncate_tail_in_same_epoch,
     accept_chunk_in_other_epoch,
     init_epoch_offsets_discards_all_when_no_overlap_in_same_epoch,
     init_corrupted_log,
     init_only_one_corrupted_segment,
     init_empty_last_files,
     evaluate_retention_max_bytes,
     evaluate_retention_max_age,
     evaluate_retention_max_age_empty,
     evaluate_retention_fun,
     offset_tracking,
     offset_tracking_snapshot,
     many_segment_overview,
     small_chunk_overview,
     overview,
     init_partial_writes,
     init_with_unexpected_file,
     overview_with_missing_segment,
     overview_with_missing_index_at_start,
     read_ahead_send_file,
     read_ahead_send_file_filter,
     read_ahead_send_file_on_off
    ].

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
    osiris:configure_logger(logger),
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    LeaderDir = filename:join(Dir, "leader"),
    Follower1Dir = filename:join(Dir, "follower1"),
    ORef = osiris_log_shared:new(),
    [{test_case, TestCase},
     {leader_dir, LeaderDir},
     {follower1_dir, Follower1Dir},
     {osiris_conf,
      #{dir => Dir,
        name => atom_to_list(TestCase),
        epoch => 1,
        readers_counter_fun => fun(_) -> ok end,
        shared => ORef,
        options => #{}}},
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
    %% validate the segment filename is recovered
    ?assertMatch(#{file := <<"00000000000000000000.segment">>},
                 osiris_log:format_status(S2)),
    ok.

init_recover_with_writers(Config) ->
    S0 = osiris_log:init(?config(osiris_conf, Config)),
    Now = erlang:system_time(millisecond),
    Writers = make_trailer(sequence, <<"wid1">>, 1),
    ChId = osiris_log:next_offset(S0),
    S1 = osiris_log:write([<<"hi">>], ?CHNK_USER, Now, Writers, S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    ok = osiris_log:close(S1),
    S2 = osiris_log:init(?config(osiris_conf, Config)),
    Trk = osiris_log:recover_tracking(S2),
    ?assertMatch(#{sequences := #{<<"wid1">> := {ChId, 1}}}, osiris_tracking:overview(Trk)),
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

write_with_filter_attach_next(Config) ->
    %% bug fix where the chunk_info size didn't include the filter size which
    %% cause the next attach strategy to point to an invalid location in the
    %% segment when filters were used.
    Conf0 = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf0#{filter_size => 32}),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    %% write an entry with a filter value and an entry without a filter value
    {_, S1} = write_committed([<<"ho">>, {<<"banana">>, <<"hi">>}], S0),
    Shared = osiris_log:get_shared(S1),
    Conf = Conf0#{shared => Shared},
    {ok, R0} = osiris_log:init_offset_reader(next, Conf),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    %% then write a chunk without any filtred entries at all
    {_, _S2} = write_committed([<<"hum">>], S1),
    ?assertMatch({[{2, <<"hum">>}], _R1},
                 osiris_log:read_chunk_parsed(R1)),
    ok.

write_batch_with_filter(Config) ->
    Conf0 = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf0#{filter_size => 32}),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    %% write an entry with a filter value and an entry without a filter value
    S1 = osiris_log:write([<<"ho">>, {<<"banana">>, <<"hi">>}], S0),
    %% then write a chunk without any filtred entries at all
    S2 = osiris_log:write([<<"hum">>], S1),
    ?assertEqual(3, osiris_log:next_offset(S2)),

    Shared = osiris_log:get_shared(S0),
    Conf = Conf0#{shared => Shared},
    osiris_log_shared:set_committed_chunk_id(Shared, 2), %% second chunk index
    %% no filter should read the chunk
    {ok, R0} = osiris_log:init_offset_reader(0, Conf),
    ?assertMatch({[{0, <<"hi">>}, {1, <<"ho">>}], _R1},
                 osiris_log:read_chunk_parsed(R0)),
    %% matching filter should read the chunk
    {ok, F0} = osiris_log:init_offset_reader(0, add_filter(<<"banana">>, Conf)),
    ?assertMatch({[{0, <<"hi">>}, _], _F1}, osiris_log:read_chunk_parsed(F0)),
    %% non-matching filter should not typically read the chunk
    {ok, M0} = osiris_log:init_offset_reader(0, add_filter(<<"apple">>, Conf)),
    ?assertMatch({end_of_stream, _M1}, osiris_log:read_chunk_parsed(M0)),

    %% interested in unfiltered entries also
    {ok, E0} = osiris_log:init_offset_reader(0, add_filter(<<"apple">>, true, Conf)),
    {[{0, <<"hi">>}, _], E1} = osiris_log:read_chunk_parsed(E0),
    ?assertMatch({[{2, <<"hum">>}], _E2}, osiris_log:read_chunk_parsed(E1)),
    ok.

write_batch_with_filters_variable_size(Config) ->
    Conf0 = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf0#{filter_size => 32}),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    %% write an entry with a filter value and an entry without a filter value
    S1 = osiris_log:write([{<<"apple">>, <<"ho">>},
                           {<<"banana">>, <<"hi">>}], S0),
    _ = osiris_log:close(S1),
    S2 = osiris_log:init(Conf0#{filter_size => 255}),
    %% then write a chunk without any filtred entries at all
    S3 = osiris_log:write([{<<"apple">>, <<"ho">>},
                           {<<"banana">>, <<"hi">>}], S2),
    ?assertEqual(4, osiris_log:next_offset(S3)),

    Shared = osiris_log:get_shared(S3),
    Conf = Conf0#{shared => Shared},
    osiris_log_shared:set_committed_chunk_id(Shared, 2), %% second chunk index
    {ok, F0} = osiris_log:init_offset_reader(0, add_filter(<<"banana">>, Conf)),
    {[{0, <<"hi">>}, _], F1} = osiris_log:read_chunk_parsed(F0),
    % debugger:start(),
    % int:i(osiris_log),
    % int:break(osiris_log, 2908),
    {[{2, <<"hi">>}, _], F2} = osiris_log:read_chunk_parsed(F1),
    osiris_log:close(F2),
    ok.

add_filter(Filter, #{options := Opts} = Conf) ->
    add_filter(Filter, false, #{options := Opts} = Conf).

add_filter(Filter, MatchUnfiltered, #{options := Opts} = Conf) ->
    Conf#{options => Opts#{filter_spec => #{filters => [Filter],
                                            match_unfiltered => MatchUnfiltered}}}.

subbatch(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    IOData = [<<0:1, 2:31/unsigned, "hi">>, <<0:1, 2:31/unsigned, "h0">>],
    CompType = 0, %% no compression
    Batch = {batch, 2, CompType, iolist_size(IOData), IOData},
    %% osiris_writer passes entries in reverse order
    S1 = osiris_log:write(
             lists:reverse([Batch, <<"simple">>]), S0),
    ?assertEqual(3, osiris_log:next_offset(S1)),
    OffRef = osiris_log:get_shared(S1),
    {ok, R0} =
        osiris_log:init_offset_reader(0, Conf#{shared => OffRef}),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    osiris_log_shared:set_committed_chunk_id(OffRef, 0), %% first chunk index

    ?assertMatch({[{0, <<"hi">>}, {1, <<"h0">>}, {2, <<"simple">>}], _},
                 osiris_log:read_chunk_parsed(R1)),

    osiris_log:close(S1),
    osiris_log:close(R1),
    ok.

subbatch_compressed(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    IOData = zlib:gzip([<<0:1, 2:31/unsigned, "hi">>, <<0:1, 2:31/unsigned, "h0">>]),
    CompType = 1, %% gzip
    Batch = {batch, 2, CompType, iolist_size(IOData), IOData},
    %% osiris_writer passes entries in reverse order
    S1 = osiris_log:write(
             lists:reverse([Batch, <<"simple">>]), S0),
    ?assertEqual(3, osiris_log:next_offset(S1)),
    OffRef = osiris_log:get_shared(S1),
    {ok, R0} =
        osiris_log:init_offset_reader(0, Conf#{shared => OffRef}),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    osiris_log_shared:set_committed_chunk_id(OffRef, 0), %% first chunk index

    %% compressed sub batches should not be parsed server side
    %% so just returns the batch as is
    ?assertMatch({[{0, Batch}, {2, <<"simple">>}], _},
                 osiris_log:read_chunk_parsed(R1)),

    osiris_log:close(S1),
    osiris_log:close(R1),
    ok.

iterator_read_chunk(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    Shared = osiris_log:get_shared(S0),
    RConf = Conf#{shared => Shared},
    {ok, R0} = osiris_log:init_offset_reader(0, RConf),
    {end_of_stream, R1} = osiris_log:chunk_iterator(R0),
    IOData = <<0:1, 2:31/unsigned, "hi", 0:1, 2:31/unsigned, "h0">>,
    CompType = 0, %% no compression
    Batch = {batch, 2, CompType, byte_size(IOData), IOData},
    EntriesRev = [Batch,
                  <<"ho">>,
                  {<<"filter">>, <<"hi">>}],
    {ChId, S1} = write_committed(EntriesRev, S0),
    {ok, _H, I0, R2} = osiris_log:chunk_iterator(R1),
    HoOffs = ChId + 1,
    BatchOffs = ChId + 2,
    {{ChId, <<"hi">>}, I1} = osiris_log:iterator_next(I0),

    {{HoOffs, <<"ho">>}, I2} = osiris_log:iterator_next(I1),
    {{BatchOffs, Batch}, I} = osiris_log:iterator_next(I2),
    ?assertMatch(end_of_chunk, osiris_log:iterator_next(I)),
    osiris_log:close(R2),
    osiris_log:close(S1),
    ok.

iterator_read_chunk_mixed_sizes_with_credit(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    Shared = osiris_log:get_shared(S0),
    RConf = Conf#{shared => Shared},
    {ok, R0} = osiris_log:init_offset_reader(0, RConf),
    {end_of_stream, R1} = osiris_log:chunk_iterator(R0),
    Big = crypto:strong_rand_bytes(100_000),
    EntriesRev = [Big,
                  <<"ho">>,
                  {<<"filter">>, <<"hi">>}],
    {ChId, S1} = write_committed(EntriesRev, S0),
    %% this is a less than ideal case where we have one large and two very
    %% small entries in the same batch. We read ahead only the 2 first entries.
    {ok, _H, I0, R2} = osiris_log:chunk_iterator(R1, 2),
    HoOffs = ChId + 1,
    BigOffs = ChId + 2,
    {{ChId, <<"hi">>}, I1} = osiris_log:iterator_next(I0),
    {{HoOffs, <<"ho">>}, I2} = osiris_log:iterator_next(I1),
    {{BigOffs, Big}, I} = osiris_log:iterator_next(I2),
    ?assertMatch(end_of_chunk, osiris_log:iterator_next(I)),
    osiris_log:close(R2),
    osiris_log:close(S1),
    ok.

iterator_read_chunk_with_read_ahead(Config) ->
    RAL = 4096, %% read ahead limit
    Conf = ?config(osiris_conf, Config),
    Writer = osiris_log:init(Conf),
    Shared = osiris_log:get_shared(Writer),
    RConf = Conf#{shared => Shared},
    {ok, R} = osiris_log:init_offset_reader(0, RConf),
    Tests =
    [
     fun(write, #{w := W0}) ->
             EntriesRev = [<<"hi">>, <<"ho">>],
             {_, W1} = write_committed(EntriesRev, W0),
             W1;
        (read, #{r := R0, tracer := T} = Ctx) ->
             %% small chunk, managed to read it with the filter read-ahead
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 2),
             {{_, <<"ho">>}, I1} = osiris_log:iterator_next(I0),
             {{_, <<"hi">>}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),
             ?assertEqual(1, osiris_tracer:call_count(T)),
             Ctx#{r => R1, i => I2}
     end,
     fun(write, #{w := W0}) ->
             EntriesRev = [<<"foo">>, <<"bar">>],
             {_, W1} = write_committed(EntriesRev, W0),
             W1;
        (read, #{r := R0, tracer := T, i := PrevI} = Ctx) ->
             %% not enough read-ahead data, we have to read from file
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 2, PrevI),
             {{_, <<"bar">>}, I1} = osiris_log:iterator_next(I0),
             {{_, <<"foo">>}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),
             ?assertEqual(1, osiris_tracer:call_count(T)),
             Ctx#{r => R1, i => I2}
     end,
     fun(write, #{w := W0}) ->
             E1 = binary:copy(<<"b">>, RAL - 200),
             EntriesRev = [E1 , <<"aaa">>],
             {_, W1} = write_committed(EntriesRev, W0),
             W1;
        (read, #{r := R0, tracer := T, i := PrevI} = Ctx) ->
             %% this one has been read ahead
             E1 = binary:copy(<<"b">>, RAL - 200),
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 1, PrevI),
             {{_, <<"aaa">>}, I1} = osiris_log:iterator_next(I0),
             {{_, E1}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),
             ?assertEqual(0, osiris_tracer:call_count(T)),
             Ctx#{r => R1, i => I2}
     end,
     fun(write, #{w := W0}) ->
             %% this one is too big to be read ahead
             E1 = binary:copy(<<"b">>, RAL * 2),
             EntriesRev = [E1 , <<"aaa">>],
             {_, W1} = write_committed(EntriesRev, W0),
             W1;
        (read, #{r := R0, tracer := T, i := PrevI} = Ctx) ->
             E1 = binary:copy(<<"b">>, RAL * 2),
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 1, PrevI),
             {{_, <<"aaa">>}, I1} = osiris_log:iterator_next(I0),
             {{_, E1}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),
             ?assertEqual(1, osiris_tracer:call_count(T)),
             Ctx#{r => R1, i => I2}
     end,
     fun(write, #{w := W0}) ->
             EntriesRev = [<<"hiiiiiiiiiiiiiiiii">>,
                           <<"hooooooooooooooooo">>],
             {_, W1} = write_committed(EntriesRev, W0),
             {_, W} = write_committed([<<"hum">>], W1),
             W;
        (read, #{r := R0, tracer := T, i := PrevI} = Ctx) ->
             %% small chunk, managed to read it with the filter read-ahead
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 1, PrevI),
             {{_, <<"hooooooooooooooooo">>}, I1} = osiris_log:iterator_next(I0),
             {{_, <<"hiiiiiiiiiiiiiiiii">>}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),
             {ok, _, I3, R2} = osiris_log:chunk_iterator(R1, 1, I2),
             {{_, <<"hum">>}, I4} = osiris_log:iterator_next(I3),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I4)),
             ?assertEqual(2, osiris_tracer:call_count(T)),
             Ctx#{r => R2, i => I4}
     end
    ],

    #{w := Wr1, r := Rd1} = run_read_ahead_tests(Tests, offset,
                                                 ?DEFAULT_FILTER_SIZE, Writer, R),
    osiris_log:close(Rd1),
    osiris_log:close(Wr1),
    ok.

iterator_read_chunk_with_read_ahead_2(Config) ->
    %% this tests a specific optimization in read ahead
    Conf0 = ?config(osiris_conf, Config),
    #{dir := Dir0} = Conf0,
    D1 = crypto:strong_rand_bytes(1024),
    D2 = crypto:strong_rand_bytes(1024),
    D3 = crypto:strong_rand_bytes(1024),
    D4 = crypto:strong_rand_bytes(1024),
    D5 = crypto:strong_rand_bytes(1024),
    D6 = crypto:strong_rand_bytes(1024),
    Tests =
    [
     fun(write, #{w := W0}) ->
             %% this enables read ahead
             {_, W1} = write_committed([D2, D1], W0),
             {_, W2} = write_committed([D5, D4, D3], W1),
             {_, W3} = write_committed([D6], W2),
             W3;
        (read, #{r := R0, tracer := T, ra := Ra} = Ctx) ->
             %% small chunk should enable read ahead
             {ok, _, I0, R1} = osiris_log:chunk_iterator(R0, 1),
             {{_, D1}, I1} = osiris_log:iterator_next(I0),
             {{_, D2}, I2} = osiris_log:iterator_next(I1),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I2)),

             {ok, _, I3, R2} = osiris_log:chunk_iterator(R1, 1, I2),
             {{_, D3}, I4} = osiris_log:iterator_next(I3),
             {{_, D4}, I5} = osiris_log:iterator_next(I4),
             {{_, D5}, I6} = osiris_log:iterator_next(I5),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I6)),

             {ok, _, I7, R3} = osiris_log:chunk_iterator(R2, 1, I6),
             {{_, D6}, I8} = osiris_log:iterator_next(I7),
             ?assertMatch(end_of_chunk, osiris_log:iterator_next(I8)),

             case Ra of
                 true ->
                     ?assertEqual(3, osiris_tracer:call_count(T));
                 false ->
                     ?assertEqual(6, osiris_tracer:call_count(T))
             end,

             Ctx#{r => R3, i => I8}
     end
    ],

    RaOnOff = [true, false],
    [begin
         Dir1 = filename:join(Dir0, io_lib:format("~p", [Ra])),
         Conf1 = Conf0#{dir => Dir1},
         Writer = osiris_log:init(Conf1),
         Shared = osiris_log:get_shared(Writer),
         RConf = Conf1#{shared => Shared, options => #{read_ahead => Ra}},
         {ok, R} = osiris_log:init_offset_reader(0, RConf),

         #{w := Wr1, r := Rd1} = run_read_ahead_tests(Tests, offset,
                                                      ?DEFAULT_FILTER_SIZE,
                                                      Writer, R,
                                                      #{ra => Ra}),
         osiris_log:close(Rd1),
         osiris_log:close(Wr1)
     end || Ra <- RaOnOff],

    ok.

read_chunk_parsed(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    RConf = Conf#{shared => osiris_log:get_shared(S0)},
    {ok, R0} = osiris_log:init_data_reader({0, empty}, RConf),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0),
    _S1 = osiris_log:write([<<"hi">>], S0),
    ?assertMatch({[{0, <<"hi">>}], _}, osiris_log:read_chunk_parsed(R1)),
    ok.

read_chunk_parsed_2(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    RConf = Conf#{shared => osiris_log:get_shared(S0)},
    {ok, R0} = osiris_log:init_data_reader({0, empty}, RConf),
    {end_of_stream, R1} = osiris_log:read_chunk_parsed(R0, with_header),
    _S1 = osiris_log:write([<<"hi">>], S0),
    {ok,
     #{num_records := 1},
     [{0, <<"hi">>}], R2} = osiris_log:read_chunk_parsed(R1, with_header),
    {end_of_stream, _} = osiris_log:read_chunk_parsed(R2, with_header),
    ok.

read_chunk_parsed_multiple_chunks(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    Entries = [<<"hi">>, <<"hi-there">>],
    %% osiris_writer passes entries in reversed order
    S1 = osiris_log:write(
             lists:reverse(Entries), S0),
    S2 = osiris_log:write([<<"hi-again">>], S1),
    RConf = Conf#{shared => osiris_log:get_shared(S2)},
    {ok, R0} = osiris_log:init_data_reader({0, empty}, RConf),
    {[{0, <<"hi">>}, {1, <<"hi-there">>}], R1} =
        osiris_log:read_chunk_parsed(R0),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R1)),
    %% open another reader at a later index
    {ok, R2} = osiris_log:init_data_reader({2, {1, 0, 0}}, RConf),
    ?assertMatch({[{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R2)),
    ok.

read_chunk_parsed_2_multiple_chunks(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    Entries = [<<"hi">>, <<"hi-there">>],
    %% osiris_writer passes entries in reversed order
    S1 = osiris_log:write(
             lists:reverse(Entries), S0),
    S2 = osiris_log:write([<<"hi-again">>], S1),
    RConf = Conf#{shared => osiris_log:get_shared(S2)},
    {ok, R0} = osiris_log:init_data_reader({0, empty}, RConf),
    {ok, #{num_records := 2},
     [{0, <<"hi">>}, {1, <<"hi-there">>}], R1} =
        osiris_log:read_chunk_parsed(R0, with_header),
    ?assertMatch({ok, #{num_records := 1}, [{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R1, with_header)),
    %% open another reader at a later index
    {ok, R2} = osiris_log:init_data_reader({2, {1, 0, 0}}, RConf),
    ?assertMatch({ok, #{num_records := 1}, [{2, <<"hi-again">>}], _},
                 osiris_log:read_chunk_parsed(R2, with_header)),
    ok.

read_header(Config) ->
    Conf = ?config(osiris_conf, Config),
    W0 = osiris_log:init(Conf),
    OffRef = osiris_log:get_shared(W0),
    {ok, R0} =
        osiris_log:init_offset_reader(first, Conf#{shared => OffRef}),
    {end_of_stream, R1} = osiris_log:read_header(R0),
    W1 = osiris_log:write([<<"hi">>, <<"ho">>], W0),
    W2 = osiris_log:write([<<"hum">>], W1),
    osiris_log_shared:set_committed_chunk_id(OffRef, 3),
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
    {end_of_stream, R4} = osiris_log:read_header(R3),
    _W = osiris_log:write([<<"bah">>], W2),
    %% simulate the case where the chunk write is not yet complate but there is
    %% data
    osiris_log_shared:set_last_chunk_id(OffRef, 2),
    {end_of_stream, _R} = osiris_log:read_header(R4),
    ok.

write_multi_log(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf#{max_segment_size_bytes => 10 * 1000 * 1000}),
    Data = crypto:strong_rand_bytes(10000),
    BatchOf10 = [Data || _ <- lists:seq(1, 10)],
    S1 = lists:foldl(fun(_, Acc0) ->
                              osiris_log:write(BatchOf10, Acc0) end,
                      S0, lists:seq(1, 101)),
    NextOffset = osiris_log:next_offset(S1),
    Segments =
        filelib:wildcard(
            filename:join(?config(dir, Config), "*.segment")),
    ?assertEqual(2, length(Segments)),

    OffRef = osiris_log:get_shared(S1),
    %% takes a single offset tracking data into account
    osiris_log_shared:set_committed_chunk_id(OffRef, NextOffset),
    %% ensure all records can be read
    {ok, R0} =
        osiris_log:init_offset_reader(first, Conf#{shared => OffRef}),

    R1 = lists:foldl(fun(_, Acc0) ->
                        {Records = [_ | _], Acc} =
                            osiris_log:read_chunk_parsed(Acc0),

                        ?assert(is_list(Records)),
                        ?assertEqual(10, length(Records)),
                        Acc
                     end,
                     R0, lists:seq(1, 101)),
    ?assertEqual(NextOffset, osiris_log:next_offset(R1)),
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
    RConf = Conf#{dir => LDir},
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

init_offset_reader_empty_directory(Config) ->
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    RConf = Conf#{dir => LDir},
    ?assertEqual({error, no_index_file}, osiris_log:init_offset_reader(first, RConf)),
    ?assertEqual({error, no_index_file}, osiris_log:init_offset_reader(last, RConf)),
    ?assertEqual({error, no_index_file}, osiris_log:init_offset_reader(next, RConf)),
    ?assertEqual({error, no_index_file}, osiris_log:init_offset_reader(0, RConf)),
    ?assertEqual({error, no_index_file}, osiris_log:init_offset_reader({abs, 1}, RConf)),
    ok.

init_offset_reader(Config) ->
    init_offset_reader(Config, offset).

init_raw_offset_reader(Config) ->
    %% Raw readers behave like offset readers
    init_offset_reader(Config, raw).

init_offset_reader(Config, Mode) ->
    EpochChunks =
        [{1, [<<"one">>]}, {2, [<<"two">>]}, {3, [<<"three">>, <<"four">>]}],
    LDir = ?config(leader_dir, Config),
    Conf = ?config(osiris_conf, Config),
    set_shared(Conf, 3),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir, mode => Mode},

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

init_offset_reader_last_chunk_is_not_user_chunk(Config) ->
    % | offset | chunk type |
    % | 0      | user       |
    % | 1      | user       |
    % | 2      | tracking   |
    % | 3      | tracking   |
    EpochChunks = [{1, [<<"one">>]}, {2, [<<"two">>]}],
    LDir = ?config(leader_dir, Config),
    S0 = seed_log(LDir, EpochChunks, Config),
    S1 = osiris_log:write([<<"1st tracking delta chunk">>],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 10),
                          S0),
    S2 = osiris_log:write([<<"2nd tracking delta chunk">>],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 11),
                          S1),
    osiris_log:close(S2),

    Conf = ?config(osiris_conf, Config),
    RConf = Conf#{dir => LDir},
    set_shared(RConf, 4),
    % Test that 'last' returns last user chunk
    {ok, L1} = osiris_log:init_offset_reader(last, RConf),
    ?assertEqual(1, osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    % Test that 'next' returns next chunk
    {ok, L2} = osiris_log:init_offset_reader(next, RConf),
    ?assertEqual(4, osiris_log:next_offset(L2)),
    osiris_log:close(L2),
    ok.

init_offset_reader_no_user_chunk_in_last_segment(Config) ->
    % | offset | chunk type | segment |
    % | 0      | user       | 1       |
    % | 1      | tracking   | 1       |
    % | 2      | tracking   | 2       |
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{max_segment_size_bytes => 120},
    S0 = seed_log(Conf, [{1, [<<"one">>]}], Config),
    set_shared(Conf, 2),
    S1 = osiris_log:write([<<"1st tracking delta chunk">>],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 10),
                          S0),
    S2 = osiris_log:write([<<"2nd tracking delta chunk">>],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 11),
                          S1),
    osiris_log:close(S2),

    RConf = Conf,
    % test that 'last' returns last user chunk
    {ok, L1} = osiris_log:init_offset_reader(last, RConf),
    ?assertEqual(0, osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    % test that 'next' returns next chunk
    {ok, L2} = osiris_log:init_offset_reader(next, RConf),
    ?assertEqual(3, osiris_log:next_offset(L2)),
    osiris_log:close(L2),
    ok.

init_offset_reader_no_user_chunk_in_segments(Config) ->
    % | offset | chunk type |
    % | 0      | tracking   |
    LDir = ?config(leader_dir, Config),
    S0 = seed_log(LDir, [], Config),
    S1 = osiris_log:write([<<"1st tracking delta chunk">>],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 10),
                          S0),
    osiris_log:close(S1),

    Conf = ?config(osiris_conf, Config),
    RConf = Conf#{dir => LDir},
    % Test that 'last' falls back to `next` behaviour when there is no user chunk
    % present in the log
    {ok, L1} = osiris_log:init_offset_reader(last, RConf),
    ?assertEqual(1, osiris_log:next_offset(L1)),
    osiris_log:close(L1),

    {ok, L2} = osiris_log:init_offset_reader(next, RConf),
    ?assertEqual(1, osiris_log:next_offset(L2)),
    osiris_log:close(L2),
    ok.

init_offset_reader_timestamp_empty(Config) ->
    ok = logger:set_primary_config(level, all),
    Now = now_ms(),
    EpochChunks = [],
    LDir = ?config(leader_dir, Config),
    Conf0 = ?config(osiris_conf, Config),
    %% TODO: separate multi segment test
    % application:set_env(osiris, max_segment_size_chunks, 1),
    Conf = Conf0#{max_segment_size_chunks => 1},
    LLog0 = seed_log(LDir, EpochChunks, Config),
    set_shared(Conf, 3),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir},

    {ok, L1} = osiris_log:init_offset_reader({timestamp, Now - 8000}, RConf),
    ?assertEqual(0, osiris_log:next_offset(L1)),
    osiris_log:close(L1),
    ok.

init_offset_reader_timestamp(Config) ->
    ok = logger:set_primary_config(level, all),
    Now = now_ms(),
    EpochChunks =
        [{1, Now - 10000, [<<"one">>]}, % 0
         {1, Now - 8000, [<<"two">>]},  % 1
         {1, Now - 5000, [<<"three">>, <<"four">>]}], % 2
    LDir = ?config(leader_dir, Config),
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{max_segment_size_chunks => 1},
    LLog0 = seed_log(LDir, EpochChunks, Config),
    set_shared(Conf, 3),
    osiris_log:close(LLog0),
    RConf = Conf#{dir => LDir},

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

    %% in between
    {ok, L4} =
        osiris_log:init_offset_reader({timestamp, Now - 6000}, RConf),
    ?assertEqual(2, osiris_log:next_offset(L4)),
    osiris_log:close(L4),
    ok.

init_offset_reader_timestamp_multi_segment(Config) ->
    ok = logger:set_primary_config(level, all),
    Now = now_ms(),
    EpochChunks =
        [{1, Now - 10000, [<<"one">>]}, % 0
         {1, Now - 8000, [<<"two">>]},  % 1
         {1, Now - 5000, [<<"three">>, <<"four">>]}], % 2
    LDir = ?config(leader_dir, Config),
    Conf0 = ?config(osiris_conf, Config),
    %% segment per chunk
    application:set_env(osiris, max_segment_size_chunks, 1),
    Conf = Conf0#{max_segment_size_chunks => 1},
    LLog0 = seed_log(LDir, EpochChunks, Config),

    set_shared(Conf, 3),
    osiris_log:close(LLog0),
    application:unset_env(osiris, max_segment_size_chunks),
    RConf = Conf#{dir => LDir},

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

    %% in between
    {ok, L4} =
        osiris_log:init_offset_reader({timestamp, Now - 6000}, RConf),
    ?assertEqual(2, osiris_log:next_offset(L4)),
    osiris_log:close(L4),
    ok.

init_offset_reader_validate_single_msg_chunks(Config) ->
    ok = logger:set_primary_config(level, all),
    Data = crypto:strong_rand_bytes(100),
    ChIds = lists:seq(0, 20000),
    EpochChunks =
        [begin {1, [Data]} end || _ <- ChIds],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    set_shared(Conf, 20000),

    % IdxFiles = osiris_log:sorted_index_files(LDir),
    RConf = Conf#{dir => LDir},
                  % index_files => IdxFiles},
    {T19999, {ok, L19999}} = timer:tc(
                               fun () ->
                                       osiris_log:init_offset_reader(19730, RConf)
                               end),
    osiris_log:close(L19999),
    ct:pal("T19999 took ~bms", [T19999 div 1000]),
    {T, _} = timer:tc(fun () ->
                              [begin
                                   {ok, L} = osiris_log:init_offset_reader(ChId, RConf),
                                   ?assertEqual(ChId, osiris_log:next_offset(L)),
                                   osiris_log:close(L)
                               end || ChId <- ChIds,
                                      %% checking all is too slow
                                      ChId rem 9 == 0]
                      end),
    ct:pal("Takne ~b", [T div 1000]),
    osiris_log:close(LLog0),
    ok.

init_offset_reader_truncated(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, EpochChunks, Config),
    set_shared(Conf, 1000),
    RConf = Conf#{dir => LDir},
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

init_data_reader_next(Config) ->
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    FDir = ?config(follower1_dir, Config),
    _LLog0 = seed_log(LDir, [{1, [<<"one">>]}], Config),
    %% seed is up to date
    FLog0 = seed_log(FDir, [{1, [<<"one">>]}], Config),
    RRConf = Conf#{dir => LDir,
                   shared => osiris_log:get_shared(FLog0)},
    %% the next offset, i.e. offset 0
    {ok, _RLog0} =
        osiris_log:init_data_reader(
            osiris_log:tail_info(FLog0), RRConf),
    ok.

init_data_reader_empty_log(Config) ->
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog0 = seed_log(LDir, [], Config),
    %% an empty log
    FLog0 = seed_log(?config(follower1_dir, Config), [], Config),
    RRConf = Conf#{dir => ?config(leader_dir, Config),
                   shared => osiris_log:get_shared(FLog0)},
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
    RConf = Conf#{dir => LDir,
                  shared => osiris_log:get_shared(LLog0)},
    osiris_log:close(LLog0),

    %% "Truncate" log by deleting first segment
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.index")),
    ok =
        file:delete(
            filename:join(LDir, "00000000000000000000.segment")),

    %% when requesting a lower offset than the start of the log
    {error, {offset_out_of_range, _}} = osiris_log:init_data_reader({0, empty}, RConf),

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
    Overview = #{range => {0, 3},
                 epoch_offsets => [{1, 0}]},
    Log0 =
        osiris_log:init_acceptor(Overview, Conf#{dir => FDir, epoch => 1}),
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
    Overview = #{range => {0, 3},
                 epoch_offsets => []},
    Log0 =
        osiris_log:init_acceptor(Overview, Conf#{dir => LDir, epoch => 2}),
    {0, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

init_epoch_offsets_truncated_writer(Config) ->
    %% test acceptor initialisation where the acceptor has no log and the writer
    %% has had retention remove the head of it's log
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    Overview = #{range => {50, 100},
                 epoch_offsets => [{3, 100}]},
    Log0 =
        osiris_log:init_acceptor(Overview, Conf#{dir => LDir, epoch => 2}),
    {50, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),

    ?assert(filelib:is_file(filename:join(LDir, "00000000000000000050.index"))),
    ok.

init_epoch_offsets(Config) ->
    EpochChunks =
        [{1, [<<"one">>]}, {1, [<<"two">>]}, {1, [<<"three">>, <<"four">>]}],
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    Overview = #{range => {0, 1},
                 epoch_offsets => [{1, 1}]},
    Log0 =
        osiris_log:init_acceptor(Overview,
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
    Overview = #{range => {0, 650},
                 epoch_offsets => [{1, 650}]},
    Log0 =
        osiris_log:init_acceptor(Overview,
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
    Overview = #{range => {0, 750},
                 epoch_offsets => [{3, 750}, {1, 650}]},
    Log0 =
        osiris_log:init_acceptor(Overview,
                                 #{dir => LDir,
                                   name => ?config(test_case, Config),
                                   epoch => 2}),
    {700, {1, 650, _}} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
    ok.

accept_chunk_inital_offset(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(osiris_conf, Config),


    Ch1 = fake_chunk_bin([<<"blob1">>], ?LINE, 1, 100),

    F0 = osiris_log:init(Conf#{initial_offset => 100}, acceptor),
    F1 = osiris_log:accept_chunk(Ch1, F0),
    ?assertEqual(101, osiris_log:next_offset(F1)),

    Dir = maps:get(dir, Conf),
    {ok, Files} = file:list_dir(Dir),
    [ok = file:delete(filename:join(Dir, F)) || F <- Files],
    osiris_log:close(F1),

    X0 = osiris_log:init(Conf#{initial_offset => 200}, acceptor),
    ?assertEqual(200, osiris_log:next_offset(X0)),

    ok.

accept_chunk_iolist_header_in_first_element(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(osiris_conf, Config),


    <<Head:48/binary, Res/binary>> = fake_chunk_bin([{<<"filter">>, <<"blob1">>}],
                                                ?LINE, 1, 100),
    Ch1 = [Head, Res],
    F0 = osiris_log:init(Conf#{initial_offset => 100}, acceptor),
    %% this should not crash
    _F1 = osiris_log:accept_chunk(Ch1, F0),
    ok.

accept_chunk(Config) ->
    ok = logger:set_primary_config(level, all),
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LConf = Conf#{dir => LDir},
    FConf = Conf#{dir => ?config(follower1_dir, Config),
                  shared => osiris_log_shared:new()},
    L0 = osiris_log:init(LConf),
    L1 = osiris_log:write([<<"hi">>], ?CHNK_USER, ?LINE, <<>>, L0),
    timer:sleep(10),
    Now = ?LINE,
    L2 = osiris_log:write([{<<"filter">>, <<"ho">>}], ?CHNK_USER, Now, <<>>, L1),

    Overview = osiris_log:overview(LDir),

    F0 = osiris_log:init_acceptor(Overview, FConf),

    %% replica reader
    RConf = LConf#{shared => osiris_log:get_shared(L2)},
    {ok, R0} = osiris_log:init_data_reader(
                 osiris_log:tail_info(F0), RConf),
    {ok, Chunk1, R1} = osiris_log:read_chunk(R0),
    F1 = osiris_log:accept_chunk(Chunk1, F0),
    {ok, Chunk2, R2} = osiris_log:read_chunk(R1),
    F2 = osiris_log:accept_chunk(Chunk2, F1),

    osiris_log:close(L2),
    osiris_log:close(R2),
    osiris_log:close(F2),
    FL0 = osiris_log:init_acceptor(Overview, FConf),
    osiris_log:close(FL0),

    osiris_log_shared:set_committed_chunk_id(maps:get(shared, FConf), 1),
    {ok, O0} = osiris_log:init_offset_reader(last, FConf),
    {[{1, <<"ho">>}], _o} = osiris_log:read_chunk_parsed(O0),
    ok.

init_acceptor_truncates_tail(Config) ->
    ok = logger:set_primary_config(level, all),
    EpochChunks =
        [{1, [<<"one">>]},   % 0
         {1, [<<"two">>]},   % 1
         {1, [<<"three">>]}, % 2
         {1, [<<"four">>]}   % 3
        ],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertMatch({4, {1, 3, _}}, LTail), %% {NextOffs, {LastEpoch, LastChunkId, Ts}}
    ok = osiris_log:close(LLog),
    Overview = #{range => {0, 5},
                 epoch_offsets => [{2, 4}, {1, 2}]},
    ALog0 =
        osiris_log:init_acceptor(Overview, Conf#{dir => LDir, epoch => 2}),
    % ?assertMatch({3, {1, 2, _}}, osiris_log:tail_info(ALog0)), %% {NextOffs, {LastEpoch, LastChunkId, Ts}}
    {3, {1, 2, _}} = osiris_log:tail_info(ALog0), %% {NextOffs, {LastEpoch, LastChunkId, Ts}}


    ok.

accept_chunk_truncates_tail(Config) ->
    ok = logger:set_primary_config(level, all),
    EpochChunks =
        [{1, [<<"one">>]}, % 0
         {2, [<<"two">>]}, % 1
         {3, [<<"three">>, <<"four">>]}, % 2
         {3, [<<"five">>]} % 4
        ],
    Conf = ?config(osiris_conf, Config),
    LDir = ?config(leader_dir, Config),
    LLog = seed_log(LDir, EpochChunks, Config),
    LTail = osiris_log:tail_info(LLog),
    ?assertMatch({5, {3, 4, _}}, LTail), %% {NextOffs, {LastEpoch, LastChunkId, Ts}}
    ok = osiris_log:close(LLog),

    FollowerEpochChunks =
        [{1, [<<"one">>]}, %% 0
         {2, [<<"two">>]}, %% 1
         {2, [<<"three">>]}], %% should be truncated next accept
    FDir = ?config(follower1_dir, Config),
    FLog0 = seed_log(FDir, FollowerEpochChunks, Config),
    osiris_log:close(FLog0),

    Overview = osiris_log:overview(LDir),
    ALog0 =
        osiris_log:init_acceptor(Overview, Conf#{dir => FDir, epoch => 2}),
    LShared = osiris_log:get_shared(LLog),
    osiris_log_shared:set_committed_chunk_id(LShared, 4),
    RConf = Conf#{dir => LDir,
                  shared => LShared},
    {ok, RLog0} =
        osiris_log:init_data_reader(osiris_log:tail_info(ALog0), RConf),
    {ok, Ch1, RLog1} = osiris_log:read_chunk(RLog0),
    ALog1 = osiris_log:accept_chunk([Ch1], ALog0),
    {ok, Ch2, _RLog} = osiris_log:read_chunk(RLog1),
    ALog = osiris_log:accept_chunk([Ch2], ALog1),

    osiris_log:close(ALog),
    % validate equal
    ?assertMatch(Overview, osiris_log:overview(FDir)),

    {ok, V0} = osiris_log:init_data_reader({0, empty}, RConf#{dir => FDir}),
    {[{0, <<"one">>}], V1} = osiris_log:read_chunk_parsed(V0),
    {[{1, <<"two">>}], V2} = osiris_log:read_chunk_parsed(V1),
    {[{2, <<"three">>}, {3, <<"four">>}], V3} = osiris_log:read_chunk_parsed(V2),
    {[{4, <<"five">>}], _V} = osiris_log:read_chunk_parsed(V3),
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

    Overview = osiris_log:overview(LDir),
    ALog0 = osiris_log:init_acceptor(Overview, Conf#{dir => FDir, epoch => 2}),
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

    Overview = osiris_log:overview(LDir),
    ALog0 = osiris_log:init_acceptor(Overview, Conf#{dir => FDir, epoch => 2}),
    ATail = osiris_log:tail_info(ALog0),
    osiris_log:close(ALog0),
    %% ensure we don't truncate too much
    ?assertMatch({3, {1, 1, _}}, ATail),
    ok.


init_epoch_offsets_discards_all_when_no_overlap_in_same_epoch(Config) ->
    EpochChunks =
        [{1, [<<"one">>]},
         {1, [<<"two">>]},
         {1, [<<"three">>, <<"four">>]}],
    LDir = ?config(leader_dir, Config),
    LogInit = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(LogInit),
    Overview = #{range => {5, 10}, %% replica's range is 0, 3
                 epoch_offsets => [{1, 10}]},
    Log0 =
        osiris_log:init_acceptor(Overview,
                                 #{dir => LDir,
                                   name => ?config(test_case, Config),
                                   epoch => 2}),
    {5, empty} = osiris_log:tail_info(Log0),
    osiris_log:close(Log0),
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
    #{range := {0, 4},
      epoch_offsets := [{1, 1}, {2, 4}]} = osiris_log:overview(LDir),
    %% non existant dir should return empty
    #{range := empty,
      epoch_offsets := []} = osiris_log:overview("/tmp/blahblah"),
    ok.

init_corrupted_log(Config) ->
    % this test intentionally corrupts the log in a way we observed
    % when a working system was suddenly powered off;
    % the index contains an entry for a chunk that was not
    % written; additionally the index contains some trailing zeros
    % index:
    % | index header
    % | chunk0 record
    % | chunk1 record
    % | chunk2 record
    % | chunk3 record
    % | 000000000000000000000000000000000000000000000000000000000000
    % segment:
    % | segment header
    % | {0, <<one>>}
    % | {1, <<two>>}
    % | {2, <<three   (this chunk is corrupted)
    % | eof           (chunk3 is missing)
    EpochChunks = [{1, [<<"one">>]}, {1, [<<"two">>]}],
    LDir = ?config(leader_dir, Config),
    Log0 = seed_log(LDir, EpochChunks, Config),
    SegPath = filename:join(LDir, "00000000000000000000.segment"),
    IdxPath = filename:join(LDir, "00000000000000000000.index"),
    % record the segment and index sizes before corrupting the log
    ValidSegSize = filelib:file_size(SegPath),
    ValidIdxSize = filelib:file_size(IdxPath),
    #{range := {0, 1}} = osiris_log:overview(LDir),

    % add one more chunk and truncate it from the segment but leave in the index)
    Log1 = osiris_log:write([<<"three">>], Log0),
    SegSizeWith3Chunks = filelib:file_size(SegPath),
    Log2 = osiris_log:write([<<"four">>], Log1),
    osiris_log:close(Log2),
    {ok, SegFd} = file:open(SegPath, [raw, binary, read, write]),
    % truncate the file so that chunk <<four>> is missing and <<three>> is corrupted
    {ok, _} = file:position(SegFd, SegSizeWith3Chunks - 3),
    _ = file:truncate(SegFd),
    _ = file:close(SegFd),
    % append 60 zeros to the index file
    {ok, IdxFd} =
    _ = file:open(IdxPath, [raw, binary, read, write]),
    {ok, _} = file:position(IdxFd, eof),
    ok = file:write(IdxFd, <<0:480>>),
    ok = file:close(IdxFd),

    % the overview should work even before init
    #{range := Range} = osiris_log:overview(LDir),
    ?assertEqual({0, 1}, Range),

    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{dir => LDir},
    _ = osiris_log:init(Conf),
    #{shared := Shared} = Conf,
    osiris_log_shared:set_committed_chunk_id(Shared, 2),
    osiris_log_shared:set_last_chunk_id(Shared, 2),

    % after osiris_log:init, the sizes of the index and segment files
    % should be as they were before they got corrrupted
    ?assertEqual(ValidIdxSize, filelib:file_size(IdxPath)),
    ?assertEqual(ValidSegSize, filelib:file_size(SegPath)),

    % the range should not include the corrupted chunk
    #{range := Range} = osiris_log:overview(LDir),
    ?assertEqual({0, 1}, Range),

    % a consumer asking for the last chunk, should receive "two"
    {ok, R0} = osiris_log:init_offset_reader(last, Conf),
    {ReadChunk, R1} = osiris_log:read_chunk_parsed(R0),
    ?assertMatch([{1, <<"two">>}], ReadChunk),
    osiris_log:close(R1),
    ok.

init_only_one_corrupted_segment(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
    [begin {1, [Data || _ <- lists:seq(1, 50)]} end
     || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    Log = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log),

    % we should have 2 segments for this test
    IdxFiles = filelib:wildcard(filename:join(LDir, "*.index")),
    ?assertEqual(2, length(IdxFiles)),
    SegFiles = filelib:wildcard(filename:join(LDir, "*.segment")),
    ?assertEqual(2, length(SegFiles)),

    % delete the first segment
    ok = file:delete(hd(IdxFiles)),
    ok = file:delete(hd(SegFiles)),

    % truncate the last segment
    LastIdxFile = lists:last(IdxFiles),
    LastSegFile = lists:last(SegFiles),
    {ok, IdxFd} = file:open(LastIdxFile, [raw, binary, write]),
    ok = file:close(IdxFd),
    {ok, SegFd} = file:open(LastSegFile, [raw, binary, write]),
    ok = file:close(SegFd),

    % init
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{dir => LDir},
    _ = osiris_log:init(Conf),
    set_shared(Conf, 1000),

    % there should be one segment, with nothing but headers
    % and its name should be the same as it was before
    % (the file is used to store the last known chunk ID)
    ?assertEqual(?LOG_HEADER_SIZE, filelib:file_size(LastSegFile)),
    ?assertEqual(?IDX_HEADER_SIZE, filelib:file_size(LastIdxFile)),
    {ok, IdxFd2} = file:open(LastIdxFile, [read, raw, binary]),
    {ok, ?IDX_HEADER} = file:read(IdxFd2, ?IDX_HEADER_SIZE),
    ok = file:close(IdxFd2),
    {ok, SegFd2} = file:open(LastSegFile, [read, raw, binary]),
    {ok, ?LOG_HEADER} = file:read(SegFd2, ?LOG_HEADER_SIZE),
    ok = file:close(SegFd2),
    ok.

init_empty_last_files(Config) ->
    % this test intentionally corrupts the log by truncating
    % both the segment and the index files to zero bytes
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
    [begin {1, [Data || _ <- lists:seq(1, 50)]} end
     || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    Log = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log),

    % we should have 2 segments for this test
    IdxFiles = filelib:wildcard(filename:join(LDir, "*.index")),
    ?assertEqual(2, length(IdxFiles)),
    SegFiles = filelib:wildcard(filename:join(LDir, "*.segment")),
    ?assertEqual(2, length(SegFiles)),

    % truncate both files (by opening with "write" but without "read")
    LastIdxFile = lists:last(IdxFiles),
    LastSegFile = lists:last(SegFiles),
    {ok, IdxFd} = file:open(LastIdxFile, [raw, binary, write]),
    _ = file:close(IdxFd),
    {ok, SegFd} = file:open(LastSegFile, [raw, binary, write]),
    _ = file:close(SegFd),

    ?assertEqual(#{range => {0,699},
                   epoch_offsets => [{1,650}]}, osiris_log:overview(LDir)),

    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{dir => LDir},
    _ = osiris_log:init(Conf),
    set_shared(Conf, 2),

    % the last segment and index files should no longer exist
    RecoveredIdxFiles =
        lists:map(fun filename:basename/1,
                  filelib:wildcard(
                    filename:join(LDir, "*.index"))),
    ?assertEqual(["00000000000000000000.index"],
                 RecoveredIdxFiles, "an empty index file was not deleted"),
    RecoveredSegFiles =
        lists:map(fun filename:basename/1,
                  filelib:wildcard(
                    filename:join(LDir, "*.segment"))),
    ?assertEqual(["00000000000000000000.segment"],
                 RecoveredSegFiles, "an empty segment file was not deleted"),
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
    ?assertEqual([],
                 lists:filter(fun(S) ->
                                 lists:suffix("00000000000000000000.segment", S)
                              end,
                              SegFiles),
                 "the retention process didn't delete the oldest segment"),
    ok.

evaluate_retention_max_age_empty(Config) ->
    %% simulates what may occur if retention is evaluated whilst
    %% the stream member is being deleted
    %% it should not crash
    LDir = ?config(leader_dir, Config),
    Spec = [{max_bytes, 100000000}, {max_age, 1000}],
    _ = osiris_log:evaluate_retention(LDir, Spec),
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
    %% than the retention of 1000ms; max_bytes shouldn't affect the result
    Spec = [{max_bytes, 100000000}, {max_age, 1000}],
    Range = osiris_log:evaluate_retention(LDir, Spec),
    %% idempotency
    Range = osiris_log:evaluate_retention(LDir, Spec),
    SegFiles =
        filelib:wildcard(
            filename:join(LDir, "*.segment")),
    ?assertEqual(1, length(SegFiles)),
    ?assertEqual([],
                 lists:filter(fun(S) ->
                                 lists:suffix("00000000000000000000.segment", S)
                              end,
                              SegFiles),
                 "the retention process didn't delete the oldest segment"),
    ok.

evaluate_retention_fun(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    LDir = ?config(leader_dir, Config),
    Log = seed_log(LDir, EpochChunks, Config),
    osiris_log:close(Log),
    %% delete only the first segment
    Fun = fun(IdxFile) ->
                  ct:pal("Eval retention for ~p", [IdxFile]),
                  filename:basename(IdxFile) =:= <<"00000000000000000000.index">>
          end,
    Spec = {'fun', Fun},
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    %% idempotency check
    Range = osiris_log:evaluate_retention(LDir, [Spec]),
    SegFiles =
        filelib:wildcard(
            filename:join(LDir, "*.segment")),
    ?assertMatch([_], SegFiles),
    ?assertEqual([],
                 lists:filter(fun(S) ->
                                 lists:suffix("00000000000000000000.segment", S)
                              end,
                              SegFiles),
                 "the retention process didn't delete the oldest segment"),
    ok.

offset_tracking(Config) ->
    Conf = ?config(osiris_conf, Config),
    S0 = osiris_log:init(Conf),
    T0 = osiris_tracking:add(<<"id1">>, offset, 0, undefined,
                             osiris_tracking:init(undefined, #{})),
    {Trailer, T1} = osiris_tracking:flush(T0),
    ?assertEqual(0, osiris_log:next_offset(S0)),
    S1 = osiris_log:write([<<"hi">>], ?CHNK_USER, ?LINE, Trailer, S0),
    ?assertEqual(1, osiris_log:next_offset(S1)),
    T2 = osiris_tracking:add(<<"id1">>, offset, 1, osiris_log:next_offset(S1), T1),
    {Trailer2, _T3} = osiris_tracking:flush(T2),
    S2 = osiris_log:write([<<"hi">>], ?CHNK_USER, ?LINE, Trailer2, S1),
    %% test recovery
    osiris_log:close(S2),
    S3 = osiris_log:init(Conf),
    T = osiris_log:recover_tracking(S3),
    ?assertMatch(#{offsets := #{<<"id1">> := 1}}, osiris_tracking:overview(T)),
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
    S00 = osiris_log:init(Conf),

    T0 = osiris_tracking:add(<<"wid1">>, sequence, 2, 0,
           osiris_tracking:add(<<"id1">>, offset, 1, undefined,
                             osiris_tracking:init(undefined, #{}))),
    S0 = osiris_log:write([<<"hi">>],
                          ?CHNK_USER,
                          ?LINE,
                          make_trailer(sequence, <<"wid1">>, 2),
                          S00),
    %% write a tracking entry
    S1 = osiris_log:write([],
                          ?CHNK_TRK_DELTA,
                          ?LINE,
                          make_trailer(offset, <<"id1">>, 1),
                          S0),
    %% this should create at least two segments
    {_, S2} = seed_log(Conf, S1, EpochChunks, Config, T0),
    osiris_log:close(S2),
    S3 = osiris_log:init(Conf),
    T = osiris_log:recover_tracking(S3),
    ?assertMatch(#{offsets := #{<<"id1">> := 1},
                   sequences := #{<<"wid1">> := {_, 2}}},
                 osiris_tracking:overview(T)),
    osiris_log:close(S3),
    ok.

small_chunk_overview(Config) ->
    Data = crypto:strong_rand_bytes(1000),
    ChSz = 1,
    ChNum = 8196 * 4,
    EpochChunks =
        [begin
            [{E, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)]
         end || E <- lists:seq(1, 10)],
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{max_segment_size_bytes => 128_000_000},
    osiris_log:close(seed_log(Conf, lists:flatten(EpochChunks), Config)),
    Dir = maps:get(dir, Conf),
    {UnsortedTaken, Files} = timer:tc(fun () -> osiris_log:index_files_unsorted(Dir) end),
    {SortedTaken, _} = timer:tc(fun () -> osiris_log:sorted_index_files(Dir) end),

    ct:pal("UnsortedTaken ~bms, SortedTaken ~bms, num files ~b",
           [UnsortedTaken div 1000, SortedTaken div 1000, length(Files)]),
    {OverviewTaken, LogOverview} = timer:tc(fun () ->
                                                    osiris_log:overview(Dir)
                                            end),
    ct:pal("OverviewTaken ~bms", [OverviewTaken div 1000]),
    ?assertMatch(#{range := {0,327839},
                   epoch_offsets := [{1,32783},
                                     {2,65567},
                                     {3,98351},
                                     {4,131135},
                                     {5,163919},
                                     {6,196703},
                                     {7,229487},
                                     {8,262271},
                                     {9,295055},
                                     {10,327839}]}, LogOverview),
    {InitTaken100, {ok, L100}} = timer:tc(
                             fun () ->
                                     osiris_log:init_offset_reader(257000, Conf)
                             end),
    osiris_log:close(L100),
    ct:pal("InitTaken100 ~bms", [InitTaken100 div 1000]),
    {InitTaken, {ok, L}} = timer:tc(
                             fun () ->
                                     osiris_log:init_offset_reader(245869, Conf)
                             end),
    ct:pal("InitTaken ~bms next ~b", [InitTaken div 1000,osiris_log:next_offset(L)]),
    osiris_log:close(L),
    ok.

many_segment_overview(Config) ->
    Data = crypto:strong_rand_bytes(1000),
    ChSz = 8,
    ChNum = 1024,
    EpochChunks =
        [{1, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)] ++
        [{2, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)] ++
        [{3, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)] ++
        [{4, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)] ++
        [{5, [Data || _ <- lists:seq(1, ChSz)]} || _ <- lists:seq(1, ChNum)],
    Conf0 = ?config(osiris_conf, Config),
    Conf = Conf0#{max_segment_size_bytes => 64000},
    Dir = maps:get(dir, Conf),
    osiris_log:close(seed_log(Conf, EpochChunks, Config)),
    {UnsortedTaken, Files} = timer:tc(fun () -> osiris_log:index_files_unsorted(Dir) end),
    {SortedTaken, _} = timer:tc(fun () -> osiris_log:sorted_index_files(Dir) end),
    ct:pal("UnsortedTaken ~bms, SortedTaken ~bms, num files ~b",
           [UnsortedTaken div 1000, SortedTaken div 1000, length(Files)]),
    %% {40051,{{0,40959},[{1,8184},{2,16376},{3,24568},{4,32760},{5,40952}]}}
    {OverviewTaken, LogOverview} = timer:tc(fun () ->
                                                    osiris_log:overview(maps:get(dir, Conf))
                                            end),
    ct:pal("OverviewTaken ~bms", [OverviewTaken div 1000]),
    ct:pal("~p", [LogOverview]),
    %% {{0,40959},[{-1,-1},{1,8184},{2,16376},{3,24568},{4,32760},{5,40952}]}
    ?assertEqual(#{epoch_offsets => [{1,8184},{2,16376},{3,24568},{4,32760},{5,40952}],
                   range => {0,40959}}, LogOverview),
    Conf6 = Conf#{epoch => 6},

    {InitTaken, _} = timer:tc(
                       fun () ->
                               osiris_log:close(osiris_log:init(Conf6))
                       end),
    ct:pal("InitTaken ~bms", [InitTaken div 1000]),

    {OffsLastTaken, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(last, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsLastTaken ~p", [OffsLastTaken]),

    {OffsFirstTaken, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(first, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsFirstTaken ~p", [OffsFirstTaken]),

    {OffsNextTaken, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(next, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsNextTaken ~p", [OffsNextTaken]),

    {OffsOffsetTakenHi, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(40000, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsOffsetTakenHi ~p", [OffsOffsetTakenHi]),
    {OffsOffsetTakenLow, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(400, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsOffsetTakenLow ~p", [OffsOffsetTakenLow]),


    {OffsOffsetTakenMid, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader(20000, Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("OffsOffsetTakenMid ~p", [OffsOffsetTakenMid]),

    %% TODO: timestamp
    Ts = erlang:system_time(millisecond) - 100,
    {TimestampTaken, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_offset_reader({timestamp, Ts},
                                                                 Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("TimestampTaken ~p", [TimestampTaken]),

    %% acceptor
    {InitAcceptorTaken, AcceptorLog} =
        timer:tc(fun () ->
                         osiris_log:init_acceptor(LogOverview, Conf6)
                 end),
    ct:pal("InitAcceptor took ~bus", [InitAcceptorTaken]),

    {InitDataReaderTaken, _} =
        timer:tc(fun () ->
                         {ok, L} = osiris_log:init_data_reader(
                                     osiris_log:tail_info(AcceptorLog), Conf6),
                         osiris_log:close(L)
                 end),
    ct:pal("InitDataReaderTaken ~p", [InitDataReaderTaken]),

    %% evaluate_retention
    Specs = [{max_age, 60000}, {max_bytes, 5_000_000_000}],
    {RetentionTaken, _} =
        timer:tc(fun () ->
                         osiris_log:evaluate_retention(maps:get(dir, Conf), Specs)
                 end),
    ct:pal("RetentionTaken ~p", [RetentionTaken]),
    ok.

init_partial_writes(Config) ->
    run_scenario(Config, []),
    %% chunk body missing
    run_scenario(Config, [{segment, {eof, -27}}]),
    %% chunk body missing, empty space
    run_scenario(Config, 23, 23, [{segment, {eof, -27}},
                                  {segment, {eof, 27}}]),
    %% last index record missing
    run_scenario(Config, [{index, {eof, -?INDEX_RECORD_SIZE_B}}]),

    run_scenario(Config, [{index, 4096},
                          {segment, 4096 * 10}]),
    run_scenario(Config, [{segment, 0}]),
    ok.

run_scenario(Config, Scenario) ->
    run_scenario(Config, 2, 23, Scenario).

run_scenario(Config, NumChunks, MsgSize, Scenario) ->
    ct:pal("Running ~p", [Scenario]),
    LDir = ?config(leader_dir, Config),
    Conf = #{dir => LDir,
             epoch => 1,
             max_segment_size_bytes => 4096 * 256,
             name => ?config(test_case, Config)},
    Msg = crypto:strong_rand_bytes(MsgSize),
    EpochChunks = [{1, [Msg]} || _ <- lists:seq(1, NumChunks)],
    Log0 = seed_log(LDir, EpochChunks, Config),
    SegPath = filename:join(LDir, "00000000000000000000.segment"),
    IdxPath = filename:join(LDir, "00000000000000000000.index"),
    % record the segment and index sizes before corrupting the log
    % ValidSegSize = filelib:file_size(SegPath),
    % ValidIdxSize = filelib:file_size(IdxPath),
    osiris_log:close(Log0),

    [begin
         case T of
             {index, At} ->
                 truncate_at(IdxPath, At);
             {segment, At} ->
                 truncate_at(SegPath, At)
         end
     end || T <- Scenario],

    Conf2 = Conf#{epoch => 2},
    Log1 = osiris_log:init(Conf2),
    _ = osiris_log:recover_tracking(Log1),
    Log2 = osiris_log:write([<<"AAAAFFFFTTTEEEERRR">>], Log1),
    osiris_log:close(Log2),

    Log3 = osiris_log:init(Conf2#{epoch => 3}),
    _ = osiris_log:recover_tracking(Log3),
    _ = osiris_log:close(Log3),

    %% this reads and parses every chunk in the segment
    dump_segment(SegPath),
    delete_dir(LDir),

    ok.

init_with_unexpected_file(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Dir = ?config(dir, Config),
    Log = seed_log(Dir, EpochChunks, Config),
    osiris_log:close(Log),
    Segments = filelib:wildcard(filename:join(Dir, "*.segment")),
    Indexes = filelib:wildcard(filename:join(Dir, "*.index")),
    ?assertEqual(2, length(Segments)),
    ?assertEqual(2, length(Indexes)),
    ok = file:write_file(filename:join(Dir, ".nfs000000000000000000"), <<"bananas">>),
    _ = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(#{range => {0,999},
                   epoch_offsets => [{1,950}]}, osiris_log:overview(Dir)),
    ok.

overview_with_missing_segment(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Dir = ?config(dir, Config),
    Log = seed_log(Dir, EpochChunks, Config),
    osiris_log:close(Log),
    Segments =
        filelib:wildcard(
            filename:join(?config(dir, Config), "*.segment")),
    Indexes =
        filelib:wildcard(
            filename:join(?config(dir, Config), "*.index")),
    ?assertEqual(2, length(Segments)),
    SegDelete = hd(lists:reverse(lists:sort(Segments))),
    ok = file:delete(SegDelete),
    %% empty index file (this case was reported by user:
    %% https://github.com/rabbitmq/rabbitmq-server/issues/12036)
    truncate_at(lists:last(Indexes), 0),
    %% init should delete any index files without corresponding segments
    _ = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(1, length(
                      filelib:wildcard(
                        filename:join(?config(dir, Config), "*.segment")))),
    ?assertEqual(1, length(
                      filelib:wildcard(
                        filename:join(?config(dir, Config), "*.index")))),
    ok.

overview_with_missing_index_at_start(Config) ->
    Data = crypto:strong_rand_bytes(1500),
    EpochChunks =
        [begin {1, [Data || _ <- lists:seq(1, 50)]} end
         || _ <- lists:seq(1, 20)],
    Dir = ?config(dir, Config),
    Log = seed_log(Dir, EpochChunks, Config),
    osiris_log:close(Log),
    Indexes =
        filelib:wildcard(
            filename:join(?config(dir, Config), "*.index")),
    ?assertEqual(2, length(Indexes)),
    IdxDelete = hd(lists:sort(Indexes)),
    ok = file:delete(IdxDelete),
    %% missing segment file due to crash during retention deletion
    %% https://github.com/rabbitmq/rabbitmq-server/issues/12036)
    %% init should delete any index files without corresponding segments
    _ = osiris_log:init(?config(osiris_conf, Config)),
    ?assertEqual(1, length(
                      filelib:wildcard(
                        filename:join(?config(dir, Config), "*.segment")))),
    ?assertEqual(1, length(
                      filelib:wildcard(
                        filename:join(?config(dir, Config), "*.index")))),
    ok.

read_ahead_send_file(Config) ->
    RAL = 4096, %% read ahead limit
    HS = ?HEADER_SIZE_B,
    Tests =
    [
     fun(write, #{w := W0}) ->
             Entries = [<<"hiiiiiiiii">>, <<"hooooooo">>],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% first chunk, we read the header and used sendfile
             Entries = [<<"hiiiiiiiii">>, <<"hooooooo">>],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, R1} = osiris_log:send_file(CS, R0),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
             ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
             R1
     end,
     fun(write, #{w := W0}) ->
             {_, W1} = write_committed([<<"hi">>, <<"ho">>], W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% small chunk, we'll read it ahead, no sendfile
             [_, _, D, _] = ra_fake_chunk([<<"hi">>, <<"ho">>]),
             {ok, R1} = osiris_log:send_file(CS, R0),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(1, osiris_tracer:call_count(T)),
             R1
     end,
     fun(write, #{w := W0}) ->
             Entries = [<<"foo">>, binary:copy(<<"b">>, RAL * 2)],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% large chunk, we read ahead the header before,
             %% but we'll use sendfile for the data
             Entries = [<<"foo">>, binary:copy(<<"b">>, RAL * 2)],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, R1} = osiris_log:send_file(CS, R0),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(0, osiris_tracer:call_count(T, file, pread)),
             ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
             R1
     end,
     fun(write, #{w := W0}) ->
             Entries = [<<"ho">>, {<<"banana">>, <<"hi">>}],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, fsize := FSize, rtype := RType,
                 tracer := T}) ->
             %% chunk with a non-empty filter
             %% data are small enough, they should be read with the header
             {ok, R1} = osiris_log:send_file(CS, R0),
             Entries = [<<"ho">>, {<<"banana">>, <<"hi">>}],
             [_, BloomD, ED, _] = ra_fake_chunk(Entries, FSize),
             Expected = case RType of
                            data ->
                                %% we expect the bloom filter data
                                <<BloomD/binary, ED/binary>>;
                            offset ->
                                ED
                        end,
             {ok, Read} = recv(SS, byte_size(Expected) + HS),
             ?assertEqual(Expected, binary:part(Read, HS, byte_size(Read) - HS)),
             %% no read ahead from before, and need a second read for the filter
             ?assertEqual(2, osiris_tracer:call_count(T)),
             ?assertEqual(2, osiris_tracer:call_count(T, file, pread)),
             R1
     end,
     fun(write, #{w := W0}) ->
             Entries = [binary:copy(<<"a">>, RAL div 2 - HS * 3)],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% chunk about half the size of the read ahead limit
             %% we read it ahead already
             {ok, R1} = osiris_log:send_file(CS, R0),
             Entries = [binary:copy(<<"a">>, RAL div 2 - HS * 3)],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(0, osiris_tracer:call_count(T)),
             R1
     end,
     fun(write, #{w := W0}) ->
             Entries = [binary:copy(<<"b">>, RAL div 2 - HS * 3)],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% chunk about half the size of the read ahead limit
             %% we read it ahead already
             {ok, R1} = osiris_log:send_file(CS, R0),
             Entries = [binary:copy(<<"b">>, RAL div 2 - HS * 3)],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(0, osiris_tracer:call_count(T)),
             R1
     end,
     fun(write, #{w := W0}) ->
             Entries = [binary:copy(<<"c">>, RAL + 1000)],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% this one is too big to be read ahead fully
             {ok, R1} = osiris_log:send_file(CS, R0),
             Entries = [binary:copy(<<"c">>, RAL + 1000)],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             %% we had the header already, no need to read
             ?assertEqual(0, osiris_tracer:call_count(T, file, pread)),
             %% send the data with sendfile
             ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
             R1
     end
    ],

    FilterSizes = [?DEFAULT_FILTER_SIZE, ?DEFAULT_FILTER_SIZE * 2],
    RTypes = [data, offset],
    Conf0 = ?config(osiris_conf, Config),
    #{dir := Dir0} = Conf0,
    [begin
         Dir1 = filename:join(Dir0, io_lib:format("~p~p", [FSize, RType])),
         Conf1 = Conf0#{dir => Dir1,
                        filter_size => FSize},
         Wr0 = osiris_log:init(Conf1),
         Shared = osiris_log:get_shared(Wr0),
         RConf = Conf1#{shared => Shared, transport => tcp},
         {ok, Rd0} = init_reader(RType, RConf),

         {CS, SS, _} = CSC = client_server_connect(),

         #{w := Wr1, r := Rd1} = run_read_ahead_tests(Tests, RType,
                                                      FSize, Wr0, Rd0,
                                                      #{ss => SS,
                                                        cs => CS}),

         client_server_close(CSC),

         osiris_log:close(Rd1),
         osiris_log:close(Wr1)
     end || FSize <- FilterSizes, RType <- RTypes],
    ok.

read_ahead_send_file_filter(Config) ->
    RAL = 4096, %% read ahead limit
    HS = ?HEADER_SIZE_B,
    %% we store the entry size on 4 bytes, so we must substract them from the data size
    MaxEntrySize = RAL - 11 - HS,
    DFS = ?DEFAULT_FILTER_SIZE,
    % FilterSizes = [DFS * 2],
    FilterSizes = [DFS, DFS * 2],
    Conf0 = ?config(osiris_conf, Config),
    #{dir := Dir0} = Conf0,

    [begin
         Dir1 = filename:join(Dir0, integer_to_list(FSize)),
         Conf1 = Conf0#{dir => Dir1,
                        filter_size => FSize},
         Wr0 = osiris_log:init(Conf1),
         Shared = osiris_log:get_shared(Wr0),
         RConf = Conf1#{shared => Shared, transport => tcp},
         {ok, Rd0} = osiris_log:init_offset_reader(first, RConf),

         %% compute the max entry size
         %% (meaning we don't read ahead enough above this entry size)
         %% first we don't know the actual filter size in the stream,
         %% so we assume the default filter size
         %% this "reduces" the max size of data we can read in the case
         %% of a larger-than-default filter size, because of the extra
         %% bytes that belong to the filter
         MES1 = MaxEntrySize - FSize,
         %% then the max entry becomes accurate, whatever the actual filter size
         MES2 = MaxEntrySize,
         ct:pal("Using MES1 ~p, MES2 ~p for filter size ~p",
                [MES1, MES2, FSize]),

         Tests =
         [
          fun(write, #{w := W0}) ->
                  Entries = [{<<"banana">>, <<"aaa">>}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  Entries = [{<<"banana">>, <<"aaa">>}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  case FSize =:= DFS of
                      true ->
                          %% just read the header
                          ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                          %% no read ahead data yet, used sendfile
                          ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile));
                      false ->
                          %% read the header, but the filter data did not fit,
                          %% so read again
                          ?assertEqual(2, osiris_tracer:call_count(T, file, pread)),
                          %% read the data in the second pread call
                          ?assertEqual(0, osiris_tracer:call_count(T, file, sendfile))
                  end,
                  R1
          end,
          fun(write, #{w := W0}) ->
                  EData = binary:copy(<<"a">>, MES1),
                  Entries = [{<<"banana">>, EData}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  %% data just fit in the read ahead
                  EData = binary:copy(<<"a">>, MES1),
                  Entries = [{<<"banana">>, EData}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  case FSize =:= DFS of
                      true ->
                          %% just read the header
                          ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                          %% data just fit in the read ahead, no sendfile
                          ?assertEqual(0, osiris_tracer:call_count(T, file, sendfile));
                      false ->
                          %% read the header ahead previously
                          %% (because it could not read the filter at first,
                          %% which triggered the read-ahead)
                          ?assertEqual(0, osiris_tracer:call_count(T, file, pread)),
                          ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile))
                  end,
                  R1
          end,
          fun(write, #{w := W0}) ->
                  EData = binary:copy(<<"b">>, MES1 + 1),
                  Entries = [{<<"banana">>, EData}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  %% data do not fit in the read ahead
                  EData = binary:copy(<<"b">>, MES1 + 1),
                  Entries = [{<<"banana">>, EData}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  %% just read the header
                  ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                  %% large chunk, used sendfile
                  ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
                  R1
          end,
          fun(write, #{w := W0}) ->
                  Entries = [{<<"banana">>, <<"aaa">>}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  %% read ahead is disabled, but will be reenabled
                  Entries = [{<<"banana">>, <<"aaa">>}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                  ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
                  R1
          end,
          fun(write, #{w := W0}) ->
                  EData = binary:copy(<<"b">>, MES1 div 2),
                  Entries = [{<<"banana">>, EData}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  %% previous chunk enabled read ahead
                  EData = binary:copy(<<"b">>, MES1 div 2),
                  Entries = [{<<"banana">>, EData}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                  ?assertEqual(0, osiris_tracer:call_count(T, file, sendfile)),
                  R1
          end,
          fun(write, #{w := W0}) ->
                  EData = binary:copy(<<"b">>, MES1 * 2),
                  Entries = [{<<"banana">>, EData}],
                  {_, W1} = write_committed(Entries, W0),
                  W1;
             (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
                  %% large chunk, we had the header already,
                  EData = binary:copy(<<"b">>, MES1 * 2),
                  Entries = [{<<"banana">>, EData}],
                  [_, _, D, _] = ra_fake_chunk(Entries),
                  {ok, R1} = osiris_log:send_file(CS, R0),
                  {ok, Read} = recv(SS, byte_size(D) + HS),
                  ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
                  ?assertEqual(0, osiris_tracer:call_count(T, file, pread)),
                  ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
                  R1
          end
         ],

         {CS, SS, _} = CSC = client_server_connect(),

         #{w := Wr1, r := Rd1} = run_read_ahead_tests(Tests, offset, FSize,
                                                      Wr0, Rd0, #{ss => SS,
                                                                  cs => CS}),

         client_server_close(CSC),

         osiris_log:close(Rd1),
         osiris_log:close(Wr1)
     end || FSize <- FilterSizes],

    ok.

read_ahead_send_file_on_off(Config) ->
    HS = ?HEADER_SIZE_B,
    Tests =
    [
     fun(write, #{w := W0}) ->
             Entries = [<<"hiiiiiiiii">>, <<"hooooooo">>],
             {_, W1} = write_committed(Entries, W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T}) ->
             %% first chunk, we read the header and used sendfile
             Entries = [<<"hiiiiiiiii">>, <<"hooooooo">>],
             [_, _, D, _] = ra_fake_chunk(Entries),
             {ok, R1} = osiris_log:send_file(CS, R0),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
             ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile)),
             R1
     end,
     fun(write, #{w := W0}) ->
             {_, W1} = write_committed([<<"hi">>, <<"ho">>], W0),
             W1;
        (read, #{r := R0, cs := CS, ss := SS, tracer := T, ra_on := RaOn}) ->
             [_, _, D, _] = ra_fake_chunk([<<"hi">>, <<"ho">>]),
             {ok, R1} = osiris_log:send_file(CS, R0),
             {ok, Read} = recv(SS, byte_size(D) + HS),
             ?assertEqual(D, binary:part(Read, HS, byte_size(Read) - HS)),
             case RaOn of
                 true ->
                     ?assertEqual(1, osiris_tracer:call_count(T));
                 false ->
                     ?assertEqual(1, osiris_tracer:call_count(T, file, pread)),
                     ?assertEqual(1, osiris_tracer:call_count(T, file, sendfile))
             end,
             R1
     end
    ],

    RaOnOff = [true, false],
    RTypes = [offset, data],
    FSize = ?DEFAULT_FILTER_SIZE,
    Conf0 = ?config(osiris_conf, Config),
    #{dir := Dir0} = Conf0,
    [begin
         Dir1 = filename:join(Dir0, io_lib:format("ra_on_~p", [RaOn])),
         Conf1 = Conf0#{dir => Dir1},
         Wr0 = osiris_log:init(Conf1),
         Shared = osiris_log:get_shared(Wr0),
         RConf = Conf1#{shared => Shared, transport => tcp,
                        options => #{read_ahead => RaOn}},
         {ok, Rd0} = init_reader(RType, RConf),

         {CS, SS, _} = CSC = client_server_connect(),

         #{w := Wr1, r := Rd1} = run_read_ahead_tests(Tests, RType,
                                                      FSize, Wr0, Rd0,
                                                      #{ss => SS,
                                                        cs => CS,
                                                        ra_on => RaOn}),

         client_server_close(CSC),

         osiris_log:close(Rd1),
         osiris_log:close(Wr1)
     end || RaOn <- RaOnOff, RType <- RTypes],
    ok.

%% Utility

init_reader(offset, Conf) ->
    osiris_log:init_offset_reader(first, Conf);
init_reader(data, Conf) ->
    osiris_log:init_data_reader({0, empty}, Conf).

client_server_connect() ->
    %% server, we will read stream data from this socket
    {ok, SLS} = gen_tcp:listen(0, [binary, {packet, 0},
                                   {active, false}]),
    {ok, Port} = inet:port(SLS),

    %% client, osiris will send to this socket
    {ok, CS} = gen_tcp:connect("localhost", Port,
                               [binary, {packet, 0}]),

    {ok, SS} = gen_tcp:accept(SLS),

    {CS, SS, SLS}.

client_server_close({CS, SS, SLS}) ->
    ok = gen_tcp:close(CS),
    ok = gen_tcp:close(SS),
    ok = gen_tcp:close(SLS).

run_read_ahead_tests(Tests, RType, FSize, Wr0, Rd0) ->
    run_read_ahead_tests(Tests, RType, FSize, Wr0, Rd0, #{}).

run_read_ahead_tests(Tests, RType, FSize, Wr0, Rd0, Ctx0) ->
    {_, W} = lists:foldl(fun(F, {N, Ctx}) ->
                                 ct:pal("Read ahead test ~p, writing...", [N]),
                                 W  = F(write, Ctx),
                                 {N + 1, Ctx#{w => W}}
                         end, {1, Ctx0#{w => Wr0}}, Tests),
    {_, R} =
        lists:foldl(fun(F, {N, Ctx}) ->
                            ct:pal("Read ahead test ~p, reading...", [N]),
                            Tracer = osiris_tracer:start([{file, pread, '_'},
                                                          {file, sendfile, '_'}]),
                            Res = F(read, Ctx#{tracer => Tracer}),
                            osiris_tracer:stop(Tracer),
                            CtxOut = case is_map(Res) of
                                         true ->
                                             %% full context returned
                                             Res;
                                         false ->
                                             %% assume osiris log reader
                                             Ctx#{r => Res}
                                     end,

                            {N + 1, CtxOut}
                    end, {1, Ctx0#{r => Rd0, rtype => RType, fsize => FSize}}, Tests),
    maps:merge(W, R).

truncate_at(File, Pos) ->
    {ok, Fd} = file:open(File, [raw, binary, read, write]),
    % truncate the file so that chunk <<four>> is missing and <<three>> is corrupted
    {ok, _} = file:position(Fd, Pos),
    _ = file:truncate(Fd),
    _ = file:close(Fd),
    ok.

seed_log(Conf, EpochChunks, Config) ->
    Trk = osiris_tracking:init(undefined, #{}),
    element(2, seed_log(Conf, EpochChunks, Config, Trk)).

seed_log(Conf, EpochChunks, Config, Trk) when is_map(Conf) ->
    Log0 = osiris_log:init(Conf),
    seed_log(Conf, Log0, EpochChunks, Config, Trk);
seed_log(Dir, EpochChunks, Config, Trk) when is_list(Dir) ->
    seed_log(#{dir => Dir,
               epoch => 1,
               max_segment_size_bytes => 1000 * 1000,
               name => ?config(test_case, Config)},
             EpochChunks, Config, Trk).

seed_log(Conf, Log, EpochChunks, _Config, Trk) ->
    lists:foldl(fun ({Epoch, Records}, {T, L}) ->
                        write_chunk(Conf, Epoch, now_ms(), Records, T, L);
                    ({Epoch, Ts, Records}, {T, L}) ->
                        write_chunk(Conf, Epoch, Ts, Records, T, L)
                end,
                {Trk, Log}, EpochChunks).

write_chunk(Conf, Epoch, Now, Records, Trk0, Log0) ->
    %% the osiris_writer pass records in reverse order to avoid
    %% unnecessary reversals.
    %% We, however, need to reverse here not to make the tests look weird
    {Log1, Trk1} = osiris_log:evaluate_tracking_snapshot(Log0, Trk0),
    case osiris_log:get_current_epoch(Log1) of
        Epoch ->
            {Trk1, osiris_log:write(lists:reverse(Records), Now, Log1)};
        _ ->
            %% need to re-init as new epoch
            osiris_log:close(Log1),
            Log = osiris_log:init(Conf#{epoch => Epoch}),
            {Trk1, osiris_log:write(lists:reverse(Records), Log)}
    end.

now_ms() ->
    erlang:system_time(millisecond).

make_trailer(Type, K, V) ->
    T = case Type of
            sequence -> 0;
            offset -> 1
        end,
    <<T:8/unsigned,
      (byte_size(K)):8/unsigned,
      K/binary,
      V:64/unsigned>>.

set_shared(#{shared := Ref}, Value) ->
    osiris_log_shared:set_committed_chunk_id(Ref, Value).

%% writes and commits the chunk
write_committed(Entries, S0) ->
    ChId = osiris_log:next_offset(S0),
    S = osiris_log:write(Entries, S0),
    Shared = osiris_log:get_shared(S0),
    osiris_log_shared:set_committed_chunk_id(Shared, ChId),
    osiris_log_shared:set_last_chunk_id(Shared, ChId),
    {ChId, S}.

dump_segment(Path) ->
    dump_segment0(osiris_log:dump_init(Path)).

dump_segment0(Fd) ->
    case osiris_log:dump_chunk(Fd) of
        eof ->
            ok;
        #{crc_match := false} = Ch ->
            ct:fail("crc invalid ~p", [Ch]);
        _Map ->
            % ct:pal("DUMP CHUNK: ~p", [Map]),
            dump_segment0(Fd)
    end.

delete_dir(Dir) ->
    case file:list_dir(Dir) of
        {ok, Files} ->
            [ok =
                 file:delete(
                     filename:join(Dir, F))
             || F <- Files],
            ok = file:del_dir(Dir);
        {error, enoent} ->
            ok
    end.

ra_fake_chunk(Blobs) ->
    ra_fake_chunk(Blobs, ?DEFAULT_FILTER_SIZE).

ra_fake_chunk(Blobs, FSize) ->
    [HD, BloomD, ED, TD] = fake_chunk(Blobs, ?LINE, 1, 100, FSize),
    [iolist_to_binary(HD), iolist_to_binary(BloomD),
     iolist_to_binary(ED), iolist_to_binary(TD)].

fake_chunk_bin(Blobs, Ts, Epoch, NextChId) ->
    iolist_to_binary(fake_chunk(Blobs, Ts, Epoch, NextChId)).

fake_chunk(Blobs, Ts, Epoch, NextChId) ->
    fake_chunk(Blobs, Ts, Epoch, NextChId, ?DEFAULT_FILTER_SIZE).

fake_chunk(Blobs, Ts, Epoch, NextChId, FSize) ->
    element(1,
            osiris_log:make_chunk(Blobs, <<>>, 0, Ts, Epoch, NextChId,
                                  FSize)).

recv(Socket, Expected) ->
    recv(Socket, Expected, <<>>).

recv(_Socket, 0, Acc) ->
    Acc;
recv(Socket, Expected, Acc) ->
    case gen_tcp:recv(Socket, Expected, 10_000) of
        {ok, Data} when byte_size(Data) == Expected ->
            {ok, Data};
        {ok, Data} when byte_size(Data) < Expected ->
            {ok, recv(Socket, Expected - byte_size(Data),
                      <<Acc/binary, Data/binary>>)};
        Other ->
            Other
    end.
