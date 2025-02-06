-module(osiris_segment_reader).

-include("osiris.hrl").
-include("osiris_log.hrl").
-include_lib("kernel/include/file.hrl").

-export([init_offset_reader/3,
         init_data_reader/2,
         sorted_index_files/1,
         offset_range_from_idx_files/1,
         build_seg_info/1,
         find_segment_for_offset/2,
         first_and_last_seginfos/1]).

%% TODO? for TESTS
-export([index_files_unsorted/1]).


-record(cfg,
        {directory :: file:filename_all(),
         name :: osiris:name(),
         max_segment_size_bytes = ?DEFAULT_MAX_SEGMENT_SIZE_B :: non_neg_integer(),
         max_segment_size_chunks = ?DEFAULT_MAX_SEGMENT_SIZE_C :: non_neg_integer(),
         tracking_config = #{} :: osiris_tracking:config(),
         retention = [] :: [osiris:retention_spec()],
         counter :: counters:counters_ref(),
         counter_id :: term(),
         %% the maximum number of active writer deduplication sessions
         %% that will be included in snapshots written to new segments
         readers_counter_fun = fun(_) -> ok end :: function(),
         shared :: atomics:atomics_ref(),
         filter_size = ?DEFAULT_FILTER_SIZE :: osiris_bloom:filter_size()
         }).
-record(read,
        {type :: data | offset,
         next_offset = 0 :: osiris:offset(),
         transport :: osiris:transport(),
         chunk_selector :: all | user_data,
         position = 0 :: non_neg_integer(),
         filter :: undefined | osiris_bloom:mstate()}).
-record(write,
        {type = writer :: writer | acceptor,
         segment_size = {?LOG_HEADER_SIZE, 0} :: {non_neg_integer(), non_neg_integer()},
         current_epoch :: non_neg_integer(),
         tail_info = {0, empty} :: osiris:tail_info()
        }).
-define(MODULE_TODO, osiris_log).
-record(?MODULE_TODO,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename_all(),
         index_fd :: undefined | file:io_device(),
         fd :: undefined | file:io_device()
        }).

init_data_reader({StartChunkId, PrevEOT}, #{dir := Dir,
                                            name := Name} = Config) ->
    IdxFiles = sorted_index_files(Dir),
    Range = offset_range_from_idx_files(IdxFiles),
    ?DEBUG_(Name, " at ~b prev ~w local range: ~w",
           [StartChunkId, PrevEOT, Range]),
    %% Invariant:  there is always at least one segment left on disk
    case Range of
        {FstOffs, LastOffs}
          when StartChunkId < FstOffs
               orelse StartChunkId > LastOffs + 1 ->
            {error, {offset_out_of_range, Range}};
        empty when StartChunkId > 0 ->
            {error, {offset_out_of_range, Range}};
        _ when PrevEOT == empty ->
            %% this assumes the offset is in range
            %% first we need to validate PrevEO
            init_data_reader_from(StartChunkId,
                                  find_segment_for_offset(StartChunkId,
                                                          IdxFiles),
                                  Config);
        _ ->
            {PrevEpoch, PrevChunkId, _PrevTs} = PrevEOT,
            case check_chunk_has_expected_epoch(Name, PrevChunkId,
                                                PrevEpoch, IdxFiles) of
                ok ->
                    init_data_reader_from(StartChunkId,
                                          find_segment_for_offset(StartChunkId,
                                                                  IdxFiles),
                                          Config);
                {error, _} = Err ->
                    Err
            end
    end.

check_chunk_has_expected_epoch(Name, ChunkId, Epoch, IdxFiles) ->
    case find_segment_for_offset(ChunkId, IdxFiles) of
        {not_found, _} ->
            %% this is unexpected and thus an error
            {error,
             {invalid_last_offset_epoch, Epoch, unknown}};
        {found, #seg_info{} = SegmentInfo} ->
            %% prev segment exists, does it have the correct
            %% epoch?
            case offset_idx_scan(Name, ChunkId, SegmentInfo) of
                {ChunkId, Epoch, _PrevPos} ->
                    ok;
                {ChunkId, OtherEpoch, _} ->
                    {error,
                     {invalid_last_offset_epoch, Epoch, OtherEpoch}}
            end
    end.


init_data_reader_at(ChunkId, FilePos, File,
                    #{dir := Dir, name := Name,
                      shared := Shared,
                      readers_counter_fun := CountersFun} = Config) ->
    case file:open(File, [raw, binary, read]) of
        {ok, Fd} ->
            Cnt = osiris_log:make_counter(Config),
            counters:put(Cnt, ?C_OFFSET, ChunkId - 1),
            CountersFun(1),
            {ok,
             #?MODULE_TODO{cfg =
                      #cfg{directory = Dir,
                           counter = Cnt,
                           counter_id = osiris_log:counter_id(Config),
                           name = Name,
                           readers_counter_fun = CountersFun,
                           shared = Shared
                          },
                      mode =
                      #read{type = data,
                            next_offset = ChunkId,
                            chunk_selector = all,
                            position = FilePos,
                            transport = maps:get(transport, Config, tcp)},
                      fd = Fd}};
        Err ->
            Err
    end.

init_data_reader_from(ChunkId,
                      {end_of_log, #seg_info{file = File,
                                             last = LastChunk}},
                      Config) ->
    {ChunkId, AttachPos} = next_location(LastChunk),
    init_data_reader_at(ChunkId, AttachPos, File, Config);
init_data_reader_from(ChunkId,
                      {found, #seg_info{file = File} = SegInfo},
                      Config) ->
    Name = maps:get(name, Config, <<>>),
    {ChunkId, _Epoch, FilePos} = offset_idx_scan(Name, ChunkId, SegInfo),
    init_data_reader_at(ChunkId, FilePos, File, Config).

init_offset_reader(_OffsetSpec, _Conf, 0) ->
    {error, retries_exhausted};
init_offset_reader(OffsetSpec, Conf, Attempt) ->

    try
        init_offset_reader0(OffsetSpec, Conf)
    catch
        missing_file ->
            %% Retention policies are likely being applied, let's try again
            %% TODO: should we limit the number of retries?
            %% Remove cached index_files from config
            init_offset_reader(OffsetSpec,
                               maps:remove(index_files, Conf), Attempt - 1);
        {retry_with, NewOffsSpec, NewConf} ->
            init_offset_reader(NewOffsSpec, NewConf, Attempt - 1)
    end.

init_offset_reader0({abs, Offs}, #{dir := Dir} = Conf) ->
    case sorted_index_files(Dir) of
        [] ->
            {error, no_index_file};
        IdxFiles ->
            Range = offset_range_from_idx_files(IdxFiles),
            case Range of
                empty ->
                    {error, {offset_out_of_range, Range}};
                {S, E} when Offs < S orelse Offs > E ->
                    {error, {offset_out_of_range, Range}};
                _ ->
                    %% it is in range, convert to standard offset
                    init_offset_reader0(Offs, Conf)
            end
    end;
init_offset_reader0({timestamp, Ts}, #{} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            init_offset_reader0(next, Conf);
        IdxFilesRev ->
            case timestamp_idx_file_search(Ts, IdxFilesRev) of
                {scan, IdxFile} ->
                    %% segment was found, now we need to scan index to
                    %% find nearest offset
                    {ChunkId, FilePos} = chunk_location_for_timestamp(IdxFile, Ts),
                    SegmentFile = osiris_log:segment_from_index_file(IdxFile),
                    open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
                {first_in, IdxFile} ->
                    {ok, Fd} = file:open(IdxFile, [raw, binary, read]),
                    {ok, ?IDX_MATCH(ChunkId, _, FilePos)} = first_idx_record(Fd),
                    SegmentFile = osiris_log:segment_from_index_file(IdxFile),
                    open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
                next ->
                    %% segment was not found, attach next
                    %% this should be rare so no need to call the more optimal
                    %% open_offset_reader_at/4 function
                    init_offset_reader0(next, Conf)
            end
    end;
init_offset_reader0(first, #{} = Conf) ->
    case sorted_index_files(Conf) of
        [] ->
            {error, no_index_file};
        [FstIdxFile | _ ] ->
            case build_seg_info(FstIdxFile) of
                {ok, #seg_info{file = File,
                               first = undefined}} ->
                    %% empty log, attach at 0
                    open_offset_reader_at(File, 0, ?LOG_HEADER_SIZE, Conf);
                {ok, #seg_info{file = File,
                               first = #chunk_info{id = FirstChunkId,
                                                   pos = FilePos}}} ->
                    open_offset_reader_at(File, FirstChunkId, FilePos, Conf);
                {error, _} = Err ->
                    exit(Err)
            end
    end;
init_offset_reader0(next, #{} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            {error, no_index_file};
        [LastIdxFile | _ ] ->
            case build_seg_info(LastIdxFile) of
                {ok, #seg_info{file = File,
                               last = LastChunk}} ->
                    {NextChunkId, FilePos} = next_location(LastChunk),
                    open_offset_reader_at(File, NextChunkId, FilePos, Conf);
                Err ->
                    exit(Err)
            end
    end;
init_offset_reader0(last, #{name := Name} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            {error, no_index_file};
        IdxFiles ->
            case last_user_chunk_location(Name, IdxFiles) of
                not_found ->
                    ?DEBUG_(Name, "offset spec: 'last', user chunk not found, fall back to next", []),
                    %% no user chunks in stream, this is awkward, fall back to next
                    init_offset_reader0(next, Conf);
                {ChunkId, FilePos, IdxFile} ->
                    File = osiris_log:segment_from_index_file(IdxFile),
                    open_offset_reader_at(File, ChunkId, FilePos, Conf)
            end
    end;
init_offset_reader0(OffsetSpec, #{} = Conf)
  when is_integer(OffsetSpec) ->
    Name = maps:get(name, Conf, <<>>),
    case sorted_index_files(Conf) of
        [] ->
            {error, no_index_file};
        IdxFiles ->
            {ok, Range} = chunk_id_range_from_idx_files(IdxFiles),
            ?DEBUG_(Name, " spec ~w chunk_id range ~w Num index files ~b ",
                   [OffsetSpec, Range, length(IdxFiles)]),

            %% clamp start offset
            StartOffset = case {OffsetSpec, Range} of
                              {_, empty} ->
                                  0;
                              {Offset, {FirstChId, _LastChId}} ->
                                  max(FirstChId, Offset)
                          end,

            case find_segment_for_offset(StartOffset, IdxFiles) of
                {not_found, high} ->
                    throw({retry_with, next, Conf});
                {end_of_log, #seg_info{file = SegmentFile,
                                       last = LastChunk}} ->
                    {ChunkId, FilePos} = next_location(LastChunk),
                    open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
                {found, #seg_info{file = SegmentFile} = SegmentInfo} ->
                    {ChunkId, _Epoch, FilePos} =
                        case offset_idx_scan(Name, StartOffset, SegmentInfo) of
                            eof ->
                                exit(offset_out_of_range);
                            enoent ->
                                %% index file was not found
                                %% throw should be caught and trigger a retry
                                throw(missing_file);
                            offset_out_of_range ->
                                exit(offset_out_of_range);
                            IdxResult when is_tuple(IdxResult) ->
                                IdxResult
                        end,
                    ?DEBUG_(Name, "resolved chunk_id ~b"
                            " at file pos: ~w ", [ChunkId, FilePos]),
                    open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf)
            end
    end.

open_offset_reader_at(SegmentFile, NextChunkId, FilePos,
                      #{dir := Dir,
                        name := Name,
                        shared := Shared,
                        readers_counter_fun := ReaderCounterFun,
                        options := Options} =
                      Conf) ->
    {ok, Fd} = osiris_log:open(SegmentFile, [raw, binary, read]),
    Cnt = osiris_log:make_counter(Conf),
    ReaderCounterFun(1),
    FilterMatcher = case Options of
                        #{filter_spec := FilterSpec} ->
                            osiris_bloom:init_matcher(FilterSpec);
                        _ ->
                            undefined
                    end,
    %% TODO this returns a osiris_log module config. Can it just return the data instead?
    %% Can read and (perhaps write) just be an opaque?
    {ok, #?MODULE_TODO{cfg = #cfg{directory = Dir,
                             counter = Cnt,
                             counter_id = osiris_log:counter_id(Conf),
                             name = Name,
                             readers_counter_fun = ReaderCounterFun,
                             shared = Shared
                            },
                  mode = #read{type = offset,
                               position = FilePos,
                               chunk_selector = maps:get(chunk_selector, Options,
                                                         user_data),
                               next_offset = NextChunkId,
                               transport = maps:get(transport, Options, tcp),
                               filter = FilterMatcher},
                  fd = Fd}}.

%% Searches the index files backwards for the ID of the last user chunk.
last_user_chunk_location(Name, RevdIdxFiles)
  when is_list(RevdIdxFiles) ->
    {Time, Result} = timer:tc(
                       fun() ->
                               last_user_chunk_id0(Name, RevdIdxFiles)
                       end),
    ?DEBUG_(Name, " completed in ~fms", [Time / 1000]),
    Result.

last_user_chunk_id0(_, []) ->
    %% There are no user chunks in any index files.
    not_found;
last_user_chunk_id0(Name, [IdxFile | Rest]) ->
    %% Do not read-ahead since we read the index file backwards chunk by chunk.
    {ok, IdxFd} = osiris_log:open(IdxFile, [read, raw, binary]),
    {ok, EofPos} = position_at_idx_record_boundary(IdxFd, eof),
    Last = last_user_chunk_id_in_index(EofPos - ?INDEX_RECORD_SIZE_B, IdxFd),
    _ = file:close(IdxFd),
    case Last of
        {ok, Id, Pos} ->
            {Id, Pos, IdxFile};
        {error, Reason} ->
            ?DEBUG_(Name, "Could not find user chunk in index file ~ts (~0p)",
                   [IdxFile, Reason]),
            last_user_chunk_id0(Name, Rest)
    end.

%% Searches the index file backwards for the chunk id of the last user chunk.
last_user_chunk_id_in_index(NextPos, IdxFd) ->
    case file:pread(IdxFd, NextPos, ?INDEX_RECORD_SIZE_B) of
        {ok, <<ChunkId:64/unsigned,
               _Timestamp:64/signed,
               _Epoch:64/unsigned,
               FilePos:32/unsigned,
               ?CHNK_USER:8/unsigned>>} ->
            {ok, ChunkId, FilePos};
        {ok, ?IDX_MATCH(_, _, _)} ->
            last_user_chunk_id_in_index(NextPos - ?INDEX_RECORD_SIZE_B, IdxFd);
        {error, _} = Error ->
            Error
    end.

sorted_index_files_rev(#{index_files := IdxFiles}) ->
    %% cached
    lists:reverse(IdxFiles);
sorted_index_files_rev(#{dir := Dir}) ->
    sorted_index_files_rev(Dir);
sorted_index_files_rev(Dir) ->
    index_files(Dir, fun (Files) ->
                             lists:sort(fun erlang:'>'/2, Files)
                     end).

chunk_id_range_from_idx_files(Files) ->
    case non_empty_index_files(Files) of
        [] ->
            {ok, empty};
        [IdxFile] ->
            chunk_id_range_from_idx_files(IdxFile, IdxFile);
        [F | Rem] ->
            L = lists:last(Rem),
            chunk_id_range_from_idx_files(F, L)
    end.

chunk_id_range_from_idx_files(FstIdxFile, LstIdxFile) ->
    {ok, LstFd} = osiris_log:open(LstIdxFile, [read, raw, binary]),
    case position_at_idx_record_boundary(LstFd, eof) of
        {ok, Pos} ->
            case file:pread(LstFd, Pos - ?INDEX_RECORD_SIZE_B,
                            ?INDEX_RECORD_SIZE_B) of
                {ok, ?IDX_MATCH(LstChId, _, _)} ->
                    ok = file:close(LstFd),
                    {ok, FstFd} = osiris_log:open(FstIdxFile, [read, raw, binary]),
                    case file:pread(FstFd, ?IDX_HEADER_SIZE,
                                    ?INDEX_RECORD_SIZE_B) of
                        {ok, ?IDX_MATCH(FstChId, _, _)} ->
                            ok = file:close(FstFd),
                            {ok, {FstChId, LstChId}};
                        Err ->
                            ok = file:close(LstFd),
                            Err
                    end;
                Err ->
                    ok = file:close(LstFd),
                    Err
            end
    end.

chunk_location_for_timestamp(Idx, Ts) ->
    %% TODO: optimise using skip search approach
    Fd = open_index_read(Idx),
    %% scan index file for nearest timestamp
    {ChunkId, _Timestamp, _Epoch, FilePos} = timestamp_idx_scan(Fd, Ts),
    {ChunkId, FilePos}.

non_empty_index_files([]) ->
    [];
non_empty_index_files(IdxFiles) ->
    LastIdxFile = lists:last(IdxFiles),
    case file_size(LastIdxFile) of
        N when N =< ?IDX_HEADER_SIZE ->
            non_empty_index_files(IdxFiles -- [LastIdxFile]);
        _ ->
            IdxFiles
    end.

timestamp_idx_scan(Fd, Ts) ->
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok,
         <<ChunkId:64/unsigned,
           Timestamp:64/signed,
           Epoch:64/unsigned,
           FilePos:32/unsigned,
           _ChType:8/unsigned>>} ->
            case Ts =< Timestamp of
                true ->
                    ok = file:close(Fd),
                    {ChunkId, Timestamp, Epoch, FilePos};
                false ->
                    timestamp_idx_scan(Fd, Ts)
            end;
        eof ->
            ok = file:close(Fd),
            eof
    end.

first_last_timestamps(IdxFile) ->
    case file:open(IdxFile, [raw, read, binary]) of
        {ok, Fd} ->
	    _ = file:advise(Fd, 0, 0, random),
            case first_idx_record(Fd) of
                {ok, <<_:64/unsigned,
                       FirstTs:64/signed,
                       _:64/unsigned,
                       _:32/unsigned,
                       _:8/unsigned>>} ->
                    %% if we can get the first we can get the last
                    {ok, <<_:64/unsigned,
                           LastTs:64/signed,
                           _:64/unsigned,
                           _:32/unsigned,
                           _:8/unsigned>>} = last_idx_record(Fd),
                    ok = file:close(Fd),
                    {FirstTs, LastTs};
                {error, einval} ->
                    %% empty index
                    undefined;
                eof ->
                    eof
            end;
        _Err ->
            file_not_found
    end.

%% accepts a list of index files in reverse order
%% [{21, 30}, {12, 20}, {5, 10}]
%% 11 = {12, 20}
timestamp_idx_file_search(Ts, [FstIdxFile | Older]) ->
    case first_last_timestamps(FstIdxFile) of
        {_FstTs, EndTs}
          when Ts > EndTs ->
            %% timestamp greater than the newest timestamp in the stream
            %% attach at 'next'
            next;
        {FstTs, _EndTs}
          when Ts < FstTs ->
            %% the requested timestamp is older than the first timestamp in
            %% this segment, keep scanning
            timestamp_idx_file_search0(Ts, Older, FstIdxFile);
        {_, _} ->
            %% else we must have found it!
            {scan, FstIdxFile};
        file_not_found ->
            %% the requested timestamp is older than the first timestamp in
            %% this segment, keep scanning
            timestamp_idx_file_search0(Ts, Older, FstIdxFile);
        eof ->
            %% empty log
            next
    end.

timestamp_idx_file_search0(Ts, [], IdxFile) ->
    case first_last_timestamps(IdxFile) of
        {FstTs, _LastTs}
          when Ts < FstTs ->
            {first_in, IdxFile};
        _ ->
            {found, IdxFile}
    end;
timestamp_idx_file_search0(Ts, [IdxFile | Older], Prev) ->
    case first_last_timestamps(IdxFile) of
        {_FstTs, EndTs}
          when Ts > EndTs ->
            %% we should attach the the first chunk in the previous segment
            %% as the requested timestamp must fall in between the
            %% current and previous segments
            {first_in, Prev};
        {FstTs, _EndTs}
          when Ts < FstTs ->
            %% the requested timestamp is older than the first timestamp in
            %% this segment, keep scanning
            timestamp_idx_file_search0(Ts, Older, IdxFile);
        _ ->
            %% else we must have found it!
            {scan, IdxFile}
    end.

%% TODO where how should this fun be used? Should the logic be moved to just get what they want from the index files, instead of the actual index files?
sorted_index_files(#{index_files := IdxFiles}) ->
    %% cached
    IdxFiles;
sorted_index_files(#{dir := Dir}) ->
    sorted_index_files(Dir);
sorted_index_files(Dir) when ?IS_STRING(Dir) ->
    index_files(Dir, fun lists:sort/1).

index_files_unsorted(Dir) ->
    index_files(Dir, fun (X) -> X end).

index_files(Dir, SortFun) ->
    [filename:join(Dir, F)
     || <<_:20/binary, ".index">> = F <- SortFun(list_dir(Dir))].

offset_range_from_idx_files([]) ->
    empty;
offset_range_from_idx_files([IdxFile]) ->
    %% there is an invariant that if there is a single index file
    %% there should also be a segment file
    {ok, #seg_info{first = First,
                   last = Last}} = build_seg_info(IdxFile),
    offset_range_from_chunk_range({First, Last});
offset_range_from_idx_files(IdxFiles) when is_list(IdxFiles) ->
    NonEmptyIdxFiles = non_empty_index_files(IdxFiles),
    {_, FstSI, LstSI} = first_and_last_seginfos0(NonEmptyIdxFiles),
    ChunkRange = chunk_range_from_segment_infos([FstSI, LstSI]),
    offset_range_from_chunk_range(ChunkRange).

first_idx_record(IdxFd) ->
    idx_read_at(IdxFd, ?IDX_HEADER_SIZE).

idx_read_at(Fd, Pos) when is_integer(Pos) ->
    case file:pread(Fd, Pos, ?INDEX_RECORD_SIZE_B) of
        {ok, ?ZERO_IDX_MATCH(_)} ->
            {error, empty_idx_record};
        Ret ->
            Ret
    end.

build_seg_info(IdxFile) ->
    case last_valid_idx_record(IdxFile) of
        {ok, ?IDX_MATCH(_, _, LastChunkPos)} ->
            SegFile = osiris_log:segment_from_index_file(IdxFile),
            build_segment_info(SegFile, LastChunkPos, IdxFile);
        undefined ->
            %% this would happen if the file only contained a header
            SegFile = osiris_log:segment_from_index_file(IdxFile),
            {ok, #seg_info{file = SegFile, index = IdxFile}};
        {error, _} = Err ->
            Err
    end.

build_segment_info(SegFile, LastChunkPos, IdxFile) ->
    {ok, Fd} = osiris_log:open(SegFile, [read, binary, raw]),
    %% we don't want to read blocks into page cache we are unlikely to need
    _ = file:advise(Fd, 0, 0, random),
    case file:pread(Fd, ?LOG_HEADER_SIZE, ?HEADER_SIZE_B) of
        eof ->
            _ = file:close(Fd),
            eof;
        {ok,
         <<?MAGIC:4/unsigned,
           ?VERSION:4/unsigned,
           FirstChType:8/unsigned,
           _NumEntries:16/unsigned,
           FirstNumRecords:32/unsigned,
           FirstTs:64/signed,
           FirstEpoch:64/unsigned,
           FirstChId:64/unsigned,
           _FirstCrc:32/integer,
           FirstSize:32/unsigned,
           FirstFSize:8/unsigned,
           FirstTSize:24/unsigned,
           _/binary>>} ->
            case file:pread(Fd, LastChunkPos, ?HEADER_SIZE_B) of
                {ok,
                 <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   LastChType:8/unsigned,
                   _LastNumEntries:16/unsigned,
                   LastNumRecords:32/unsigned,
                   LastTs:64/signed,
                   LastEpoch:64/unsigned,
                   LastChId:64/unsigned,
                   _LastCrc:32/integer,
                   LastSize:32/unsigned,
                   LastTSize:32/unsigned,
                   LastFSize:8/unsigned,
                   _Reserved:24>>} ->
                    LastChunkSize = LastFSize + LastSize + LastTSize,
                    Size = LastChunkPos + ?HEADER_SIZE_B + LastChunkSize,
                    %% TODO: this file:position/2 all has no actual function and
                    %% is only used to emit a debug log. Remove?
                    {ok, Eof} = file:position(Fd, eof),
                    ?DEBUG_IF("~s: segment ~ts has trailing data ~w ~w",
                              [?MODULE, filename:basename(SegFile),
                               Size, Eof], Size =/= Eof),
                    _ = file:close(Fd),
                    FstChInfo = #chunk_info{epoch = FirstEpoch,
                                            timestamp = FirstTs,
                                            id = FirstChId,
                                            num = FirstNumRecords,
                                            type = FirstChType,
                                            size = FirstFSize + FirstSize + FirstTSize,
                                            pos = ?LOG_HEADER_SIZE},
                    LastChInfo = #chunk_info{epoch = LastEpoch,
                                             timestamp = LastTs,
                                             id = LastChId,
                                             num = LastNumRecords,
                                             type = LastChType,
                                             size = LastChunkSize,
                                             pos = LastChunkPos},
                    {ok, #seg_info{file = SegFile,
                                   index = IdxFile,
                                   size = Size,
                                   first = FstChInfo,
                                   last = LastChInfo}};
                _ ->
                    % last chunk is corrupted - try the previous one
                    _ = file:close(Fd),
                    {ok, ?IDX_MATCH(_ChId, _E, PrevChPos)} =
                        nth_last_idx_record(IdxFile, 2),
                    case PrevChPos == LastChunkPos of
                        false ->
                            build_segment_info(SegFile, PrevChPos , IdxFile);
                        true ->
                            % avoid an infinite loop if multiple chunks are corrupted
                            ?ERROR("Multiple corrupted chunks in segment file ~0p",
                                   [SegFile]),
                            exit({corrupted_segment, {segment_file, SegFile}})
                    end
            end
    end.

find_segment_for_offset(Offset, IdxFiles) ->
    %% we assume index files are in the default low-> high order here
    case lists:search(
           fun(IdxFile) ->
                   Offset >= index_file_first_offset(IdxFile)
           end, lists:reverse(IdxFiles)) of
        {value, File} ->
            case build_seg_info(File) of
                {ok, #seg_info{first = undefined,
                               last = undefined} = Info} ->
                    {end_of_log, Info};
                {ok, #seg_info{last =
                               #chunk_info{id = LastChId,
                                           num = LastNumRecs}} = Info}
                  when Offset == LastChId + LastNumRecs ->
                    %% the last segment and offset is the next offset
                    {end_of_log, Info};
                {ok, #seg_info{first = #chunk_info{id = FirstChId},
                               last = #chunk_info{id = LastChId,
                                                  num = LastNumRecs}} = Info} ->
                    NextChId = LastChId + LastNumRecs,
                    case Offset >= FirstChId andalso Offset < NextChId of
                        true ->
                            %% we found it
                            {found, Info};
                        false when Offset >= NextChId ->
                            {not_found, high};
                        false ->
                            {not_found, low}
                    end;
                {error, _} = Err ->
                    Err
            end;
        false ->
            {not_found, low}
    end.

offset_idx_scan(Name, Offset, #seg_info{index = IndexFile} = SegmentInfo) ->
    T1 = erlang:monotonic_time(),
    Result = case offset_range_from_segment_infos([SegmentInfo]) of
                 empty ->
                     eof;
                 {SegmentStartOffs, SegmentEndOffs} ->
                     case Offset < SegmentStartOffs orelse
                          Offset > SegmentEndOffs of
                         true ->
                             offset_out_of_range;
                         false ->
                             {ok, IdxFd} = osiris_log:open(IndexFile,
                                                [read, raw, binary]),
                             _ = file:advise(IdxFd, 0, 0, random),
                             {Offset, SearchResult} =
                                 idx_skip_search(IdxFd, ?IDX_HEADER_SIZE,
                                                 fun offset_search_fun/3,
                                                 {Offset, not_found}),
                             ok = file:close(IdxFd),
                             SearchResult
                     end
             end,
    T2 = erlang:monotonic_time(),
    Time = erlang:convert_time_unit(T2 - T1, native, microsecond),

    ?DEBUG_(Name, " completed in ~fms",
           [Time/1000]),
    Result.

idx_skip_search(Fd, ?IDX_HEADER_SIZE = Pos0, Fun, Acc0) ->
    %% avoid skipping on very first record
    case idx_read_at(Fd, Pos0) of
        {ok, IdxRecordBin} ->
            case Fun(scan, IdxRecordBin, Acc0) of
                {continue, Acc} ->
                    Pos = Pos0 + ?INDEX_RECORD_SIZE_B,
                    idx_skip_search(Fd, Pos, Fun, Acc);
                {return, Acc} ->
                    Acc
            end;
        _ ->
            Acc0
    end;
idx_skip_search(Fd, Pos, Fun, Acc0) ->
    SkipSize = ?SKIP_SEARCH_JUMP * ?INDEX_RECORD_SIZE_B,
    PeekPos = Pos + SkipSize,
    case idx_read_at(Fd, PeekPos) of
        {ok, IdxRecordBin} ->
            case Fun(peek, IdxRecordBin, Acc0) of
                {continue, Acc} ->
                    idx_skip_search(Fd, PeekPos + ?INDEX_RECORD_SIZE_B, Fun, Acc);
                {return, Acc} ->
                    Acc;
                {scan, Acc1} ->
                    {ok, Data} = file:pread(Fd, Pos, SkipSize + ?INDEX_RECORD_SIZE_B),
                    case idx_lin_scan(Data, Fun, Acc1) of
                        {continue, Acc} ->
                            idx_skip_search(Fd, PeekPos + ?INDEX_RECORD_SIZE_B,
                                            Fun, Acc);
                        {return, Acc} ->
                            Acc
                    end
            end;
        _ ->
            %% eof or invalid index record
            case file:pread(Fd, Pos, SkipSize + ?INDEX_RECORD_SIZE_B) of
                {ok, Data} ->
                    case idx_lin_scan(Data, Fun, Acc0) of
                        {continue, Acc} ->
                            %% eof so can't continue
                            Acc;
                        {return, Acc} ->
                            Acc
                    end;
                eof ->
                    Acc0
            end
    end.


idx_lin_scan(<<>>, _Fun, Acc) ->
    {continue, Acc};
idx_lin_scan(?ZERO_IDX_MATCH(_), _Fun, Acc0) ->
    {continue, Acc0};
idx_lin_scan(<<IdxRecordBin:?INDEX_RECORD_SIZE_B/binary, Rem/binary>>, Fun, Acc0) ->
    case Fun(scan, IdxRecordBin, Acc0) of
        {continue, Acc} ->
            idx_lin_scan(Rem, Fun, Acc);
        {return, _} = Ret ->
            Ret
    end.

offset_search_fun(scan, ?IDX_MATCH(ChId, _Epoch, _Pos), {Offset, _} = State)
  when Offset < ChId ->
    {return, State};
offset_search_fun(peek, ?IDX_MATCH(ChId, _Epoch, _Pos), {Offset, _} = State)
  when Offset < ChId ->
    {scan, State};
offset_search_fun(_Type, ?IDX_MATCH(ChId, Epoch, Pos), {Offset, _}) ->
    {continue, {Offset, {ChId, Epoch, Pos}}}.

%% Some file:position/2 operations are subject to race conditions. In particular, `eof` may position the Fd
%% in the middle of a record being written concurrently. If that happens, we need to re-position at the nearest
%% record boundry. See https://github.com/rabbitmq/osiris/issues/73
position_at_idx_record_boundary(IdxFd, At) ->
    case file:position(IdxFd, At) of
        {ok, Pos} ->
            case (Pos - ?IDX_HEADER_SIZE) rem ?INDEX_RECORD_SIZE_B of
                0 -> {ok, Pos};
                N -> file:position(IdxFd, {cur, -N})
            end;
        Error -> Error
    end.

open_index_read(File) ->
    {ok, Fd} = osiris_log:open(File, [read, raw, binary, read_ahead]),
    %% We can't use the assertion that index header is correct because of a
    %% race condition between opening the file and writing the header
    %% It seems to happen when retention policies are applied
    {ok, ?IDX_HEADER_SIZE} = file:position(Fd, ?IDX_HEADER_SIZE),
    Fd.

file_size(Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        {error, enoent} ->
            throw(missing_file)
    end.

last_idx_record(IdxFd) ->
    nth_last_idx_record(IdxFd, 1).

nth_last_idx_record(IdxFile, N) when ?IS_STRING(IdxFile) ->
    {ok, IdxFd} = osiris_log:open(IdxFile, [read, raw, binary]),
    IdxRecord = nth_last_idx_record(IdxFd, N),
    _ = file:close(IdxFd),
    IdxRecord;
nth_last_idx_record(IdxFd, N) ->
    case position_at_idx_record_boundary(IdxFd, {eof, -?INDEX_RECORD_SIZE_B * N}) of
        {ok, _} ->
            file:read(IdxFd, ?INDEX_RECORD_SIZE_B);
        Err ->
            Err
    end.

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
    end.

offset_range_from_chunk_range(empty) ->
    empty;
offset_range_from_chunk_range({undefined, undefined}) ->
    empty;
offset_range_from_chunk_range({#chunk_info{id = FirstChId},
                               #chunk_info{id = LastChId,
                                           num = LastNumRecs}}) ->
    {FirstChId, LastChId + LastNumRecs - 1}.

first_and_last_seginfos(#{index_files := IdxFiles}) ->
    first_and_last_seginfos0(IdxFiles);
first_and_last_seginfos(#{dir := Dir}) ->
    first_and_last_seginfos0(sorted_index_files(Dir)).

first_and_last_seginfos0([]) ->
    none;
first_and_last_seginfos0([FstIdxFile]) ->
    {ok, SegInfo} = build_seg_info(FstIdxFile),
    {1, SegInfo, SegInfo};
first_and_last_seginfos0([FstIdxFile | Rem] = IdxFiles) ->
    %% this function is only used by init
    case build_seg_info(FstIdxFile) of
        {ok, FstSegInfo} ->
            LastIdxFile = lists:last(Rem),
            case build_seg_info(LastIdxFile) of
                {ok, #seg_info{first = undefined,
                               last = undefined}} ->
                    %% the last index file doesn't have any index records yet
                    %% retry without it
                    [_ | RetryIndexFiles] = lists:reverse(IdxFiles),
                    first_and_last_seginfos0(lists:reverse(RetryIndexFiles));
                {ok, LastSegInfo} ->
                    {length(Rem) + 1, FstSegInfo, LastSegInfo};
                {error, Err} ->
                    ?ERROR("~s: failed to build seg_info from file ~ts, error: ~w",
                           [?MODULE, LastIdxFile, Err]),
                    error(Err)
            end;
        {error, enoent} ->
            %% most likely retention race condition
            first_and_last_seginfos0(Rem)
    end.

offset_range_from_segment_infos(SegInfos) ->
    ChunkRange = chunk_range_from_segment_infos(SegInfos),
    offset_range_from_chunk_range(ChunkRange).

chunk_range_from_segment_infos([#seg_info{first = undefined,
                                          last = undefined}]) ->
    empty;
chunk_range_from_segment_infos(SegInfos) when is_list(SegInfos) ->
    #seg_info{first = First} = hd(SegInfos),
    #seg_info{last = Last} = lists:last(SegInfos),
    {First, Last}.

last_valid_idx_record(IdxFile) ->
    {ok, IdxFd} = osiris_log:open(IdxFile, [read, raw, binary]),
    case position_at_idx_record_boundary(IdxFd, eof) of
        {ok, Pos} ->
            SegFile = osiris_log:segment_from_index_file(IdxFile),
            SegSize = file_size(SegFile),
            ok = skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos),
            case file:position(IdxFd, {cur, -?INDEX_RECORD_SIZE_B}) of
                {ok, _} ->
                    IdxRecord = file:read(IdxFd, ?INDEX_RECORD_SIZE_B),
                    _ = file:close(IdxFd),
                    IdxRecord;
                _ ->
                    _ = file:close(IdxFd),
                    undefined
            end;
        Err ->
            _ = file:close(IdxFd),
            Err
    end.

skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos) ->
    case Pos >= ?IDX_HEADER_SIZE + ?INDEX_RECORD_SIZE_B of
        true ->
            {ok, _} = file:position(IdxFd, Pos - ?INDEX_RECORD_SIZE_B),
            case file:read(IdxFd, ?INDEX_RECORD_SIZE_B) of
                {ok, ?ZERO_IDX_MATCH(_)} ->
                    % trailing zeros found
                    skip_invalid_idx_records(IdxFd, SegFile, SegSize,
                                             Pos - ?INDEX_RECORD_SIZE_B);
                {ok, ?IDX_MATCH(_, _, ChunkPos)} ->
                    % a non-zero index record
                    case ChunkPos < SegSize andalso
                         is_valid_chunk_on_disk(SegFile, ChunkPos) of
                        true ->
                            ok;
                        false ->
                            % this chunk doesn't exist in the segment or is invalid
                            skip_invalid_idx_records(IdxFd, SegFile, SegSize,
                                                     Pos - ?INDEX_RECORD_SIZE_B)
                    end;
                Err ->
                    Err
            end;
        false ->
            %% TODO should we validate the correctness of index/segment headers?
            {ok, _} = file:position(IdxFd, ?IDX_HEADER_SIZE),
            ok
    end.

is_valid_chunk_on_disk(SegFile, Pos) ->
    %% read a chunk from a specified location in the segment
    %% then checks the CRC
    case osiris_log:open(SegFile, [read, raw, binary]) of
        {ok, SegFd} ->
            IsValid = case file:pread(SegFd, Pos, ?HEADER_SIZE_B) of
                          {ok,
                           <<?MAGIC:4/unsigned,
                             ?VERSION:4/unsigned,
                             _ChType:8/unsigned,
                             _NumEntries:16/unsigned,
                             _NumRecords:32/unsigned,
                             _Timestamp:64/signed,
                             _Epoch:64/unsigned,
                             _NextChId0:64/unsigned,
                             Crc:32/integer,
                             DataSize:32/unsigned,
                             _TrailerSize:32/unsigned,
                             FilterSize:8/unsigned,
                             _Reserved:24>>} ->
                              DataPos = Pos + FilterSize + ?HEADER_SIZE_B,
                              case file:pread(SegFd, DataPos, DataSize) of
                                  {ok, Data} ->
                                      case erlang:crc32(Data) of
                                          Crc ->
                                              true;
                                          _ ->
                                              false
                                      end;
                                  eof ->
                                      false
                              end;
                          _ ->
                              false
                      end,
            _ = file:close(SegFd),
            IsValid;
       _Err ->
            false
    end.

index_file_first_offset(IdxFile) when is_list(IdxFile) ->
    list_to_integer(filename:basename(IdxFile, ".index"));
index_file_first_offset(IdxFile) when is_binary(IdxFile) ->
    binary_to_integer(filename:basename(IdxFile, <<".index">>)).

next_location(undefined) ->
    {0, ?LOG_HEADER_SIZE};
next_location(#chunk_info{id = Id,
                          num = Num,
                          pos = Pos,
                          size = Size}) ->
    {Id + Num, Pos + Size + ?HEADER_SIZE_B}.
