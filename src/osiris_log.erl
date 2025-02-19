%%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log).

-include("osiris.hrl").
-include("osiris_log.hrl").
-include_lib("kernel/include/file.hrl").

-export([init/1,
         init/2,
         init_acceptor/3,
         write/2,
         write/3,
         write/5,
         recover_tracking/1,
         accept_chunk/2,
         next_offset/1,
         can_read_next/1,
         first_offset/1,
         first_timestamp/1,
         tail_info/1,
         evaluate_tracking_snapshot/2,
         send_file/2,
         send_file/3,
         init_data_reader/2,
         init_offset_reader/2,
         read_header/1,
         chunk_iterator/1,
         chunk_iterator/2,
         iterator_next/1,
         read_chunk/1,
         read_chunk_parsed/1,
         read_chunk_parsed/2,
         committed_offset/1,
         committed_chunk_id/1,
         set_committed_chunk_id/2,
         last_chunk_id/1,
         get_current_epoch/1,
         get_directory/1,
         get_name/1,
         get_shared/1,
         get_default_max_segment_size_bytes/0,
         counters_ref/1,
         close/1,
         overview/1,
         format_status/1,
         update_retention/2,
         evaluate_retention/2,
         directory/1,
         delete_directory/1,
         make_counter/1]).

-export([segment_from_index_file/1,
         make_file_name/2,
         open/2,
         counter_id/1,
         validate_crc/3]).

-export([dump_init/1,
         dump_init_idx/1,
         dump_chunk/1,
         dump_index/1,
         dump_crc_check/1]).
%% for testing
-export([
         sorted_index_files/1,
         index_files_unsorted/1,
         make_chunk/7,
         orphaned_segments/1
        ]).

%% Specification of the Log format.
%%
%% Notes:
%%   * All integers are in Big Endian order.
%%   * Timestamps are expressed in milliseconds.
%%   * Timestamps are stored in signed 64-bit integers.
%%
%% Log format
%% ==========
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +---------------+---------------+---------------+---------------+
%%   | Log magic (0x4f53494c = "OSIL")                               |
%%   +---------------------------------------------------------------+
%%   | Log version (0x00000001)                                      |
%%   +---------------------------------------------------------------+
%%   | Contiguous list of Chunks                                     |
%%   : (until EOF)                                                   :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% Chunk format
%% ============
%%
%% A chunk is the unit of replication and read.
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +-------+-------+---------------+---------------+---------------+
%%   | Mag   | Ver   | Chunk type    | Number of entries             |
%%   +-------+-------+---------------+-------------------------------+
%%   | Number of records                                             |
%%   +---------------------------------------------------------------+
%%   | Timestamp                                                     |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | Epoch                                                         |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | ChunkId                                                       |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | Data CRC                                                      |
%%   +---------------------------------------------------------------+
%%   | Data length                                                   |
%%   +---------------------------------------------------------------+
%%   | Trailer length                                                |
%%   +---------------------------------------------------------------+
%%   | Bloom Size    | Reserved                                      |
%%   +---------------------------------------------------------------+
%%   | Bloom filter data                                             |
%%   : (<bloom size> bytes)                                          :
%%   +---------------------------------------------------------------+
%%   | Contiguous list of Data entries                               |
%%   : (<data length> bytes)                                         :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%   | Contiguous list of Trailer entries                            |
%%   : (<trailer length> bytes)                                      :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% Mag = 0x5
%%   Magic number to identify the beginning of a chunk
%%
%% Ver = 0x1
%%   Version of the chunk format
%%
%% Chunk type = CHNK_USER (0x00) |
%%              CHNK_TRK_DELTA (0x01) |
%%              CHNK_TRK_SNAPSHOT (0x02)
%%   Type which determines what to do with entries.
%%
%% Number of entries = unsigned 16-bit integer
%%   Number of entries in the data section following the chunk header.
%%
%% Number of records = unsigned 32-bit integer
%%
%% Timestamp = signed 64-bit integer
%%
%% Epoch = unsigned 64-bit integer
%%
%% ChunkId = unsigned 64-bit integer, the first offset in the chunk
%%
%% Data CRC = unsigned 32-bit integer
%%
%% Data length = unsigned 32-bit integer
%%
%% Trailer length = unsigned 32-bit integer
%%
%% Reserved = 4 bytes reserved for future extensions
%%
%% Data Entry format
%% =================
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +---------------+---------------+---------------+---------------+
%%   |T| Data entry header and body                                  |
%%   +-+ (format and length depends on the chunk and entry types)    :
%%   :                                                               :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% T = SimpleEntry (0) |
%%     SubBatchEntry (1)
%%
%% SimpleEntry (CHNK_USER)
%% -----------------------
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +---------------+---------------+---------------+---------------+
%%   |0| Entry body length                                           |
%%   +-+-------------------------------------------------------------+
%%   | Entry body                                                    :
%%   : (<body length> bytes)                                         :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% Entry body length = unsigned 31-bit integer
%%
%% Entry body = arbitrary data
%%
%% SimpleEntry (CHNK_TRK_SNAPSHOT)
%% -------------------------------------------------
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +-+-------------+---------------+---------------+---------------+
%%   |0| Entry body length                                           |
%%   +-+-------------------------------------------------------------+
%%   | Entry Body                                                    |
%%   : (<body length> bytes)                                         :
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% The entry body is made of a contiguous list of the following block, until
%% the body length is reached.
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +---------------+---------------+---------------+---------------+
%%   | Trk    type   | ID size       | ID                            |
%%   +---------------+---------------+                               |
%%   |                                                               |
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%   | Type specific data                                            |
%%   |                                                               |
%%   +---------------------------------------------------------------+
%%
%% ID type = unsigned 8-bit integer 0 = sequence, 1 = offset
%%
%% ID size = unsigned 8-bit integer
%%
%% ID = arbitrary data
%%
%% Type specific data:
%% When ID type = 0: ChunkId, Sequence = unsigned 64-bit integers
%% When ID type = 1: Offset = unsigned 64-bit integer
%%
%%
%% SubBatchEntry (CHNK_USER)
%% -------------------------
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +-+-----+-------+---------------+---------------+---------------+
%%   |1| Cmp | Rsvd  | Number of records             | Uncmp Length..|
%%   +-+-----+-------+-------------------------------+---------------+
%%   | Uncompressed Length                           | Length  (...) |
%%   +-+-----+-------+-------------------------------+---------------+
%%   | Length                                        | Body          |
%%   +-+---------------------------------------------+               +
%%   | Body                                                          |
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%
%% Cmp = unsigned 3-bit integer
%%   Compression type
%%
%% Rsvd = 0x0
%%   Reserved bits
%%
%% Number of records = unsigned 16-bit integer
%%
%% Uncompressed Length = unsigned 32-bit integer
%% Length = unsigned 32-bit integer
%%
%% Body = arbitrary data
%%
%% Tracking format
%% ==============
%%
%% Tracking is made of a contiguous list of the following block,
%% until the trailer length stated in the chunk header is reached.
%% The same format is used for the CHNK_TRK_DELTA chunk entry.
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +-+-------------+---------------+---------------+---------------+
%%   | Tracking type |TrackingID size| TrackingID                    |
%%   +---------------+---------------+                               |
%%   |                                                               |
%%   :                                                               :
%%   +---------------------------------------------------------------+
%%   | Tracking data                                                 |
%%   |                                                               |
%%   +---------------------------------------------------------------+
%%
%%
%% Tracking type: 0 = sequence, 1 = offset, 2 = timestamp
%%
%% Tracking type 0 = sequence to track producers for msg deduplication
%% use case: producer sends msg -> osiris persists msg -> confirm doens't reach producer -> producer resends same msg
%%
%% Tracking type 1 = offset to track consumers
%% use case: consumer or server restarts -> osiris knows offset where consumer needs to resume
%%
%% Tracking data: 64 bit integer
%%
%% Index format
%% ============
%%
%% Each index record in the index file maps to a chunk in the segment file.
%%
%% Index record
%% ------------
%%
%%   |0              |1              |2              |3              | Bytes
%%   |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7| Bits
%%   +---------------+---------------+---------------+---------------+
%%   | Offset                                                        |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | Timestamp                                                     |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | Epoch                                                         |
%%   | (8 bytes)                                                     |
%%   +---------------------------------------------------------------+
%%   | File Offset                                                   |
%%   +---------------+-----------------------------------------------+
%%   | Chunk Type    |
%%   +---------------+

-type offset() :: osiris:offset().
-type epoch() :: osiris:epoch().
-type range() :: empty | {From :: offset(), To :: offset()}.
-type counter_spec() :: {Tag :: term(), Fields :: [atom()]}.
-type config() ::
    osiris:config() |
    #{dir := file:filename_all(),
      epoch => non_neg_integer(),
      % first_offset_fun => fun((integer()) -> ok),
      shared => atomics:atomics_ref(),
      max_segment_size_bytes => non_neg_integer(),
      %% max number of writer ids to keep around
      tracking_config => osiris_tracking:config(),
      %% if the counter is created before init is passed here
      counter => counters:counters_ref(),
      %% spec for creating the counter
      counter_spec => counter_spec(),
      %% used when initialising a log from an offset other than 0
      initial_offset => osiris:offset(),
      %% a cached list of the index files for a given log
      %% avoids scanning disk for files multiple times if already know
      %% e.g. in init_acceptor
      index_files => [file:filename_all()],
      filter_size => osiris_bloom:filter_size()
     }.
-type record() :: {offset(), osiris:entry()}.
-type offset_entry() :: {offset(), osiris:entry()}.
-type offset_spec() :: osiris:offset_spec().
-type retention_spec() :: osiris:retention_spec().
-type chunk_iterator() :: osiris_log_read:chunk_iterator().
-type header_map() ::
    #{chunk_id => offset(),
      epoch => epoch(),
      type => chunk_type(),
      crc => integer(),
      num_records => non_neg_integer(),
      num_entries => non_neg_integer(),
      timestamp => osiris:timestamp(),
      data_size => non_neg_integer(),
      trailer_size => non_neg_integer(),
      filter_size => 16..255,
      header_data => binary(),
      position => non_neg_integer(),
      next_position => non_neg_integer()}.
-type transport() :: tcp | ssl.

-opaque state() :: #osiris_log{}.

-export_type([state/0,
              chunk_iterator/0,
              range/0,
              config/0,
              counter_spec/0,
              transport/0]).

-spec directory(osiris:config() | list()) -> file:filename_all().
directory(#{name := Name, dir := Dir}) ->
    filename:join(Dir, Name);
directory(#{name := Name}) ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    filename:join(Dir, Name);
directory(Name) when ?IS_STRING(Name) ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    filename:join(Dir, Name).

-spec init(config()) -> state().
init(Config) ->
    init(Config, writer).

-spec init(config(), writer | acceptor) -> state().
init(#{dir := Dir,
       name := Name,
       epoch := Epoch} = Config,
     WriterType) ->
    %% scan directory for segments if in write mode
    MaxSizeBytes = maps:get(max_segment_size_bytes, Config,
                            ?DEFAULT_MAX_SEGMENT_SIZE_B),
    MaxSizeChunks = application:get_env(osiris, max_segment_size_chunks,
                                        ?DEFAULT_MAX_SEGMENT_SIZE_C),
    Retention = maps:get(retention, Config, []),
    FilterSize = maps:get(filter_size, Config, ?DEFAULT_FILTER_SIZE),
    ?INFO("Stream: ~ts will use ~ts for osiris log data directory",
          [Name, Dir]),
    ?DEBUG_(Name, "max_segment_size_bytes: ~b,
           max_segment_size_chunks ~b, retention ~w, filter size ~b",
            [MaxSizeBytes, MaxSizeChunks, Retention, FilterSize]),
    ok = filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        Err ->
            throw(Err)
    end,

    Cnt = make_counter(Config),
    %% initialise offset counter to -1 as 0 is the first offset in the log and
    %% it hasn't necessarily been written yet, for an empty log the first offset
    %% is initialised to 0 however and will be updated after each retention run.
    counters:put(Cnt, ?C_OFFSET, -1),
    counters:put(Cnt, ?C_SEGMENTS, 0),
    Shared = case Config of
                 #{shared := S} ->
                     S;
                 _ ->
                     osiris_log_shared:new()
             end,
    Cfg = #cfg{directory = Dir,
               name = Name,
               max_segment_size_bytes = MaxSizeBytes,
               max_segment_size_chunks = MaxSizeChunks,
               tracking_config = maps:get(tracking_config, Config, #{}),
               retention = Retention,
               counter = Cnt,
               counter_id = counter_id(Config),
               shared = Shared,
               filter_size = FilterSize},
    ok = osiris_log_read:maybe_fix_corrupted_files(Config),
    DefaultNextOffset = case Config of
                            #{initial_offset := IO}
                              when WriterType == acceptor ->
                                IO;
                            _ ->
                                0
                        end,
    case osiris_log_read:first_and_last_seginfos(Config) of
        none ->
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            open_new_segment(#?MODULE{cfg = Cfg,
                                      mode =
                                          #write{type = WriterType,
                                                 tail_info = {DefaultNextOffset,
                                                              empty},
                                                 current_epoch = Epoch}});
        {NumSegments,
         #seg_info{first = #chunk_info{id = FstChId,
                                       timestamp = FstTs}},
         #seg_info{file = Filename,
                   index = IdxFilename,
                   size = Size,
                   last = #chunk_info{epoch = LastEpoch,
                                      timestamp = LastTs,
                                      id = LastChId,
                                      num = LastNum}}} ->
            %% assert epoch is same or larger
            %% than last known epoch
            case LastEpoch > Epoch of
                true ->
                    exit({invalid_epoch, LastEpoch, Epoch});
                _ ->
                    ok
            end,
            TailInfo = {LastChId + LastNum,
                        {LastEpoch, LastChId, LastTs}},

            counters:put(Cnt, ?C_FIRST_OFFSET, FstChId),
            counters:put(Cnt, ?C_FIRST_TIMESTAMP, FstTs),
            counters:put(Cnt, ?C_OFFSET, LastChId + LastNum - 1),
            counters:put(Cnt, ?C_SEGMENTS, NumSegments),
            osiris_log_shared:set_first_chunk_id(Shared, FstChId),
            osiris_log_shared:set_last_chunk_id(Shared, LastChId),
            ?DEBUG_(Name, " next offset ~b first offset ~b",
                    [element(1, TailInfo),
                     FstChId]),
            {ok, SegFd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, Size} = file:position(SegFd, Size),
            %% maybe_fix_corrupted_files has truncated the index to the last
            %% record pointing
            %% at a valid chunk we can now truncate the segment to size in
            %% case there is trailing data
            ok = file:truncate(SegFd),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            {ok, IdxEof} = file:position(IdxFd, eof),
            NumChunks = (IdxEof - ?IDX_HEADER_SIZE) div ?INDEX_RECORD_SIZE_B,
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = TailInfo,
                                segment_size = {Size, NumChunks},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     segment_io = {fd, SegFd},
                     index_io = {fd, IdxFd}};
        {1, #seg_info{file = Filename,
                      index = IdxFilename,
                      last = undefined}, _} ->
            %% the empty log case
            {ok, SegFd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            {ok, _} = file:position(SegFd, ?LOG_HEADER_SIZE),
            counters:put(Cnt, ?C_SEGMENTS, 1),
            %% the segment could potentially have trailing data here so we'll
            %% do a truncate just in case. The index would have been truncated
            %% earlier
            ok = file:truncate(SegFd),
            {ok, _} = file:position(IdxFd, ?IDX_HEADER_SIZE),
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = {DefaultNextOffset, empty},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     segment_io = {fd, SegFd},
                     index_io = {fd, IdxFd}}
    end.

-spec write([osiris:data()], state()) -> state().
write(Entries, State) when is_list(Entries) ->
    Timestamp = erlang:system_time(millisecond),
    write(Entries, ?CHNK_USER, Timestamp, <<>>, State).

-spec write([osiris:data()], integer(), state()) -> state().
write(Entries, Now, #?MODULE{mode = #write{}} = State)
    when is_integer(Now) ->
    write(Entries, ?CHNK_USER, Now, <<>>, State).

-spec write([osiris:data()], chunk_type(), osiris:timestamp(),
            iodata(), state()) -> state().
write([_ | _] = Entries,
      ChType,
      Now,
      Trailer,
      #?MODULE{cfg = #cfg{filter_size = FilterSize},
               mode =
                   #write{current_epoch = Epoch, tail_info = {Next, _}} =
                       _Write0} =
          State0)
    when is_integer(Now) andalso
         is_integer(ChType) ->
    %% The osiris writer always pass Entries in the reversed order
    %% in order to avoid unnecessary lists rev|trav|ersals
    {ChunkData, NumRecords} =
        make_chunk(Entries, Trailer, ChType, Now, Epoch, Next, FilterSize),
    write_chunk(ChunkData, ChType, Now, Epoch, NumRecords, State0);
write([], _ChType, _Now, _Trailer, #?MODULE{} = State) ->
    State.

-spec accept_chunk(iodata(), state()) -> state().
accept_chunk([<<?MAGIC:4/unsigned,
                ?VERSION:4/unsigned,
                ChType:8/unsigned,
                _NumEntries:16/unsigned,
                NumRecords:32/unsigned,
                Timestamp:64/signed,
                Epoch:64/unsigned,
                Next:64/unsigned,
                Crc:32/integer,
                DataSize:32/unsigned,
                _TrailerSize:32/unsigned,
                FilterSize:8/unsigned,
                _Reserved:24,
                _Filter:FilterSize/binary,
                Data/binary>>
              | DataParts] =
                 Chunk,
             #?MODULE{cfg = #cfg{}, mode = #write{tail_info = {Next, _}}} =
                 State0) ->
    DataAndTrailer = [Data | DataParts],
    validate_crc(Next, Crc, part(DataSize, DataAndTrailer)),
    %% assertion
    % true = iolist_size(DataAndTrailer) == (DataSize + TrailerSize),
    write_chunk(Chunk, ChType, Timestamp, Epoch, NumRecords, State0);
accept_chunk(Binary, State) when is_binary(Binary) ->
    accept_chunk([Binary], State);
accept_chunk([<<?MAGIC:4/unsigned,
                ?VERSION:4/unsigned,
                _ChType:8/unsigned,
                _NumEntries:16/unsigned,
                _NumRecords:32/unsigned,
                _Timestamp:64/signed,
                _Epoch:64/unsigned,
                Next:64/unsigned,
                _Crc:32/integer,
                _/binary>>
              | _] =
                 _Chunk,
             #?MODULE{cfg = #cfg{},
                      mode = #write{tail_info = {ExpectedNext, _}}}) ->
    exit({accept_chunk_out_of_order, Next, ExpectedNext}).

-spec next_offset(state()) -> offset().
next_offset(#?MODULE{mode = #write{tail_info = {Next, _}}}) ->
    Next;
next_offset(#?MODULE{mode = #read{next_offset = Next}}) ->
    Next.

-spec first_offset(state()) -> offset().
first_offset(#?MODULE{cfg = #cfg{counter = Cnt}}) ->
    counters:get(Cnt, ?C_FIRST_OFFSET).

-spec first_timestamp(state()) -> osiris:timestamp().
first_timestamp(#?MODULE{cfg = #cfg{counter = Cnt}}) ->
    counters:get(Cnt, ?C_FIRST_TIMESTAMP).

-spec tail_info(state()) -> osiris:tail_info().
tail_info(#?MODULE{mode = #write{tail_info = TailInfo}}) ->
    TailInfo.

%% called by the writer before every write to evalaute segment size
%% and write a tracking snapshot
-spec evaluate_tracking_snapshot(state(), osiris_tracking:state()) ->
    {state(), osiris_tracking:state()}.
evaluate_tracking_snapshot(#?MODULE{mode = #write{type = writer}} = State0, Trk0) ->
    IsEmpty = osiris_tracking:is_empty(Trk0),
    case max_segment_size_reached(State0) of
        true when not IsEmpty ->
            State1 = State0,
            %% write a new tracking snapshot
            Now = erlang:system_time(millisecond),
            FstOffs = first_offset(State1),
            FstTs = first_timestamp(State1),
            {SnapBin, Trk1} = osiris_tracking:snapshot(FstOffs, FstTs, Trk0),
            {write([SnapBin],
                   ?CHNK_TRK_SNAPSHOT,
                   Now,
                   <<>>,
                   State1), Trk1};
        _ ->
            {State0, Trk0}
    end.

% -spec
-spec init_acceptor(range(), list(), config()) ->
    state().
init_acceptor(Range, EpochOffsets0,
              #{name := Name, dir := Dir} = Conf) ->
    %% truncate to first common last epoch offset
    %% * if the last local chunk offset has the same epoch but is lower
    %% than the last chunk offset then just attach at next offset.
    %% * if it is higher - truncate to last epoch offset
    %% * if it has a higher epoch than last provided - truncate to last offset
    %% of previous
    %% sort them so that the highest epochs go first
    EpochOffsets =
        lists:reverse(
            lists:sort(EpochOffsets0)),

    %% then truncate to
    IdxFiles = osiris_log_read:sorted_index_files(Dir),
    ?DEBUG_(Name, "from epoch offsets: ~w range ~w", [EpochOffsets, Range]),
    RemIdxFiles = osiris_log_read:truncate_to(Name, Range, EpochOffsets, IdxFiles),
    %% after truncation we can do normal init
    InitOffset = case Range  of
                     empty -> 0;
                     {O, _} -> O
                 end,
    init(Conf#{initial_offset => InitOffset,
               index_files => RemIdxFiles}, acceptor).

-spec init_data_reader(osiris:tail_info(), config()) ->
    {ok, state()} |
    {error, {offset_out_of_range, empty | {offset(), offset()}}} |
    {error, {invalid_last_offset_epoch, epoch(), offset()}} |
    {error, file:posix()}.
init_data_reader(TailInfo, Config) ->
    osiris_log_read:init_data_reader(TailInfo, Config).


%% @doc Initialise a new offset reader
%% @param OffsetSpec specifies where in the log to attach the reader
%% `first': Attach at first available offset.
%% `last': Attach at the last available chunk offset or the next available offset
%% if the log is empty.
%% `next': Attach to the next chunk offset to be written.
%% `{abs, offset()}': Attach at the provided offset. If this offset does not exist
%% in the log it will error with `{error, {offset_out_of_range, Range}}'
%% `offset()': Like `{abs, offset()}' but instead of erroring it will fall back
%% to `first' (if lower than first offset in log) or `nextl if higher than
%% last offset in log.
%% @param Config The configuration. Requires the `dir' key.
%% @returns `{ok, state()} | {error, Error}' when error can be
%% `{offset_out_of_range, empty | {From :: offset(), To :: offset()}}'
%% @end
-spec init_offset_reader(OffsetSpec :: offset_spec(),
                         Config :: config()) ->
                            {ok, state()} |
                            {error,
                             {offset_out_of_range,
                              empty | {From :: offset(), To :: offset()}}} |
                            {error, {invalid_chunk_header, term()}} |
                            {error, no_index_file} |
                            {error, retries_exhausted}.
init_offset_reader(OffsetSpec, Conf) ->
    osiris_log_read:init_offset_reader(OffsetSpec, Conf, _Attemps = 3).

-spec committed_offset(state()) -> integer().
committed_offset(State) ->
    committed_chunk_id(State).

-spec committed_chunk_id(state()) -> integer().
committed_chunk_id(#?MODULE{cfg = #cfg{shared = Ref}}) ->
    osiris_log_shared:committed_chunk_id(Ref).

-spec set_committed_chunk_id(state(), offset()) -> ok.
set_committed_chunk_id(#?MODULE{mode = #write{},
                                cfg = #cfg{shared = Ref}}, ChunkId)
  when is_integer(ChunkId) ->
    osiris_log_shared:set_committed_chunk_id(Ref, ChunkId).

-spec last_chunk_id(state()) -> integer().
last_chunk_id(#?MODULE{cfg = #cfg{shared = Ref}}) ->
    osiris_log_shared:last_chunk_id(Ref).

-spec get_current_epoch(state()) -> non_neg_integer().
get_current_epoch(#?MODULE{mode = #write{current_epoch = Epoch}}) ->
    Epoch.

-spec get_directory(state()) -> file:filename_all().
get_directory(#?MODULE{cfg = #cfg{directory = Dir}}) ->
    Dir.

-spec get_name(state()) -> string().
get_name(#?MODULE{cfg = #cfg{name = Name}}) ->
    Name.

-spec get_shared(state()) -> atomics:atomics_ref().
get_shared(#?MODULE{cfg = #cfg{shared = Shared}}) ->
    Shared.

-spec get_default_max_segment_size_bytes() -> non_neg_integer().
get_default_max_segment_size_bytes() ->
    ?DEFAULT_MAX_SEGMENT_SIZE_B.

-spec counters_ref(state()) -> counters:counters_ref().
counters_ref(#?MODULE{cfg = #cfg{counter = C}}) ->
    C.

-spec read_header(state()) ->
    {ok, header_map(), state()} | {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_header(State) ->
    osiris_log_read:read_header(State).

-define(REC_MATCH_SIMPLE(Len, Rem),
        <<0:1, Len:31/unsigned, Rem/binary>>).
-define(REC_MATCH_SUBBATCH(CompType, NumRec, UncompLen, Len, Rem),
        <<1:1, CompType:3/unsigned, _:4/unsigned,
          NumRecs:16/unsigned,
          UncompressedLen:32/unsigned,
          Len:32/unsigned, Rem/binary>>).

-define(REC_HDR_SZ_SIMPLE_B, 4).
-define(REC_HDR_SZ_SUBBATCH_B, 11).
-define(ITER_READ_AHEAD_B, 64).


-spec chunk_iterator(state()) ->
    {ok, header_map(), osiris_log_read:chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(State) ->
    chunk_iterator(State, 1).

-spec chunk_iterator(state(), pos_integer() | all) ->
    {ok, header_map(), osiris_log_read:chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(State, CreditHint) ->
    osiris_log_read:chunk_iterator(State, CreditHint).

-spec iterator_next(osiris_log_read:chunk_iterator()) ->
    end_of_chunk | {offset_entry(), osiris_log_read:chunk_iterator()}.
iterator_next(Iter) ->
    osiris_log_read:iterator_next(Iter).

-spec read_chunk(state()) ->
    {ok, binary(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk(State) ->
    osiris_log_read:read_chunk(State).

-spec read_chunk_parsed(state()) ->
                           {[record()], state()} |
                           {end_of_stream, state()} |
                           {error, {invalid_chunk_header, term()}}.
read_chunk_parsed(State) ->
    read_chunk_parsed(State, no_header).

-spec read_chunk_parsed(state(), no_header | with_header) ->
    {[record()], state()} |
    {ok, header_map(), [record()], state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk_parsed(State,
                  HeaderOrNot) ->
    osiris_log_read:read_chunk_parsed(State, HeaderOrNot).

-spec send_file(gen_tcp:socket(), state()) ->
                   {ok, state()} |
                   {error, term()} |
                   {end_of_stream, state()}.
send_file(Sock, State) ->
    send_file(Sock, State, fun(_, _) -> ok end).

-spec send_file(gen_tcp:socket() | ssl:socket(), state(),
                fun((header_map(), non_neg_integer()) -> term())) ->
    {ok, state()} |
    {error, term()} |
    {end_of_stream, state()}.
send_file(Sock,
          State,
          Callback) ->
    osiris_log_read:send_file(Sock, State, Callback).

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{counter_id = CntId,
                          readers_counter_fun = Fun},
               segment_io = SegIO,
               index_io = IndexIO}) ->
    close_fd(SegIO),
    close_fd(IndexIO),
    Fun(-1),
    case CntId of
        undefined ->
            ok;
        _ ->
            osiris_counters:delete(CntId)
    end.

delete_directory(#{name := Name,
                   dir := _} = Config) ->
    Dir = directory(Config),
    ?DEBUG_(Name, " deleting directory ~ts", [Dir]),
    delete_dir(Dir);
delete_directory(#{name := Name}) ->
    delete_directory(Name);
delete_directory(Name) when ?IS_STRING(Name) ->
    Dir = directory(Name),
    ?DEBUG_(Name, " deleting directory ~ts", [Dir]),
    delete_dir(Dir).

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

%% TODO Here due to tests, will update test later
sorted_index_files(C) ->
    osiris_log_read:sorted_index_files(C).

index_files_unsorted(Dir) ->
    osiris_log_read:index_files_unsorted(Dir).

orphaned_segments(Dir) ->
    osiris_log_read:orphaned_segments(Dir).

-spec overview(file:filename_all()) ->
    {range(), [{epoch(), offset()}]}.
overview(Dir) ->
    Files = list_dir(Dir),
    %% index files with matching segment
    %% init/1 would repair this situation however as overview may
    %% be called before init/1 happens on a system we need to
    %% explicitly filter these out
    case index_files_with_segment(lists:sort(Files), Dir, []) of
        [] ->
            {empty, []};
        IdxFiles ->
            Range = osiris_log_read:offset_range_from_idx_files(IdxFiles),
            EpochOffsets = osiris_log_read:last_epoch_chunk_ids(<<>>, IdxFiles),
            {Range, EpochOffsets}
    end.

index_files_with_segment([], _, Acc) ->
    lists:reverse(Acc);
index_files_with_segment([<<Name:20/binary, ".index">> = I,
                          <<Name:20/binary, ".segment">>
                           | Rem], Dir, Acc) ->
    index_files_with_segment(Rem, Dir, [filename:join(Dir, I) | Acc]);
index_files_with_segment([_OrphanedOrUnexpected | Rem], Dir, Acc) ->
    %% orphaned segment file or unexpected file, ignore
    index_files_with_segment(Rem, Dir, Acc).



-spec format_status(state()) -> map().
format_status(#?MODULE{cfg = #cfg{directory = Dir,
                                  max_segment_size_bytes  = MSSB,
                                  max_segment_size_chunks  = MSSC,
                                  tracking_config = TrkConf,
                                  retention = Retention,
                                  filter_size = FilterSize},
                       mode = Mode0,
                       current_file = File}) ->
    Mode = case Mode0 of
               #read{type = T,
                     next_offset = Next} ->
                   #{mode => read,
                     type => T,
                     next_offset => Next};
               #write{type = T,
                      current_epoch =E,
                      tail_info = Tail} ->
                   #{mode => write,
                     type => T,
                     tail => Tail,
                     epoch => E}
           end,

    #{mode => Mode,
      directory => Dir,
      max_segment_size_bytes  => MSSB,
      max_segment_size_chunks  => MSSC,
      tracking_config => TrkConf,
      retention => Retention,
      filter_size => FilterSize,
      file => filename:basename(File)}.

-spec update_retention([retention_spec()], state()) -> state().
update_retention(Retention,
                 #?MODULE{cfg = #cfg{name = Name,
                                     retention = Retention0} = Cfg} = State0)
    when is_list(Retention) ->
    ?DEBUG_(Name, " from: ~w to ~w", [Retention0, Retention]),
    State = State0#?MODULE{cfg = Cfg#cfg{retention = Retention}},
    trigger_retention_eval(State).

-spec evaluate_retention(file:filename_all(), [retention_spec()]) ->
    {range(), FirstTimestamp :: osiris:timestamp(),
     NumRemainingFiles :: non_neg_integer()}.
evaluate_retention(Dir, Specs) when is_list(Dir) ->
    % convert to binary for faster operations later
    % mostly in segment_from_index_file/1
    evaluate_retention(unicode:characters_to_binary(Dir), Specs);
evaluate_retention(Dir, Specs) when is_binary(Dir) ->
    osiris_log_read:evaluate_retention(Dir, Specs).

segment_from_index_file(IdxFile) when is_list(IdxFile) ->
    unicode:characters_to_list(string:replace(IdxFile, ".index", ".segment", trailing));
segment_from_index_file(IdxFile) when is_binary(IdxFile) ->
    unicode:characters_to_binary(string:replace(IdxFile, ".index", ".segment", trailing)).

process_entry({FilterValue, Data}, {Entries, Count, Bloom, Acc}) ->
    %% filtered value
    process_entry0(Data, {Entries, Count,
                          osiris_bloom:insert(FilterValue, Bloom), Acc});
process_entry(Data, {Entries, Count, Bloom, Acc}) ->
    %% unfiltered, pass the <<>> empty string
    process_entry0(Data, {Entries, Count,
                          osiris_bloom:insert(<<>>, Bloom), Acc}).

process_entry0({batch, NumRecords, CompType, UncompLen, B},
               {Entries, Count, Bloom, Acc}) ->
    Data = [<<1:1, %% batch record type
              CompType:3/unsigned,
              0:4/unsigned,
              NumRecords:16/unsigned,
              UncompLen:32/unsigned,
              (iolist_size(B)):32/unsigned>>,
            B],
    {Entries + 1, Count + NumRecords, Bloom, [Data | Acc]};
process_entry0(B, {Entries, Count, Bloom, Acc})
  when is_binary(B) orelse
       is_list(B) ->
    %% simple record type
    Data = [<<0:1, (iolist_size(B)):31/unsigned>>, B],
    {Entries + 1, Count + 1, Bloom, [Data | Acc]}.


make_chunk(Blobs, TData, ChType, Timestamp, Epoch, Next, FilterSize) ->
    Bloom0 = osiris_bloom:init(FilterSize),
    {NumEntries, NumRecords, Bloom, EData} =
        lists:foldl(fun process_entry/2, {0, 0, Bloom0, []}, Blobs),

    BloomData = osiris_bloom:to_binary(Bloom),
    BloomSize = byte_size(BloomData),
    Size = iolist_size(EData),
    TSize = iolist_size(TData),
    %% checksum is over entry data only
    Crc = erlang:crc32(EData),
    {[<<?MAGIC:4/unsigned,
        ?VERSION:4/unsigned,
        ChType:8/unsigned,
        NumEntries:16/unsigned,
        NumRecords:32/unsigned,
        Timestamp:64/signed,
        Epoch:64/unsigned,
        Next:64/unsigned,
        Crc:32/integer,
        Size:32/unsigned,
        TSize:32/unsigned,
        BloomSize:8/unsigned,
        0:24/unsigned>>,
      BloomData,
      EData,
      TData],
     NumRecords}.

write_chunk(Chunk,
            ChType,
            Timestamp,
            Epoch,
            NumRecords,
            #?MODULE{cfg = #cfg{counter = CntRef,
                                shared = Shared} = Cfg,
                     segment_io = {fd, Fd},
                     index_io = {fd, IdxFd},
                     mode =
                         #write{segment_size = {SegSizeBytes, SegSizeChunks},
                                tail_info = {Next, _}} =
                             Write} =
                State) ->
    case max_segment_size_reached(State) of
        true ->
            trigger_retention_eval(
              write_chunk(Chunk,
                          ChType,
                          Timestamp,
                          Epoch,
                          NumRecords,
                          open_new_segment(State)));
        false ->
            NextOffset = Next + NumRecords,
            Size = iolist_size(Chunk),
            {ok, Cur} = file:position(Fd, cur),
            ok = file:write(Fd, Chunk),

            ok = file:write(IdxFd,
                            <<Next:64/unsigned,
                              Timestamp:64/signed,
                              Epoch:64/unsigned,
                              Cur:32/unsigned,
                              ChType:8/unsigned>>),
            osiris_log_shared:set_last_chunk_id(Shared, Next),
            %% update counters
            counters:put(CntRef, ?C_OFFSET, NextOffset - 1),
            counters:add(CntRef, ?C_CHUNKS, 1),
            maybe_set_first_offset(Next, Cfg),
            State#?MODULE{mode =
                          Write#write{tail_info = {NextOffset,
                                                   {Epoch, Next, Timestamp}},
                                      segment_size = {SegSizeBytes + Size,
                                                      SegSizeChunks + 1}}}
    end.


maybe_set_first_offset(0, #cfg{shared = Ref}) ->
    osiris_log_shared:set_first_chunk_id(Ref, 0);
maybe_set_first_offset(_, _Cfg) ->
    ok.

max_segment_size_reached(
  #?MODULE{mode = #write{segment_size = {CurrentSizeBytes,
                                         CurrentSizeChunks}},
           cfg = #cfg{max_segment_size_bytes = MaxSizeBytes,
                      max_segment_size_chunks = MaxSizeChunks}}) ->
    CurrentSizeBytes >= MaxSizeBytes orelse
    CurrentSizeChunks >= MaxSizeChunks.

-spec can_read_next(state()) -> boolean().
can_read_next(State) ->
    osiris_log_read:can_read_next(State).

make_file_name(N, Suff) ->
    lists:flatten(
        io_lib:format("~20..0B.~s", [N, Suff])).

open_new_segment(#?MODULE{cfg = #cfg{name = Name,
                                     directory = Dir,
                                     counter = Cnt},
                          segment_io = SegmentIO,
                          index_io = IndexIO,
                          mode = #write{type = _WriterType,
                                        tail_info = {NextOffset, _}} = Write} =
                 State0) ->
    _ = close_fd(SegmentIO),
    _ = close_fd(IndexIO),
    Filename = make_file_name(NextOffset, "segment"),
    IdxFilename = make_file_name(NextOffset, "index"),
    ?DEBUG_(Name, "~ts", [Filename]),
    {ok, IdxFd} =
        file:open(
            filename:join(Dir, IdxFilename), ?FILE_OPTS_WRITE),
    ok = file:write(IdxFd, ?IDX_HEADER),
    {ok, Fd} =
        file:open(
            filename:join(Dir, Filename), ?FILE_OPTS_WRITE),
    ok = file:write(Fd, ?LOG_HEADER),
    %% we always move to the end of the file
    {ok, _} = file:position(Fd, eof),
    {ok, _} = file:position(IdxFd, eof),
    counters:add(Cnt, ?C_SEGMENTS, 1),

    State0#?MODULE{current_file = Filename,
                   segment_io = {fd, Fd},
                   %% reset segment_size counter
                   index_io = {fd, IdxFd},
                   mode = Write#write{segment_size = {?LOG_HEADER_SIZE, 0}}}.

throw_missing({error, enoent}) ->
    throw(missing_file);
throw_missing(Any) ->
    Any.

open(File, Options) ->
    throw_missing(file:open(File, Options)).

validate_crc(ChunkId, Crc, IOData) ->
    case erlang:crc32(IOData) of
        Crc ->
            ok;
        _ ->
            ?ERROR("crc validation failure at chunk id ~bdata size "
                   "~b:",
                   [ChunkId, iolist_size(IOData)]),
            exit({crc_validation_failure, {chunk_id, ChunkId}})
    end.

-spec make_counter(osiris_log:config()) ->
    counters:counters_ref().
make_counter(#{counter := Counter}) ->
    Counter;
make_counter(#{counter_spec := {Name, Fields}}) ->
    %% create a registered counter
    osiris_counters:new(Name, ?COUNTER_FIELDS ++ Fields);
make_counter(_) ->
    %% if no spec is provided we create a local counter only
    counters:new(?C_NUM_LOG_FIELDS, []).

counter_id(#{counter_spec := {Name, _}}) ->
    Name;
counter_id(_) ->
    undefined.

part(0, _) ->
    [];
part(Len, [B | L]) when Len > 0 ->
    S = byte_size(B),
    case Len > S of
        true ->
            [B | part(Len - byte_size(B), L)];
        false ->
            [binary:part(B, {0, Len})]
    end.

-spec recover_tracking(state()) ->
    osiris_tracking:state().
recover_tracking(#?MODULE{cfg = #cfg{directory = Dir,
                                     tracking_config = TrkConfig},
                          current_file = File}) ->
    %% we need to open a new file handle here as we cannot use the one that is
    %% being used for appending to the segment as pread _may_ move the file
    %% position on some systems (such as windows)
    {ok, Fd} = open(filename:join(Dir, File), [read, raw, binary]),
    _ = file:advise(Fd, 0, 0, random),
    %% TODO: if the first chunk in the segment isn't a tracking snapshot and
    %% there are prior segments we could scan at least two segments increasing
    %% the chance of encountering a snapshot and thus ensure we don't miss any
    %% tracking entries
    Trk0 = osiris_tracking:init(undefined, TrkConfig),
    Trk = recover_tracking(Fd, Trk0, ?LOG_HEADER_SIZE),
    _ = file:close(Fd),
    Trk.

recover_tracking(Fd, Trk0, Pos0) ->
    case file:pread(Fd, Pos0, ?HEADER_SIZE_B) of
        {ok,
         <<?MAGIC:4/unsigned,
           ?VERSION:4/unsigned,
           ChType:8/unsigned,
           _:16/unsigned,
           _NumRecords:32/unsigned,
           _Timestamp:64/signed,
           _Epoch:64/unsigned,
           ChunkId:64/unsigned,
           _Crc:32/integer,
           Size:32/unsigned,
           TSize:32/unsigned,
           FSize:8/unsigned,
           _Reserved:24>>} ->
            Pos = Pos0 + ?HEADER_SIZE_B,
            NextPos = Pos + Size + TSize + FSize,
            case ChType of
                ?CHNK_TRK_DELTA ->
                    %% tracking is written a single record so we don't
                    %% have to parse
                    {ok, <<0:1, S:31, Data:S/binary>>} =
                        file:pread(Fd, Pos + FSize, Size),
                    Trk = osiris_tracking:append_trailer(ChunkId, Data, Trk0),
                    %% A tracking delta chunk will not have any writer data
                    %% so no need to parse writers here
                    recover_tracking(Fd, Trk, NextPos);
                ?CHNK_TRK_SNAPSHOT ->
                    {ok, <<0:1, S:31, Data:S/binary>>} =
                        file:pread(Fd, Pos + FSize, Size),
                    Trk = osiris_tracking:init(Data, Trk0),
                    recover_tracking(Fd, Trk, NextPos);
                ?CHNK_USER when TSize > 0 ->
                    {ok, TData} = file:pread(Fd, Pos + FSize + Size, TSize),
                    Trk = osiris_tracking:append_trailer(ChunkId, TData, Trk0),
                    recover_tracking(Fd, Trk, NextPos);
                ?CHNK_USER ->
                    recover_tracking(Fd, Trk0, NextPos)
            end;
        eof ->
            Trk0
    end.

trigger_retention_eval(#?MODULE{cfg =
                                    #cfg{name = Name,
                                         directory = Dir,
                                         retention = RetentionSpec,
                                         counter = Cnt,
                                         shared = Shared}} = State) ->

    %% updates first offset and first timestamp
    %% after retention has been evaluated
    EvalFun = fun ({{FstOff, _}, FstTs, NumSegLeft})
                    when is_integer(FstOff),
                         is_integer(FstTs) ->
                      osiris_log_shared:set_first_chunk_id(Shared, FstOff),
                      counters:put(Cnt, ?C_FIRST_OFFSET, FstOff),
                      counters:put(Cnt, ?C_FIRST_TIMESTAMP, FstTs),
                      counters:put(Cnt, ?C_SEGMENTS, NumSegLeft);
                  (_) ->
                      ok
              end,
    ok = osiris_retention:eval(Name, Dir, RetentionSpec, EvalFun),
    State.

close_fd(undefined) ->
    ok;
close_fd({fd, Fd}) ->
    close_fd(Fd);
%%TODO when is_record(IoDevice, file_descriptor)?
close_fd(Fd) ->
    _ = file:close(Fd),
    ok.


dump_init(File) ->
    {ok, Fd} = file:open(File, [raw, binary, read]),
    {ok, <<"OSIL", _V:4/binary>> } = file:read(Fd, ?LOG_HEADER_SIZE),
    Fd.

dump_init_idx(File) ->
    {ok, Fd} = file:open(File, [raw, binary, read]),
    {ok, <<"OSII", _V:4/binary>> } = file:read(Fd, ?IDX_HEADER_SIZE),
    Fd.

dump_index(Fd) ->
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok,
         <<ChunkId:64/unsigned,
           Timestamp:64/signed,
           Epoch:64/unsigned,
           FilePos:32/unsigned,
           ChType:8/unsigned>>} ->
            #{chunk_id => ChunkId,
              timestamp => Timestamp,
              epoch => Epoch,
              file_pos => FilePos,
              type => ChType};
        Err ->
            Err
    end.



dump_chunk(Fd) ->
    {ok, Pos} = file:position(Fd, cur),
    case file:read(Fd, ?HEADER_SIZE_B + ?DEFAULT_FILTER_SIZE) of
        {ok, <<?MAGIC:4/unsigned,
               ?VERSION:4/unsigned,
               ChType:8/unsigned,
               NumEntries:16/unsigned,
               NumRecords:32/unsigned,
               Timestamp:64/signed,
               Epoch:64/unsigned,
               NextChId0:64/unsigned,
               Crc:32/integer,
               DataSize:32/unsigned,
               TrailerSize:32/unsigned,
               FilterSize:8/unsigned,
               _Reserved:24,
               MaybeFilter/binary>>} ->

            NextPos = Pos + ?HEADER_SIZE_B + FilterSize + DataSize + TrailerSize,

            ChunkFilter = case MaybeFilter of
                              <<F:FilterSize/binary, _/binary>> ->
                                  %% filter is of default size or 0
                                  F;
                              _ when FilterSize > 0 ->
                                  %% the filter is larger than default
                                  case file:pread(Fd, Pos + ?HEADER_SIZE_B,
                                                  FilterSize) of
                                      {ok, F} ->
                                          F;
                                      eof ->
                                          eof
                                  end;
                              _ ->
                                  <<>>
                          end,
            {ok, Data} = file:pread(Fd, Pos + FilterSize + ?HEADER_SIZE_B, DataSize),
            CrcMatch = erlang:crc32(Data) =:= Crc,
            _ = file:position(Fd, NextPos),
            #{chunk_id => NextChId0,
              epoch => Epoch,
              type => ChType,
              crc => Crc,
              data => Data,
              crc_match => CrcMatch,
              num_records => NumRecords,
              num_entries => NumEntries,
              timestamp => Timestamp,
              data_size => DataSize,
              trailer_size => TrailerSize,
              filter_size => FilterSize,
              chunk_filter => ChunkFilter,
              next_position => NextPos,
              position => Pos};
        eof ->
            eof
    end.

dump_crc_check(Fd) ->
    case dump_chunk(Fd) of
        eof ->
            eof;
        #{crc_match := false} = Ch ->
            Ch;
        _ ->
            dump_crc_check(Fd)
    end.

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
    end.

-ifdef(TEST).


part_test() ->
    [<<"ABCD">>] = part(4, [<<"ABCDEF">>]),
    [<<"AB">>, <<"CD">>] = part(4, [<<"AB">>, <<"CDEF">>]),
    [<<"AB">>, <<"CDEF">>] = part(6, [<<"AB">>, <<"CDEF">>]),
    ok.

-endif.
