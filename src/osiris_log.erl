%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log).

-include("src/osiris.hrl").
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

%% osiris_log_manifest callbacks (default implementations)
-export([writer_manifest/1,
         acceptor_manifest/3,
         find_data_reader_position/2,
         find_offset_reader_position/2,
         handle_event/2,
         close_manifest/1,
         delete/1]).

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

-behaviour(osiris_log_manifest).

% maximum size of a segment in bytes
-define(DEFAULT_MAX_SEGMENT_SIZE_B, 500 * 1000 * 1000).
% maximum number of chunks per segment
-define(DEFAULT_MAX_SEGMENT_SIZE_C, 256_000).
-define(C_OFFSET, 1).
-define(C_FIRST_OFFSET, 2).
-define(C_FIRST_TIMESTAMP, 3).
-define(C_CHUNKS, 4).
-define(C_SEGMENTS, 5).
-define(COUNTER_FIELDS,
        [
         {offset, ?C_OFFSET, counter,
          "The last offset (not chunk id) in the log for writers. The last offset read for readers" },
         {first_offset, ?C_FIRST_OFFSET, counter, "First offset, not updated for readers"},
         {first_timestamp, ?C_FIRST_TIMESTAMP, counter, "First timestamp, not updated for readers"},
         {chunks, ?C_CHUNKS, counter, "Number of chunks read or written, incremented even if a reader only reads the header"},
         {segments, ?C_SEGMENTS, counter, "Number of segments"}
        ]
       ).

-define(ZERO_IDX_MATCH(Rem),
        <<0:64/unsigned,
          0:64/signed,
          0:64/unsigned,
          0:32/unsigned,
          0:8/unsigned,
         Rem/binary>>).

-define(IDX_MATCH(ChId, Epoch, FilePos),
        <<ChId:64/unsigned,
          _:64/signed,
          Epoch:64/unsigned,
          FilePos:32/unsigned,
          _:8/unsigned,
        _/binary>>).

-define(SKIP_SEARCH_JUMP, 2048).

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
-type chunk_type() ::
    ?CHNK_USER |
    ?CHNK_TRK_DELTA |
    ?CHNK_TRK_SNAPSHOT.
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

%% holds static or rarely changing fields
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
         reader :: {module(), osiris_log_reader:state()},
         next_offset = 0 :: offset(),
         transport :: transport(),
         chunk_selector :: all | user_data,
         position = 0 :: non_neg_integer(),
         filter :: undefined | osiris_bloom:mstate()}).
-record(write,
        {type = writer :: writer | acceptor,
         segment_size = {?LOG_HEADER_SIZE, 0} :: {non_neg_integer(), non_neg_integer()},
         current_epoch :: non_neg_integer(),
         tail_info = {0, empty} :: osiris:tail_info(),
         manifest :: {module(), osiris_log_manifest:state()}
        }).
-record(?MODULE,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename_all(),
         fd :: undefined | file:io_device()
        }).
-record(seg_info,
        {file :: file:filename_all(),
         size = 0 :: non_neg_integer(),
         index :: file:filename_all(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).
%% Default manifest implementation which lists the configured dir for index
%% files.
-record(manifest,
        {dir :: file:filename_all(),
         index_fd :: file:io_device() | undefined}).

-opaque state() :: #?MODULE{}.
-type manifest() :: #manifest{}.

-export_type([state/0,
              chunk_iterator/0,
              range/0,
              config/0,
              counter_spec/0,
              transport/0,
              chunk_type/0,
              manifest/0]).

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
    DefaultNextOffset = case Config of
                            #{initial_offset := IO}
                              when WriterType == acceptor ->
                                IO;
                            _ ->
                                0
                        end,

    ManifestMod = application:get_env(osiris, log_manifest, ?MODULE),
    {Info, Manifest0} = case Config of
                            #{acceptor_manifest := {I, M}} ->
                                acceptor = WriterType,
                                {I, M};
                            _ ->
                                ManifestMod:writer_manifest(Config)
                        end,
    case Info of
        #{num_segments := 0} ->
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            open_new_segment(#?MODULE{cfg = Cfg,
                                      mode =
                                          #write{type = WriterType,
                                                 tail_info = {DefaultNextOffset,
                                                              empty},
                                                 manifest = {ManifestMod,
                                                             Manifest0},
                                                 current_epoch = Epoch}});
        #{num_segments := NumSegments,
          first := #{first := #chunk_info{id = FstChId,
                                          timestamp = FstTs}},
          last := #{file := Filename,
                    size := Size,
                    last := #chunk_info{epoch = LastEpoch,
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
            Event = {segment_opened, undefined, Filename},
            Manifest = ManifestMod:handle_event(Event, Manifest0),
            NumChunks = LastChId - FstChId,
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = TailInfo,
                                segment_size = {Size, NumChunks},
                                manifest = {ManifestMod, Manifest},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     fd = SegFd};
        #{num_segments := 1,
          first := #{file := Filename}} ->
            %% the empty log case
            {ok, SegFd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, _} = file:position(SegFd, ?LOG_HEADER_SIZE),
            counters:put(Cnt, ?C_SEGMENTS, 1),
            %% the segment could potentially have trailing data here so we'll
            %% do a truncate just in case. The index would have been truncated
            %% earlier
            ok = file:truncate(SegFd),
            osiris_log_shared:set_first_chunk_id(Shared, DefaultNextOffset - 1),
            osiris_log_shared:set_last_chunk_id(Shared, DefaultNextOffset - 1),
            Event = {segment_opened, undefined, Filename},
            Manifest = ManifestMod:handle_event(Event, Manifest0),
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = {DefaultNextOffset, empty},
                                manifest = {ManifestMod, Manifest},
                                current_epoch = Epoch},
                     current_file = filename:basename(Filename),
                     fd = SegFd}
    end.

writer_manifest(#{dir := Dir} = Config) ->
    ok = filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok ->
            ok;
        {error, eexist} ->
            ok;
        Err ->
            throw(Err)
    end,
    ok = maybe_fix_corrupted_files(Config),
    Info = case first_and_last_seginfos(Config) of
               none ->
                   #{num_segments => 0,
                     segment_offsets => []};
               {SegmentOffsets,
                #seg_info{file = FirstFile,
                          size = FirstSize,
                          first = FirstFirstChunk,
                          last = FirstLastChunk},
                #seg_info{file = LastFile,
                          size = LastSize,
                          first = LastFirstChunk,
                          last = #chunk_info{} = LastLastChunk}} ->
                   First = #{file => FirstFile,
                             size => FirstSize,
                             first => FirstFirstChunk,
                             last => FirstLastChunk},
                   Last = #{file => LastFile,
                            size => LastSize,
                            first => LastFirstChunk,
                            last => LastLastChunk},
                   #{num_segments => length(SegmentOffsets),
                     first => First,
                     last => Last,
                     segment_offsets => SegmentOffsets};
               {[_] = SegmentOffsets,
                #seg_info{last = undefined,
                          file = Filename,
                          size = Size}, _} ->
                   SegInfo = #{file => Filename,
                               size => Size},
                   #{num_segments => 1,
                     first => SegInfo,
                     last => SegInfo,
                     segment_offsets => SegmentOffsets}
           end,
    %% The segment_opened event will create the index fd.
    {Info, #manifest{dir = Dir}}.

maybe_fix_corrupted_files([]) ->
    ok;
maybe_fix_corrupted_files(#{dir := Dir}) ->
    ok = maybe_fix_corrupted_files(sorted_index_files(Dir)),
    %% dangling segments can be left behind if the server process crashes
    %% after the retention evaluator process deleted the index but
    %% before it deleted the corresponding segment
    [begin
         ?INFO("deleting left over segment '~s' in directory ~s",
               [F, Dir]),
         ok = prim_file:delete(filename:join(Dir, F))
     end|| F <- orphaned_segments(Dir)],
    ok;
maybe_fix_corrupted_files([IdxFile]) ->
    SegFile = segment_from_index_file(IdxFile),
    ok = truncate_invalid_idx_records(IdxFile, file_size_or_zero(SegFile)),
    case file_size(IdxFile) =< ?IDX_HEADER_SIZE + ?INDEX_RECORD_SIZE_B of
        true ->
            % the only index doesn't contain a single valid record
            % make sure it has a valid header
            {ok, IdxFd} = file:open(IdxFile, ?FILE_OPTS_WRITE),
            ok = file:write(IdxFd, ?IDX_HEADER),
            ok = file:close(IdxFd);
        false ->
            ok
    end,
    case file_size_or_zero(SegFile) =< ?LOG_HEADER_SIZE + ?HEADER_SIZE_B of
        true ->
            % the only segment doesn't contain a single valid chunk
            % make sure it has a valid header
            {ok, SegFd} = file:open(SegFile, ?FILE_OPTS_WRITE),
            ok = file:write(SegFd, ?LOG_HEADER),
            ok = file:close(SegFd);
        false ->
            ok
    end;
maybe_fix_corrupted_files(IdxFiles) ->
    LastIdxFile = lists:last(IdxFiles),
    LastSegFile = segment_from_index_file(LastIdxFile),
    try file_size(LastSegFile) of
        N when N =< ?HEADER_SIZE_B ->
            % if the segment doesn't contain any chunks, just delete it
            ?WARNING("deleting an empty segment file: ~0p", [LastSegFile]),
            ok = prim_file:delete(LastIdxFile),
            ok = prim_file:delete(LastSegFile),
            maybe_fix_corrupted_files(IdxFiles -- [LastIdxFile]);
        LastSegFileSize ->
            ok = truncate_invalid_idx_records(LastIdxFile, LastSegFileSize)
    catch missing_file ->
            % if the last segment is missing, just delete its index
            ?WARNING("deleting index of the missing last segment file: ~0p",
                     [LastSegFile]),
            ok = prim_file:delete(LastIdxFile),
            maybe_fix_corrupted_files(IdxFiles -- [LastIdxFile])
    end.

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

truncate_invalid_idx_records(IdxFile, SegSize) ->
    % TODO currently, if we have no valid index records,
    % we truncate the segment, even though it could theoretically
    % contain valid chunks. This should never happen in normal
    % operations, since we write to the index first
    % and fsync it first. However, it feels wrong, since we can
    % reconstruct the index from a segment. We should probably
    % add an option to perform a full segment scan and reconstruct
    % the index for the valid chunks.
    SegFile = segment_from_index_file(IdxFile),
    {ok, IdxFd} = open(IdxFile, [raw, binary, write, read]),
    {ok, Pos} = position_at_idx_record_boundary(IdxFd, eof),
    ok = skip_invalid_idx_records(IdxFd, SegFile, SegSize, Pos),
    ok = file:truncate(IdxFd),
    file:close(IdxFd).

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
                Data/binary>>
              | DataParts] =
                 Chunk,
             #?MODULE{cfg = #cfg{}, mode = #write{tail_info = {Next, _}}} =
                 State0) ->
    DataAndTrailer = skip(FilterSize, [Data | DataParts]),
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

%% called by the writer before every write to evaluate segment size
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
init_acceptor(Range, EpochOffsets0, Conf) ->
    EpochOffsets =
        lists:reverse(
            lists:sort(EpochOffsets0)),
    ManifestMod = application:get_env(osiris, log_manifest, ?MODULE),
    {Info, Manifest} = ManifestMod:acceptor_manifest(Range, EpochOffsets,
                                                     Conf),
    InitOffset = case Range  of
                     empty -> 0;
                     {O, _} -> O
                 end,
    init(Conf#{initial_offset => InitOffset,
               acceptor_manifest => {Info, Manifest}}, acceptor).

acceptor_manifest(Range, EpochOffsets,
                  #{name := Name, dir := Dir} = Conf) ->
    %% truncate to first common last epoch offset
    %% * if the last local chunk offset has the same epoch but is lower
    %% than the last chunk offset then just attach at next offset.
    %% * if it is higher - truncate to last epoch offset
    %% * if it has a higher epoch than last provided - truncate to last offset
    %% of previous
    %% sort them so that the highest epochs go first

    %% then truncate to
    IdxFiles = sorted_index_files(Dir),
    ?DEBUG_(Name, "from epoch offsets: ~w range ~w", [EpochOffsets, Range]),
    RemIdxFiles = truncate_to(Name, Range, EpochOffsets, IdxFiles),
    %% after truncation we can do normal init
    writer_manifest(Conf#{index_files => RemIdxFiles}).

chunk_id_index_scan(IdxFile, ChunkId)
  when ?IS_STRING(IdxFile) ->
    Fd = open_index_read(IdxFile),
    chunk_id_index_scan0(Fd, ChunkId).

chunk_id_index_scan0(Fd, ChunkId) ->
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok, ?IDX_MATCH(ChunkId, Epoch, FilePos)} ->
            {ok, IdxPos} = file:position(Fd, cur),
            ok = file:close(Fd),
            {ChunkId, Epoch, FilePos, IdxPos - ?INDEX_RECORD_SIZE_B};
        {ok, _} ->
            chunk_id_index_scan0(Fd, ChunkId);
        eof ->
            ok = file:close(Fd),
            eof
    end.

delete_segment_from_index(Index) ->
    File = segment_from_index_file(Index),
    ?DEBUG("osiris_log: deleting segment ~ts", [File]),
    ok = prim_file:delete(Index),
    ok = prim_file:delete(File),
    ok.

truncate_to(_Name, _Range, _EpochOffsets, []) ->
    %% the target log is empty
    [];
truncate_to(_Name, _Range, [], IdxFiles) ->
    %% ?????  this means the entire log is out
    [begin ok = delete_segment_from_index(I) end || I <- IdxFiles],
    [];
truncate_to(Name, RemoteRange, [{E, ChId} | NextEOs], IdxFiles) ->
    case find_segment_for_offset(ChId, IdxFiles) of
        {Result, _} when Result == not_found orelse
                         Result == end_of_log ->
            %% both not_found and end_of_log needs to be treated as not found
            %% as they are...
            case build_seg_info(lists:last(IdxFiles)) of
                {ok, #seg_info{last = #chunk_info{epoch = E,
                                                  id = LastChId,
                                                  num = Num}}}
                when ChId > LastChId ->
                    %% the last available local chunk id is smaller than the
                    %% source's last chunk id but is in the same epoch
                    %% check if there is any overlap
                    LastOffsLocal = LastChId + Num - 1,
                    FstOffsetRemote = case RemoteRange of
                                          empty -> 0;
                                          {F, _} -> F
                                      end,
                    case LastOffsLocal < FstOffsetRemote of
                        true ->
                            %% there is no overlap, need to delete all
                            %% local segments
                            [begin ok = delete_segment_from_index(I) end
                             || I <- IdxFiles],
                            [];
                        false ->
                            %% there is overlap
                            %% no truncation needed
                            IdxFiles
                    end;
                {ok, _} ->
                    truncate_to(Name, RemoteRange, NextEOs, IdxFiles)
                    %% TODO: what to do if error is returned from
                    %% build_seg_info/1?
            end;
        {found, #seg_info{file = File, index = IdxFile}} ->
            ?DEBUG_(Name, " truncating to chunk_id ~b in epoch ~b",
                    [ChId, E]),
            %% this is the inclusive case
            %% next offset needs to be a chunk offset
            %% if it is not found we know the offset requested isn't a chunk
            %% id and thus isn't valid
            case chunk_id_index_scan(IdxFile, ChId) of
                {ChId, E, Pos, IdxPos} when is_integer(Pos) ->
                    %% the  Chunk id was found and has the right epoch
                    %% lets truncate to this point
                    %% FilePos could be eof here which means the next offset
                    {ok, Fd} = file:open(File, [read, write, binary, raw]),
                    _ = file:advise(Fd, 0, 0, random),
                    {ok, IdxFd} = file:open(IdxFile, [read, write, binary, raw]),

                    NextPos = next_chunk_pos(Fd, Pos),
                    {ok, _} = file:position(Fd, NextPos),
                    ok = file:truncate(Fd),

                    {ok, _} = file:position(IdxFd, IdxPos + ?INDEX_RECORD_SIZE_B),
                    ok = file:truncate(IdxFd),
                    ok = file:close(Fd),
                    ok = file:close(IdxFd),
                    %% delete all segments with a first offset larger then ChId
                    %% and return the remainder
                    lists:filter(
                      fun (I) ->
                              case index_file_first_offset(I) > ChId of
                                  true ->
                                      ok = delete_segment_from_index(I),
                                      false;
                                  false ->
                                      true
                              end
                      end, IdxFiles);
                _ ->
                    truncate_to(Name, RemoteRange, NextEOs, IdxFiles)
            end
    end.

-spec init_data_reader(osiris:tail_info(), config()) ->
    {ok, state()} |
    {error, {offset_out_of_range, empty | {offset(), offset()}}} |
    {error, {invalid_last_offset_epoch, epoch(), offset()}} |
    {error, file:posix()}.
init_data_reader(TailInfo, Config) ->
    ManifestMod = application:get_env(osiris, log_manifest, ?MODULE),
    case ManifestMod:find_data_reader_position(TailInfo, Config) of
        {ok, ChunkId, Pos, Segment} ->
            init_data_reader_at(ChunkId, Pos, Segment, Config);
        {error, _} = Err ->
            Err
    end.

find_data_reader_position({StartChunkId, PrevEOT},
                          #{dir := Dir,
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
            find_data_reader_pos(StartChunkId,
                                 find_segment_for_offset(StartChunkId,
                                                         IdxFiles),
                                 Config);
        _ ->
            {PrevEpoch, PrevChunkId, _PrevTs} = PrevEOT,
            case check_chunk_has_expected_epoch(Name, PrevChunkId,
                                                PrevEpoch, IdxFiles) of
                ok ->
                    find_data_reader_pos(StartChunkId,
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
    ReaderMod = application:get_env(osiris, log_reader, osiris_log_reader),
    case ReaderMod:open(File) of
        {ok, Reader} ->
            Cnt = make_counter(Config),
            counters:put(Cnt, ?C_OFFSET, ChunkId - 1),
            CountersFun(1),
            {ok,
             #?MODULE{cfg =
                      #cfg{directory = Dir,
                           counter = Cnt,
                           counter_id = counter_id(Config),
                           name = Name,
                           readers_counter_fun = CountersFun,
                           shared = Shared
                          },
                      mode =
                      #read{type = data,
                            reader = {ReaderMod, Reader},
                            next_offset = ChunkId,
                            chunk_selector = all,
                            position = FilePos,
                            transport = maps:get(transport, Config, tcp)}}};
        Err ->
            Err
    end.

find_data_reader_pos(ChunkId,
                     {end_of_log, #seg_info{file = File, last = LastChunk}},
                     _Config) ->
    {ChunkId, AttachPos} = next_location(LastChunk),
    {ok, ChunkId, AttachPos, File};
find_data_reader_pos(ChunkId,
                     {found, #seg_info{file = File} = SegInfo},
                     Config) ->
    Name = maps:get(name, Config, <<>>),
    {ChunkId, _Epoch, FilePos} = offset_idx_scan(Name, ChunkId, SegInfo),
    {ok, ChunkId, FilePos, File}.

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
    init_offset_reader(OffsetSpec, Conf, 3).

init_offset_reader(_OffsetSpec, _Conf, 0) ->
    {error, retries_exhausted};
init_offset_reader(OffsetSpec, Conf, Attempt) ->
    ManifestMod = application:get_env(osiris, log_manifest, ?MODULE),
    try ManifestMod:find_offset_reader_position(OffsetSpec, Conf) of
        {ok, ChunkId, Pos, Segment} ->
            open_offset_reader_at(Segment, ChunkId, Pos, Conf);
        {error, _} = Err ->
            Err
    catch
        missing_file ->
            %% Retention policies are likely being applied, let's try again
            %% TODO: should we limit the number of retries?
            %% Remove cached index_files from config
            init_offset_reader(OffsetSpec, Conf, Attempt - 1);
        {retry_with, NewOffsSpec, NewConf} ->
            init_offset_reader(NewOffsSpec, NewConf, Attempt - 1)
    end.

find_offset_reader_position({abs, Offs}, #{dir := Dir} = Config) ->
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
                    find_offset_reader_position(Offs, Config)
            end
    end;
find_offset_reader_position({timestamp, Ts}, #{} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            find_offset_reader_position(next, Conf);
        IdxFilesRev ->
            case timestamp_idx_file_search(Ts, IdxFilesRev) of
                {scan, IdxFile} ->
                    %% segment was found, now we need to scan index to
                    %% find nearest offset
                    {ChunkId, FilePos} = chunk_location_for_timestamp(IdxFile, Ts),
                    SegmentFile = segment_from_index_file(IdxFile),
                    {ok, ChunkId, FilePos, SegmentFile};
                {first_in, IdxFile} ->
                    {ok, Fd} = file:open(IdxFile, [raw, binary, read]),
                    {ok, ?IDX_MATCH(ChunkId, _, FilePos)} = first_idx_record(Fd),
                    SegmentFile = segment_from_index_file(IdxFile),
                    {ok, ChunkId, FilePos, SegmentFile};
                next ->
                    %% segment was not found, attach next
                    %% this should be rare
                    find_offset_reader_position(next, Conf)
            end
    end;
find_offset_reader_position(first, #{} = Conf) ->
    case sorted_index_files(Conf) of
        [] ->
            {error, no_index_file};
        [FstIdxFile | _ ] ->
            case build_seg_info(FstIdxFile) of
                {ok, #seg_info{file = File,
                               first = undefined}} ->
                    %% empty log, attach at 0
                    {ok, 0, ?LOG_HEADER_SIZE, File};
                {ok, #seg_info{file = File,
                               first = #chunk_info{id = FirstChunkId,
                                                   pos = FilePos}}} ->
                    {ok, FirstChunkId, FilePos, File};
                {error, _} = Err ->
                    exit(Err)
            end
    end;
find_offset_reader_position(next, #{} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            {error, no_index_file};
        [LastIdxFile | _ ] ->
            case build_seg_info(LastIdxFile) of
                {ok, #seg_info{file = File,
                               last = LastChunk}} ->
                    {NextChunkId, FilePos} = next_location(LastChunk),
                    {ok, NextChunkId, FilePos, File};
                Err ->
                    exit(Err)
            end
    end;
find_offset_reader_position(last, #{name := Name} = Conf) ->
    case sorted_index_files_rev(Conf) of
        [] ->
            {error, no_index_file};
        IdxFiles ->
            case last_user_chunk_location(Name, IdxFiles) of
                not_found ->
                    ?DEBUG_(Name, "offset spec: 'last', user chunk not found, fall back to next", []),
                    %% no user chunks in stream, this is awkward, fall back to next
                    find_offset_reader_position(next, Conf);
                {ChunkId, FilePos, IdxFile} ->
                    File = segment_from_index_file(IdxFile),
                    {ok, ChunkId, FilePos, File}
            end
    end;
find_offset_reader_position(OffsetSpec, #{} = Conf)
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
                    %% TODO: shouldn't this cache the index files?
                    throw({retry_with, next, Conf});
                {end_of_log, #seg_info{file = SegmentFile,
                                       last = LastChunk}} ->
                    {ChunkId, FilePos} = next_location(LastChunk),
                    {ok, ChunkId, FilePos, SegmentFile};
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
                    {ok, ChunkId, FilePos, SegmentFile}
            end
    end.

open_offset_reader_at(SegmentFile, NextChunkId, FilePos,
                      #{dir := Dir,
                        name := Name,
                        shared := Shared,
                        readers_counter_fun := ReaderCounterFun,
                        options := Options} =
                      Conf) ->
    ReaderMod = application:get_env(osiris, log_reader, osiris_log_reader),
    {ok, Reader} = throw_missing(ReaderMod:open(SegmentFile)),
    Cnt = make_counter(Conf),
    ReaderCounterFun(1),
    FilterMatcher = case Options of
                        #{filter_spec := FilterSpec} ->
                            osiris_bloom:init_matcher(FilterSpec);
                        _ ->
                            undefined
                    end,
    {ok, #?MODULE{cfg = #cfg{directory = Dir,
                             counter = Cnt,
                             counter_id = counter_id(Conf),
                             name = Name,
                             readers_counter_fun = ReaderCounterFun,
                             shared = Shared
                            },
                  mode = #read{type = offset,
                               reader = {ReaderMod, Reader},
                               position = FilePos,
                               chunk_selector = maps:get(chunk_selector, Options,
                                                         user_data),
                               next_offset = NextChunkId,
                               transport = maps:get(transport, Options, tcp),
                               filter = FilterMatcher}}}.

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
    {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
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
read_header(#?MODULE{cfg = #cfg{}} = State0) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    %% TODO: skip non user chunks for offset readers
    case catch read_header0(State0) of
        {ok,
         #{num_records := NumRecords,
           next_position := NextPos} =
             Header,
         #?MODULE{mode = #read{next_offset = ChId} = Read} = State} ->
            %% skip data portion
            {ok, Header,
             State#?MODULE{mode = Read#read{next_offset = ChId + NumRecords,
                                            position = NextPos}}};
        {end_of_stream, _} = EOF ->
            EOF;
        {error, _} = Err ->
            Err
    end.

-record(iterator, {reader :: {module(), osiris_log_reader:state()},
                   next_offset :: offset(),
                   %% entries left
                   num_left :: non_neg_integer(),
                   %% any trailing data from last read
                   %% we try to capture at least the size of the next record
                   data :: undefined | binary(),
                   next_record_pos :: non_neg_integer()}).
-opaque chunk_iterator() :: #iterator{}.
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
    {ok, header_map(), chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(State) ->
    chunk_iterator(State, 1).

-spec chunk_iterator(state(), pos_integer() | all) ->
    {ok, header_map(), chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
chunk_iterator(#?MODULE{cfg = #cfg{},
                        mode = #read{type = RType,
                                     chunk_selector = Selector}
                       } = State0, CreditHint)
  when (is_integer(CreditHint) andalso CreditHint > 0) orelse
       is_atom(CreditHint) ->
    %% reads the next chunk of unparsed chunk data
    case catch read_header0(State0) of
        {ok,
         #{type := ChType,
           chunk_id := ChId,
           crc := Crc,
           num_entries := NumEntries,
           num_records := NumRecords,
           data_size := DataSize,
           filter_size := FilterSize,
           position := Pos,
           next_position := NextPos} = Header,
         #?MODULE{mode = #read{reader = {ReaderMod, Reader0},
                               next_offset = ChId} = Read} = State1} ->
            State = State1#?MODULE{mode = Read#read{next_offset = ChId + NumRecords,
                                                    position = NextPos}},
            case needs_handling(RType, Selector, ChType) of
                true ->
                    DataPos = Pos + ?HEADER_SIZE_B + FilterSize,
                    {Data, Reader} = iter_read_ahead(ReaderMod, Reader0,
                                                     DataPos, ChId, Crc,
                                                     CreditHint, DataSize,
                                                     NumEntries),
                    Iterator = #iterator{reader = {ReaderMod, Reader},
                                         data = Data,
                                         next_offset = ChId,
                                         num_left = NumEntries,
                                         next_record_pos = DataPos},
                    {ok, Header, Iterator, State};
                false ->
                    %% skip
                    chunk_iterator(State, CreditHint)
            end;
        Other ->
            Other
    end.

-spec iterator_next(chunk_iterator()) ->
    end_of_chunk | {offset_entry(), chunk_iterator()}.
iterator_next(#iterator{num_left = 0}) ->
    end_of_chunk;
iterator_next(#iterator{reader = {ReaderMod, Reader0},
                        next_offset = NextOffs,
                        num_left = Num,
                        data = ?REC_MATCH_SIMPLE(Len, Rem0),
                        next_record_pos = Pos} = I0) ->
    {Record, Rem, Reader} =
        case Rem0 of
            <<Record0:Len/binary, Rem1/binary>> ->
                {Record0, Rem1, Reader0};
            _ ->
                %% not enough in Rem0 to read the entire record
                %% so we need to read it from disk
                {ok, <<Record0:Len/binary, Rem1/binary>>, Reader1} =
                    ReaderMod:pread(Reader0, Pos + ?REC_HDR_SZ_SIMPLE_B,
                                    Len + ?ITER_READ_AHEAD_B, within),
                {Record0, Rem1, Reader1}
        end,

    I = I0#iterator{reader = {ReaderMod, Reader},
                    next_offset = NextOffs + 1,
                    num_left = Num - 1,
                    data = Rem,
                    next_record_pos = Pos + ?REC_HDR_SZ_SIMPLE_B + Len},
    {{NextOffs, Record}, I};
iterator_next(#iterator{reader = {ReaderMod, Reader0},
                        next_offset = NextOffs,
                        num_left = Num,
                        data = ?REC_MATCH_SUBBATCH(CompType, NumRecs,
                                                   UncompressedLen,
                                                   Len, Rem0),
                        next_record_pos = Pos} = I0) ->
    {Data, Rem, Reader} =
        case Rem0 of
            <<Record0:Len/binary, Rem1/binary>> ->
                {Record0, Rem1, Reader0};
            _ ->
                %% not enough in Rem0 to read the entire record
                %% so we need to read it from disk
                {ok, <<Record0:Len/binary, Rem1/binary>>, Reader1} =
                    ReaderMod:pread(Reader0, Pos + ?REC_HDR_SZ_SUBBATCH_B,
                                    Len + ?ITER_READ_AHEAD_B, within),
                {Record0, Rem1, Reader1}
        end,
    Record = {batch, NumRecs, CompType, UncompressedLen, Data},
    I = I0#iterator{reader = {ReaderMod, Reader},
                    next_offset = NextOffs + NumRecs,
                    num_left = Num - 1,
                    data = Rem,
                    next_record_pos = Pos + ?REC_HDR_SZ_SUBBATCH_B + Len},
    {{NextOffs, Record}, I};
iterator_next(#iterator{reader = {ReaderMod, Reader0},
                        next_record_pos = Pos} = I) ->
    {ok, Data, Reader} = ReaderMod:pread(Reader0, Pos, ?ITER_READ_AHEAD_B,
                                         within),
    iterator_next(I#iterator{reader = {ReaderMod, Reader},
                             data = Data}).

-spec read_chunk(state()) ->
    {ok, binary(), state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk(#?MODULE{cfg = #cfg{}} = State0) ->
    %% reads the next chunk of unparsed chunk data
    case catch read_header0(State0) of
        {ok,
         #{type := _ChType,
           chunk_id := ChId,
           epoch := _Epoch,
           crc := Crc,
           num_records := NumRecords,
           header_data := _HeaderData,
           data_size := DataSize,
           filter_size := FilterSize,
           position := Pos,
           next_position := NextPos,
           trailer_size := TrailerSize},
         #?MODULE{mode = #read{reader = {ReaderMod, Reader0},
                               next_offset = ChId} = Read} = State} ->
            ToRead = ?HEADER_SIZE_B + FilterSize + DataSize + TrailerSize,
            {ok, ChData, Reader} = ReaderMod:pread(Reader0, Pos, ToRead,
                                                   within),
            <<_:?HEADER_SIZE_B/binary,
              _:FilterSize/binary,
              RecordData:DataSize/binary,
              _/binary>> = ChData,
            validate_crc(ChId, Crc, RecordData),
            {ok, ChData,
             State#?MODULE{mode = Read#read{reader = {ReaderMod, Reader},
                                            next_offset = ChId + NumRecords,
                                            position = NextPos}}};
        Other ->
            Other
    end.

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
read_chunk_parsed(#?MODULE{mode = #read{}} = State0,
                  HeaderOrNot) ->
    %% the Header parameter isn't used anywhere in RabbitMQ so is ignored
    case chunk_iterator(State0, all) of
        {end_of_stream, _} = Eos ->
            Eos;
        {ok, _H, I0, State1} when HeaderOrNot == no_header ->
            Records = iter_all_records(iterator_next(I0), []),
            {Records, State1};
        {ok, Header, I0, State1} when HeaderOrNot == with_header ->
            Records = iter_all_records(iterator_next(I0), []),
            {ok, Header, Records, State1};
        Err ->
            Err
    end.

iter_all_records(end_of_chunk, Acc) ->
    lists:reverse(Acc);
iter_all_records({{ChId, {batch, _Num, 0, _Size, Data}}, I}, Acc0) ->
    %% TODO validate that sub batch is correct
    Acc = parse_subbatch(ChId, Data, Acc0),
    iter_all_records(iterator_next(I), Acc);
iter_all_records({X, I}, Acc0) ->
    Acc = [X | Acc0],
    iter_all_records(iterator_next(I), Acc).

is_valid_chunk_on_disk(SegFile, Pos) ->
    %% read a chunk from a specified location in the segment
    %% then checks the CRC
    case open(SegFile, [read, raw, binary]) of
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

-spec send_file(gen_tcp:socket() | ssl:socket(), state()) ->
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
          #?MODULE{mode = #read{type = RType,
                                chunk_selector = Selector,
                                transport = Transport}} = State0,
          Callback) ->
    case catch read_header0(State0) of
        {ok, #{type := ChType,
               chunk_id := ChId,
               num_records := NumRecords,
               filter_size := FilterSize,
               data_size := DataSize,
               trailer_size := TrailerSize,
               position := Pos,
               next_position := NextPos,
               header_data := HeaderData} = Header,
         #?MODULE{mode = #read{reader = {ReaderMod, Reader0},
                               next_offset = ChId} = Read0} = State1} ->
            %% read header
            %% used to write frame headers to socket
            %% and return the number of bytes to sendfile
            %% this allow users of this api to send all the data
            %% or just header and entry data
            {ToSkip, ToSend} =
                case RType of
                    offset ->
                        select_amount_to_send(Selector, ChType, FilterSize,
                                              DataSize, TrailerSize);
                    data ->
                        {0, FilterSize + DataSize + TrailerSize}
                end,

            Read1 = Read0#read{next_offset = ChId + NumRecords,
                               position = NextPos},
            %% only sendfile if either the reader is a data reader
            %% or the chunk is a user type (for offset readers)
            case needs_handling(RType, Selector, ChType) of
                true ->
                    _ = Callback(Header, ToSend + byte_size(HeaderData)),
                    case send(Transport, Sock, HeaderData) of
                        ok ->
                            case ReaderMod:sendfile(Transport, Reader0, Sock,
                                                    Pos + ?HEADER_SIZE_B + ToSkip,
                                                    ToSend) of
                                {ok, Reader} ->
                                    Read = Read1#read{reader = {ReaderMod,
                                                                Reader}},
                                    State = State1#?MODULE{mode = Read},
                                    {ok, State};
                                Err ->
                                    %% reset the position to the start of the current
                                    %% chunk so that subsequent reads won't error
                                    Err
                            end;
                        Err ->
                            Err
                    end;
                false ->
                    State = State1#?MODULE{mode = Read1},
                    %% skip chunk and recurse
                    send_file(Sock, State, Callback)
            end;
        Other ->
            Other
    end.

%% There could be many more selectors in the future
select_amount_to_send(user_data, ?CHNK_USER, FilterSize, DataSize, _TrailerSize) ->
    {FilterSize, DataSize};
select_amount_to_send(_, _, FilterSize, DataSize, TrailerSize) ->
    {FilterSize, DataSize + TrailerSize}.

needs_handling(data, _, _) ->
    true;
needs_handling(offset, all, _ChType) ->
    true;
needs_handling(offset, user_data, ?CHNK_USER) ->
    true;
needs_handling(_, _, _) ->
    false.

-spec close(state()) -> ok.
close(#?MODULE{cfg = #cfg{counter_id = CntId,
                          readers_counter_fun = Fun},
               fd = SegFd} = State) ->
    close_fd(SegFd),
    case State of
        #?MODULE{mode = #write{manifest = {ManifestMod, Manifest}}} ->
            ok = ManifestMod:close_manifest(Manifest);
        _ ->
            ok
    end,
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
    delete_dir(Dir),
    case application:get_env(osiris, log_manifest) of
        {ok, ManifestMod} ->
            ok = ManifestMod:delete(Config);
        undefined ->
            ok
    end;
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

%% Internal

next_chunk_pos(Fd, Pos) ->
    {ok, <<?MAGIC:4/unsigned,
           ?VERSION:4/unsigned,
           _ChType:8/unsigned,
           _NumEntries:16/unsigned,
           _Num:32/unsigned,
           _Timestamp:64/signed,
           _Epoch:64/unsigned,
           _Offset:64/unsigned,
           _Crc:32/integer,
           Size:32/unsigned,
           TSize:32/unsigned,
           FSize:8/unsigned,
           _Reserved:24>>} = file:pread(Fd, Pos, ?HEADER_SIZE_B),
    Pos + ?HEADER_SIZE_B + FSize + Size + TSize.


parse_subbatch(_Offs, <<>>, Acc) ->
    Acc;
parse_subbatch(Offs,
               <<0:1, %% simple
                 Len:31/unsigned,
                 Data:Len/binary,
                 Rem/binary>>,
               Acc) ->
    parse_subbatch(Offs + 1, Rem, [{Offs, Data} | Acc]).


sorted_index_files(#{index_files := IdxFiles}) ->
    %% cached
    IdxFiles;
sorted_index_files(#{dir := Dir}) ->
    sorted_index_files(Dir);
sorted_index_files(Dir) when ?IS_STRING(Dir) ->
    index_files(Dir, fun lists:sort/1).

sorted_index_files_rev(#{index_files := IdxFiles}) ->
    %% cached
    lists:reverse(IdxFiles);
sorted_index_files_rev(#{dir := Dir}) ->
    sorted_index_files_rev(Dir);
sorted_index_files_rev(Dir) ->
    index_files(Dir, fun (Files) ->
                             lists:sort(fun erlang:'>'/2, Files)
                     end).

index_files_unsorted(Dir) ->
    index_files(Dir, fun (X) -> X end).

index_files(Dir, SortFun) ->
    [filename:join(Dir, F)
     || <<_:20/binary, ".index">> = F <- SortFun(list_dir(Dir))].

orphaned_segments(Dir) ->
    orphaned_segments(lists:sort(list_dir(Dir)), []).

orphaned_segments([], Acc) ->
    Acc;
orphaned_segments([<<_:20/binary, ".index">>], Acc) ->
    Acc;
orphaned_segments([<<Name:20/binary, ".index">>,
                   <<Name:20/binary, ".segment">> | _Rem],
                  Acc) ->
    %% when we find a matching pair we can return
    Acc;
orphaned_segments([<<_:20/binary, ".segment">> = Dangler | Rem], Acc) ->
    orphaned_segments(Rem, [Dangler | Acc]);
orphaned_segments([_Unexpected | Rem], Acc) ->
    %% just ignore unexpected files
    orphaned_segments(Rem, Acc).

first_and_last_seginfos(#{index_files := IdxFiles}) ->
    first_and_last_seginfos0(IdxFiles);
first_and_last_seginfos(#{dir := Dir}) ->
    first_and_last_seginfos0(sorted_index_files(Dir)).

first_and_last_seginfos0([]) ->
    none;
first_and_last_seginfos0([FstIdxFile]) ->
    {ok, SegInfo} = build_seg_info(FstIdxFile),
    {index_file_offsets([FstIdxFile]), SegInfo, SegInfo};
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
                    {index_file_offsets(IdxFiles), FstSegInfo, LastSegInfo};
                {error, Err} ->
                    ?ERROR("~s: failed to build seg_info from file ~ts, error: ~w",
                           [?MODULE, LastIdxFile, Err]),
                    error(Err)
            end;
        {error, enoent} ->
            %% most likely retention race condition
            first_and_last_seginfos0(Rem)
    end.

index_file_offsets(IdxFiles) ->
    [begin
         <<Offset:20/binary, ".index">> = filename:basename(I),
         binary_to_integer(Offset)
     end || I <- IdxFiles].

build_seg_info(IdxFile) ->
    case last_valid_idx_record(IdxFile) of
        {ok, ?IDX_MATCH(_, _, LastChunkPos)} ->
            SegFile = segment_from_index_file(IdxFile),
            build_segment_info(SegFile, LastChunkPos, IdxFile);
        undefined ->
            %% this would happen if the file only contained a header
            SegFile = segment_from_index_file(IdxFile),
            {ok, #seg_info{file = SegFile, index = IdxFile}};
        {error, _} = Err ->
            Err
    end.

last_idx_record(IdxFd) ->
    nth_last_idx_record(IdxFd, 1).

nth_last_idx_record(IdxFile, N) when ?IS_STRING(IdxFile) ->
    {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
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

last_valid_idx_record(IdxFile) ->
    {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
    case position_at_idx_record_boundary(IdxFd, eof) of
        {ok, Pos} ->
            SegFile = segment_from_index_file(IdxFile),
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

first_idx_record(IdxFd) ->
    idx_read_at(IdxFd, ?IDX_HEADER_SIZE).

idx_read_at(Fd, Pos) when is_integer(Pos) ->
    case file:pread(Fd, Pos, ?INDEX_RECORD_SIZE_B) of
        {ok, ?ZERO_IDX_MATCH(_)} ->
            {error, empty_idx_record};
        Ret ->
            Ret
    end.

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

build_segment_info(SegFile, LastChunkPos, IdxFile) ->
    {ok, Fd} = open(SegFile, [read, binary, raw]),
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
            Range = offset_range_from_idx_files(IdxFiles),
            EpochOffsets = last_epoch_chunk_ids(<<>>, IdxFiles),
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

    {Time, Result} = timer:tc(
                       fun() ->
                               IdxFiles0 = sorted_index_files(Dir),
                               IdxFiles = evaluate_retention0(IdxFiles0, Specs),
                               OffsetRange = offset_range_from_idx_files(IdxFiles),
                               FirstTs = first_timestamp_from_index_files(IdxFiles),
                               {OffsetRange, FirstTs, length(IdxFiles)}
                       end),
    ?DEBUG_(<<>>," (~w) completed in ~fms", [Specs, Time/1_000]),
    Result.

evaluate_retention0(IdxFiles, []) ->
    IdxFiles;
evaluate_retention0(IdxFiles, [{max_bytes, MaxSize} | Specs]) ->
    RemIdxFiles = eval_max_bytes(IdxFiles, MaxSize),
    evaluate_retention0(RemIdxFiles, Specs);
evaluate_retention0(IdxFiles, [{max_age, Age} | Specs]) ->
    RemIdxFiles = eval_age(IdxFiles, Age),
    evaluate_retention0(RemIdxFiles, Specs).

eval_age([_] = IdxFiles, _Age) ->
    IdxFiles;
eval_age([IdxFile | IdxFiles] = AllIdxFiles, Age) ->
    case last_timestamp_in_index_file(IdxFile) of
        {ok, Ts} ->
            Now = erlang:system_time(millisecond),
            case Ts < Now - Age of
                true ->
                    %% the oldest timestamp is older than retention
                    %% and there are other segments available
                    %% we can delete
                    ok = delete_segment_from_index(IdxFile),
                    eval_age(IdxFiles, Age);
                false ->
                    AllIdxFiles
            end;
        _Err ->
            AllIdxFiles
    end;
eval_age([], _Age) ->
    %% this could happen if retention is evaluated whilst
    %% a stream is being deleted
    [].

eval_max_bytes([], _) -> [];
eval_max_bytes(IdxFiles, MaxSize) ->
    [Latest|Older] = lists:reverse(IdxFiles),
    eval_max_bytes(Older,
                   %% for retention eval it is ok to use a file size function
                   %% that implicitly return 0 when file is not found
                   MaxSize - file_size_or_zero(
                               segment_from_index_file(Latest)),
                   [Latest]).

eval_max_bytes([], _, Acc) ->
    Acc;
eval_max_bytes([IdxFile | Rest], Limit, Acc) ->
    SegFile = segment_from_index_file(IdxFile),
    Size = file_size(SegFile),
    case Size =< Limit of
        true ->
            eval_max_bytes(Rest, Limit - Size, [IdxFile | Acc]);
        false ->
            [ok = delete_segment_from_index(Seg) || Seg <- [IdxFile | Rest]],
            Acc
    end.

file_size(Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        {error, enoent} ->
            throw(missing_file)
    end.

file_size_or_zero(Path) ->
    case prim_file:read_file_info(Path) of
        {ok, #file_info{size = Size}} ->
            Size;
        {error, enoent} ->
            0
    end.

last_epoch_chunk_ids(Name, IdxFiles) ->
    T1 = erlang:monotonic_time(),
    %% no need to filter out empty index files as
    %% that will be done by last_epoch_chunk_ids0/2
    Return = last_epoch_chunk_ids0(IdxFiles, undefined),
    T2 = erlang:monotonic_time(),
    Time = erlang:convert_time_unit(T2 - T1, native, microsecond),
    ?DEBUG_(Name, " completed in ~bms", [Time div 1000]),
    Return.

last_epoch_chunk_ids0([], {LastE, LastO, Res}) ->
    lists:reverse([{LastE, LastO} | Res]);
last_epoch_chunk_ids0([], undefined) ->
    %% the empty stream
    [];
last_epoch_chunk_ids0([IdxFile | _] = Files, undefined) ->
    {ok, Fd} = open(IdxFile, [read, raw, binary]),
    case first_idx_record(Fd) of
        {ok, ?IDX_MATCH(FstChId, FstEpoch, _)} ->
            ok = file:close(Fd),
            last_epoch_chunk_ids0(Files, {FstEpoch, FstChId, []});
        _ ->
            ok = file:close(Fd),
            []
    end;
last_epoch_chunk_ids0([IdxFile | Rem], {PrevE, _PrevChId, EOs} = Acc0) ->
    %% TODO: make last_valid_idx_record/1 take a file handle
    case last_valid_idx_record(IdxFile) of
        {ok, ?IDX_MATCH(_LstChId, LstEpoch, _)}
          when LstEpoch > PrevE ->
            {ok, Fd} = open(IdxFile, [read, raw, binary]),
            Acc = idx_skip_search(Fd, ?IDX_HEADER_SIZE,
                                  fun leo_search_fun/3,
                                  Acc0),
            ok = file:close(Fd),
            last_epoch_chunk_ids0(Rem, Acc);
        {ok, ?IDX_MATCH(LstChId, LstEpoch, _)} ->
            %% no scan needed, just pass last epoch chunk id pair
            Acc = {LstEpoch, LstChId, EOs},
            last_epoch_chunk_ids0(Rem, Acc);
        undefined ->
            %% this means the index had a header but no entries
            %% we assume there are no further valid index files
            last_epoch_chunk_ids0([], Acc0);
        eof ->
            %% last index file must have been empty
            last_epoch_chunk_ids0([], Acc0)
    end.


leo_search_fun(_Type, ?IDX_MATCH(ChId, Epoch, _), {Epoch, _, Prev}) ->
    {continue, {Epoch, ChId, Prev}};
leo_search_fun(peek, ?IDX_MATCH(_ChId, Epoch, _), {CurEpoch, _, _} = Acc)
  when Epoch > CurEpoch ->
    {scan, Acc};
leo_search_fun(scan, ?IDX_MATCH(ChId, Epoch, _), {CurEpoch, CurChId, Prev})
  when Epoch > CurEpoch ->
    {continue, {Epoch, ChId, [{CurEpoch, CurChId} | Prev]}};
leo_search_fun(scan, ?IDX_MATCH(ChId, Epoch, _), undefined) ->
    {continue, {Epoch, ChId, []}};
leo_search_fun(_Type, _, Acc) ->
    %% invalid index record
    {continue, Acc}.

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
                     fd = Fd,
                     mode =
                         #write{segment_size = {SegSizeBytes, SegSizeChunks},
                                manifest = {ManifestMod, Manifest0},
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

            ChunkInfo = #chunk_info{id = Next, timestamp = Timestamp,
                                    epoch = Epoch, num = NumRecords,
                                    type = ChType, size = Size, pos = Cur},
            Event = {chunk_written, ChunkInfo, Chunk},
            Manifest = ManifestMod:handle_event(Event, Manifest0),
            osiris_log_shared:set_last_chunk_id(Shared, Next),
            %% update counters
            counters:put(CntRef, ?C_OFFSET, NextOffset - 1),
            counters:add(CntRef, ?C_CHUNKS, 1),
            maybe_set_first_offset(Next, Cfg),
            State#?MODULE{mode =
                          Write#write{tail_info = {NextOffset,
                                                   {Epoch, Next, Timestamp}},
                                      manifest = {ManifestMod, Manifest},
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

send(tcp, Sock, Data) ->
    gen_tcp:send(Sock, Data);
send(ssl, Sock, Data) ->
    ssl:send(Sock, Data).

last_timestamp_in_index_file(IdxFile) ->
    case file:open(IdxFile, [raw, binary, read]) of
        {ok, IdxFd} ->
            case last_idx_record(IdxFd) of
                {ok, <<_O:64/unsigned,
                       LastTimestamp:64/signed,
                       _E:64/unsigned,
                       _ChunkPos:32/unsigned,
                       _ChType:8/unsigned>>} ->
                    _ = file:close(IdxFd),
                    {ok, LastTimestamp};
                Err ->
                    _ = file:close(IdxFd),
                    Err
            end;
        Err ->
            Err
    end.

first_timestamp_from_index_files([IdxFile | _]) ->
    case file:open(IdxFile, [raw, binary, read]) of
        {ok, IdxFd} ->
            case first_idx_record(IdxFd) of
                {ok, <<_FstO:64/unsigned,
                       FstTimestamp:64/signed,
                       _FstE:64/unsigned,
                       _FstChunkPos:32/unsigned,
                       _FstChType:8/unsigned>>} ->
                    _ = file:close(IdxFd),
                    FstTimestamp;
                _ ->
                    _ = file:close(IdxFd),
                    0
            end;
        _Err ->
            0
    end;
first_timestamp_from_index_files([]) ->
    0.

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
    {ok, LstFd} = open(LstIdxFile, [read, raw, binary]),
    case position_at_idx_record_boundary(LstFd, eof) of
        {ok, Pos} ->
            case file:pread(LstFd, Pos - ?INDEX_RECORD_SIZE_B,
                            ?INDEX_RECORD_SIZE_B) of
                {ok, ?IDX_MATCH(LstChId, _, _)} ->
                    ok = file:close(LstFd),
                    {ok, FstFd} = open(FstIdxFile, [read, raw, binary]),
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

offset_range_from_chunk_range(empty) ->
    empty;
offset_range_from_chunk_range({undefined, undefined}) ->
    empty;
offset_range_from_chunk_range({#chunk_info{id = FirstChId},
                               #chunk_info{id = LastChId,
                                           num = LastNumRecs}}) ->
    {FirstChId, LastChId + LastNumRecs - 1}.

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

-spec can_read_next(state()) -> boolean().
can_read_next(#?MODULE{mode = #read{type = offset,
                                    next_offset = NextOffset},
                       cfg = #cfg{shared = Ref}}) ->
    osiris_log_shared:last_chunk_id(Ref) >= NextOffset andalso
    osiris_log_shared:committed_chunk_id(Ref) >= NextOffset;
can_read_next(#?MODULE{mode = #read{type = data,
                                    next_offset = NextOffset},
                       cfg = #cfg{shared = Ref}}) ->
    osiris_log_shared:last_chunk_id(Ref) >= NextOffset.

make_file_name(N, Suff) ->
    lists:flatten(
        io_lib:format("~20..0B.~s", [N, Suff])).

open_new_segment(#?MODULE{cfg = #cfg{name = Name,
                                     directory = Dir,
                                     counter = Cnt},
                          fd = OldFd,
                          current_file = OldFilename,
                          mode = #write{type = _WriterType,
                                        manifest = {ManifestMod, Manifest0},
                                        tail_info = {NextOffset, _}} = Write} =
                 State0) ->
    _ = close_fd(OldFd),
    Filename = make_file_name(NextOffset, "segment"),
    ?DEBUG_(Name, "~ts", [Filename]),
    {ok, Fd} =
        file:open(
            filename:join(Dir, Filename), ?FILE_OPTS_WRITE),
    ok = file:write(Fd, ?LOG_HEADER),
    %% we always move to the end of the file
    {ok, _} = file:position(Fd, eof),

    Event = {segment_opened, OldFilename, Filename},
    Manifest = ManifestMod:handle_event(Event, Manifest0),

    counters:add(Cnt, ?C_SEGMENTS, 1),

    State0#?MODULE{current_file = Filename,
                   fd = Fd,
                   %% reset segment_size counter
                   mode = Write#write{segment_size = {?LOG_HEADER_SIZE, 0},
                                      manifest = {ManifestMod, Manifest}}}.

open_index_read(File) ->
    {ok, Fd} = open(File, [read, raw, binary, read_ahead]),
    %% We can't use the assertion that index header is correct because of a
    %% race condition between opening the file and writing the header
    %% It seems to happen when retention policies are applied
    {ok, ?IDX_HEADER_SIZE} = file:position(Fd, ?IDX_HEADER_SIZE),
    Fd.

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
                             {ok, IdxFd} = open(IndexFile,
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


offset_search_fun(scan, ?IDX_MATCH(ChId, _Epoch, _Pos), {Offset, _} = State)
  when Offset < ChId ->
    {return, State};
offset_search_fun(peek, ?IDX_MATCH(ChId, _Epoch, _Pos), {Offset, _} = State)
  when Offset < ChId ->
    {scan, State};
offset_search_fun(_Type, ?IDX_MATCH(ChId, Epoch, Pos), {Offset, _}) ->
    {continue, {Offset, {ChId, Epoch, Pos}}}.

throw_missing({error, enoent}) ->
    throw(missing_file);
throw_missing(Any) ->
    Any.

open(File, Options) ->
    throw_missing(file:open(File, Options)).

chunk_location_for_timestamp(Idx, Ts) ->
    %% TODO: optimise using skip search approach
    Fd = open_index_read(Idx),
    %% scan index file for nearest timestamp
    {ChunkId, _Timestamp, _Epoch, FilePos} = timestamp_idx_scan(Fd, Ts),
    {ChunkId, FilePos}.

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

skip(0, L) ->
    L;
skip(Len, [B | L]) when Len > 0 ->
    S = byte_size(B),
    case Len < S of
        true ->
            [binary:part(B, {S, Len - S}) | L];
        false ->
            skip(Len - S, L)
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

read_header0(#?MODULE{cfg = #cfg{directory = Dir,
                                 shared = Shared,
                                 counter = CntRef},
                      mode = #read{reader = {ReaderMod, Reader0},
                                   next_offset = NextChId0,
                                   position = Pos,
                                   filter = Filter} = Read0,
                      current_file = CurFile} =
             State) ->
    %% reads the next header if permitted
    case can_read_next(State) of
        true ->
            %% optimistically read 64 bytes (small binary) as it may save us
            %% a syscall reading the filter if the filter is of the default
            %% 16 byte size
            case ReaderMod:pread(Reader0, Pos,
                                 ?HEADER_SIZE_B + ?DEFAULT_FILTER_SIZE,
                                 boundary) of
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
                       MaybeFilter/binary>> = HeaderData0, Reader1} ->
                    <<HeaderData:?HEADER_SIZE_B/binary, _/binary>> = HeaderData0,
                    counters:put(CntRef, ?C_OFFSET, NextChId0 + NumRecords),
                    counters:add(CntRef, ?C_CHUNKS, 1),
                    NextPos = Pos + ?HEADER_SIZE_B + FilterSize + DataSize + TrailerSize,

                    {ChunkFilter, Reader} =
                    case MaybeFilter of
                        <<F:FilterSize/binary, _/binary>> ->
                            %% filter is of default size or 0
                            {F, Reader1};
                        _  when Filter =/= undefined ->
                            %% the filter is larger than default
                            case ReaderMod:pread(Reader1,
                                                 Pos + ?HEADER_SIZE_B,
                                                 FilterSize, within) of
                                {ok, F, Reader2} ->
                                    {F, Reader2};
                                eof ->
                                    throw({end_of_stream, State})
                            end;
                        _ ->
                            {<<>>, Reader1}
                    end,

                    case osiris_bloom:is_match(ChunkFilter, Filter) of
                        true ->
                            {ok, #{chunk_id => NextChId0,
                                   epoch => Epoch,
                                   type => ChType,
                                   crc => Crc,
                                   num_records => NumRecords,
                                   num_entries => NumEntries,
                                   timestamp => Timestamp,
                                   data_size => DataSize,
                                   trailer_size => TrailerSize,
                                   header_data => HeaderData,
                                   filter_size => FilterSize,
                                   next_position => NextPos,
                                   position => Pos}, State};
                        false ->
                            Read = Read0#read{next_offset = NextChId0 + NumRecords,
                                              position = NextPos,
                                              reader = {ReaderMod, Reader}},
                            read_header0(State#?MODULE{mode = Read});
                        {retry_with, NewFilter} ->
                            Read = Read0#read{filter = NewFilter,
                                              reader = {ReaderMod, Reader}},
                            read_header0(State#?MODULE{mode = Read})
                    end;
                {ok, Bin} when byte_size(Bin) < ?HEADER_SIZE_B ->
                    %% partial header read
                    %% this can happen when a replica reader reads ahead
                    %% optimistically
                    %% treat as end_of_stream
                    {end_of_stream, State};
                eof ->
                    FirstOffset = osiris_log_shared:first_chunk_id(Shared),
                    %% open next segment file and start there if it exists
                    NextChId = max(FirstOffset, NextChId0),
                    %% TODO: replace this check with a last chunk id counter
                    %% updated by the writer and replicas
                    SegFile = make_file_name(NextChId, "segment"),
                    case SegFile == CurFile of
                        true ->
                            %% the new filename is the same as the old one
                            %% this should only really happen for an empty
                            %% log but would cause an infinite loop if it does
                            {end_of_stream, State};
                        false ->
                            ok = ReaderMod:close(Reader0),
                            case ReaderMod:open(filename:join(Dir, SegFile)) of
                                {ok, Reader} ->
                                    Read = Read0#read{reader = {ReaderMod,
                                                                Reader},
                                                      next_offset = NextChId,
                                                      position = ?LOG_HEADER_SIZE},
                                    read_header0(
                                      State#?MODULE{current_file = SegFile,
                                                    mode = Read});
                                {error, enoent} ->
                                    {end_of_stream, State}
                            end
                    end;
                {ok,
                 <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   _ChType:8/unsigned,
                   _NumEntries:16/unsigned,
                   _NumRecords:32/unsigned,
                   _Timestamp:64/signed,
                   _Epoch:64/unsigned,
                   UnexpectedChId:64/unsigned,
                   _Crc:32/integer,
                   _DataSize:32/unsigned,
                   _TrailerSize:32/unsigned,
                   _Reserved:32>>, _Reader} ->
                    %% TODO: we may need to return the new state here if
                    %% we've crossed segments
                    {error, {unexpected_chunk_id, UnexpectedChId, NextChId0}};
                Invalid ->
                    {error, {invalid_chunk_header, Invalid}}
            end;
        false ->
            {end_of_stream, State}
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

next_location(undefined) ->
    {0, ?LOG_HEADER_SIZE};
next_location(#chunk_info{id = Id,
                          num = Num,
                          pos = Pos,
                          size = Size}) ->
    {Id + Num, Pos + Size + ?HEADER_SIZE_B}.

index_file_first_offset(IdxFile) when is_list(IdxFile) ->
    list_to_integer(filename:basename(IdxFile, ".index"));
index_file_first_offset(IdxFile) when is_binary(IdxFile) ->
    binary_to_integer(filename:basename(IdxFile, <<".index">>)).

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

close_fd(undefined) ->
    ok;
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

iter_read_ahead(_ReaderMod, Reader, _Pos, _ChunkId, _Crc, 1, _DataSize,
                _NumEntries) ->
    %% no point reading ahead if there is only one entry to be read at this
    %% time
    {undefined, Reader};
iter_read_ahead(ReaderMod, Reader0, Pos, ChunkId, Crc, Credit, DataSize,
                NumEntries)
  when Credit == all orelse NumEntries == 1 ->
    {ok, Data, Reader} = ReaderMod:pread(Reader0, Pos, DataSize, within),
    validate_crc(ChunkId, Crc, Data),
    {Data, Reader};
iter_read_ahead(ReaderMod, Reader0, Pos, _ChunkId, _Crc, Credit0, DataSize,
                NumEntries) ->
    %% read ahead, assumes roughly equal entry sizes which may not be the case
    %% TODO round up to nearest block?
    %% We can only practically validate CRC if we read the whole data
    Credit = min(Credit0, NumEntries),
    Size = DataSize div NumEntries * Credit,
    {ok, Data, Reader} = ReaderMod:pread(Reader0, Pos,
                                         Size + ?ITER_READ_AHEAD_B,
                                         within),
    {Data, Reader}.

list_dir(Dir) ->
    case prim_file:list_dir(Dir) of
        {error, enoent} ->
            [];
        {ok, Files} ->
            [list_to_binary(F) || F <- Files]
    end.

handle_event({segment_opened, _OldSegment, NewSegment},
             #manifest{dir = Dir,
                       index_fd = Fd0} = Manifest) ->
    _ = close_fd(Fd0),
    IdxFilename = unicode:characters_to_list(
                    string:replace(NewSegment, ".segment", ".index",
                                   trailing)),
    {ok, Fd} =
        file:open(
            filename:join(Dir, IdxFilename), ?FILE_OPTS_WRITE),
    %% Write the header if this is a new index file.
    case file:position(Fd, eof) of
        {ok, 0} ->
            ok = file:write(Fd, ?IDX_HEADER);
        {ok, _} ->
            ok
    end,
    {ok, _} = file:position(Fd, eof),
    Manifest#manifest{index_fd = Fd};
handle_event({chunk_written, #chunk_info{id = Offset,
                                         timestamp = Timestamp,
                                         epoch = Epoch,
                                         pos = SegmentFilePos,
                                         type = ChType}, _Chunk},
             #manifest{index_fd = Fd} = Manifest) ->
    ok = file:write(Fd,
                    <<Offset:64/unsigned,
                      Timestamp:64/signed,
                      Epoch:64/unsigned,
                      SegmentFilePos:32/unsigned,
                      ChType:8/unsigned>>),
    Manifest.

close_manifest(#manifest{index_fd = Fd}) ->
    _ = close_fd(Fd),
    ok.

delete(_Config) ->
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").


part_test() ->
    [<<"ABCD">>] = part(4, [<<"ABCDEF">>]),
    [<<"AB">>, <<"CD">>] = part(4, [<<"AB">>, <<"CDEF">>]),
    [<<"AB">>, <<"CDEF">>] = part(6, [<<"AB">>, <<"CDEF">>]),
    ok.

skip_test() ->
    [<<"EF">>] = skip(4, [<<"ABCDEF">>]),
    [<<"B">>, <<"CDEF">>] = skip(1, [<<"AB">>, <<"CDEF">>]),
    [<<"CDEF">>] = skip(2, [<<"AB">>, <<"CDEF">>]),
    [<<"DEF">>] = skip(3, [<<"AB">>, <<"CDEF">>]),
    [<<"AB">>, <<"CDEF">>] = skip(0, [<<"AB">>, <<"CDEF">>]),
    ok.

-endif.
