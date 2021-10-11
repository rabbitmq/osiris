%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_log).

-include("osiris.hrl").

-export([init/1,
         init/2,
         init_acceptor/3,
         write/2,
         write/3,
         write/5,
         recover_tracking/1,
         accept_chunk/2,
         next_offset/1,
         first_offset/1,
         first_timestamp/1,
         tail_info/1,
         is_open/1,
         send_file/2,
         send_file/3,
         init_data_reader/2,
         init_offset_reader/2,
         read_header/1,
         read_chunk/1,
         read_chunk_parsed/1,
         read_chunk_parsed/2,
         committed_offset/1,
         get_current_epoch/1,
         get_directory/1,
         get_name/1,
         get_default_max_segment_size_bytes/0,
         counters_ref/1,
         close/1,
         overview/1,
         format_status/1,
         update_retention/2,
         evaluate_retention/2,
         directory/1,
         delete_directory/1]).

-define(IDX_VERSION, 1).
-define(LOG_VERSION, 1).
-define(IDX_HEADER, <<"OSII", ?IDX_VERSION:32/unsigned>>).
-define(LOG_HEADER, <<"OSIL", ?LOG_VERSION:32/unsigned>>).
-define(IDX_HEADER_SIZE, 8).
-define(LOG_HEADER_SIZE, 8).
% maximum size of a segment in bytes
-define(DEFAULT_MAX_SEGMENT_SIZE_B, 500 * 1000 * 1000).
% maximum number of chunks per segment
-define(DEFAULT_MAX_SEGMENT_SIZE_C, 256_000).
-define(INDEX_RECORD_SIZE_B, 29).
-define(C_OFFSET, 1).
-define(C_FIRST_OFFSET, 2).
-define(C_FIRST_TIMESTAMP, 3).
-define(C_CHUNKS, 4).
-define(C_SEGMENTS, 5).
-define(COUNTER_FIELDS,
        [
         {offset, ?C_OFFSET, counter, "The last offset (not chunk id) in the log for writers. The last offset read for readers"
         },
         {first_offset, ?C_FIRST_OFFSET, counter, "First offset, not updated for readers"},
         {first_timestamp, ?C_FIRST_TIMESTAMP, counter, "First timestamp, not updated for readers"},
         {chunks, ?C_CHUNKS, counter, "Number of chunks read or written, incremented even if a reader only reads the header"},
         {segments, ?C_SEGMENTS, counter, "Number of segments"}
        ]
       ).

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
%%   | Reserved                                                      |
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
    #{dir := file:filename(),
      epoch => non_neg_integer(),
      first_offset_fun => fun((integer()) -> ok),
      max_segment_size_bytes => non_neg_integer(),
      %% max number of writer ids to keep around
      tracking_config => osiris_tracking:config(),
      counter_spec => counter_spec(),
      %% used when initialising a log from an offset other than 0
      initial_offset => osiris:offset()}.
-type record() :: {offset(), osiris:data()}.
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
      header_data => binary(),
      position => non_neg_integer()}.
-type transport() :: tcp | ssl.

%% holds static or rarely changing fields
-record(cfg,
        {directory :: file:filename(),
         name :: string(),
         max_segment_size_bytes = ?DEFAULT_MAX_SEGMENT_SIZE_B :: non_neg_integer(),
         max_segment_size_chunks = ?DEFAULT_MAX_SEGMENT_SIZE_C :: non_neg_integer(),
         tracking_config = #{} :: osiris_tracking:config(),
         retention = [] :: [osiris:retention_spec()],
         counter :: counters:counters_ref(),
         counter_id :: term(),
         %% the maximum number of active writer deduplication sessions
         %% that will be included in snapshots written to new segments
         readers_counter_fun = fun(_) -> ok end :: function(),
         first_offset_fun :: fun ((integer()) -> ok)}).
-record(read,
        {type :: data | offset,
         offset_ref :: undefined | atomics:atomics_ref(),
         last_offset = 0 :: offset(),
         next_offset = 0 :: offset(),
         transport :: transport(),
         chunk_selector :: all | user_data}).
-record(write,
        {type = writer :: writer | acceptor,
         segment_size = {0, 0} :: {non_neg_integer(), non_neg_integer()},
         current_epoch :: non_neg_integer(),
         tail_info = {0, empty} :: osiris:tail_info()
        }).
-record(?MODULE,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename(),
         fd :: undefined | file:io_device(),
         index_fd :: undefined | file:io_device()}).
%% record chunk_info does not map exactly to an index record (field 'num' differs)
-record(chunk_info,
        {id :: offset(),
         timestamp :: non_neg_integer(),
         epoch :: epoch(),
         num :: non_neg_integer(),
         type :: chunk_type(),
         %% size of data + trailer
         size :: non_neg_integer(),
         %% position in segment file
         pos :: integer()
        }).
-record(seg_info,
        {file :: file:filename(),
         size = 0 :: non_neg_integer(),
         index :: file:filename(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).

-opaque state() :: #?MODULE{}.

-export_type([state/0,
              range/0,
              config/0,
              counter_spec/0]).

-spec directory(osiris:config() | list()) -> file:filename().
directory(#{name := Name, dir := Dir}) ->
    filename:join(Dir, Name);
directory(#{name := Name}) ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    filename:join(Dir, Name);
directory(Name) when is_list(Name) ->
    {ok, Dir} = application:get_env(osiris, data_dir),
    filename:join(Dir, Name).

-spec init(config()) -> state().
init(Config) ->
    init(Config, writer).

-spec init(config(), writer | acceptor) -> state().
init(#{dir := Dir,
       name := Name,
       epoch := Epoch} =
         Config,
     WriterType) ->
    %% scan directory for segments if in write mode
    MaxSizeBytes =
        maps:get(max_segment_size_bytes, Config, ?DEFAULT_MAX_SEGMENT_SIZE_B),
    MaxSizeChunks = application:get_env(osiris, max_segment_size_chunks, ?DEFAULT_MAX_SEGMENT_SIZE_C),
    Retention = maps:get(retention, Config, []),
    ?DEBUG("osiris_log:init/1 max_segment_size_bytes: ~b, max_segment_size_chunks ~b, retention ~w",
          [MaxSizeBytes, MaxSizeChunks, Retention]),
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
    FirstOffsetFun = maps:get(first_offset_fun, Config, fun (_) -> ok end),
    Cfg = #cfg{directory = Dir,
               name = Name,
               max_segment_size_bytes = MaxSizeBytes,
               max_segment_size_chunks = MaxSizeChunks,
               tracking_config = maps:get(tracking_config, Config, #{}),
               retention = Retention,
               counter = Cnt,
               counter_id = counter_id(Config),
               first_offset_fun = FirstOffsetFun},
    case lists:reverse(build_log_overview(Dir)) of
        [] ->
            NextOffset = case Config of
                             #{initial_offset := IO}
                               when WriterType == acceptor ->
                                 IO;
                             _ ->
                                 0
                         end,
            open_new_segment(#?MODULE{cfg = Cfg,
                                      mode =
                                          #write{type = WriterType,
                                                 tail_info = {NextOffset, empty},
                                                 current_epoch = Epoch}});
        [#seg_info{file = Filename,
                   index = IdxFilename,
                   size = Size,
                   last =
                       #chunk_info{epoch = LastEpoch,
                                   timestamp = LastTs,
                                   id = LastChId,
                                   num = LastNum}}
         | _] = Infos ->
            [#seg_info{first = #chunk_info{id = FstChId,
                                           timestamp = FstTs}} | _] =
            lists:reverse(Infos),
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
            counters:put(Cnt, ?C_SEGMENTS, length(Infos)),
            ?DEBUG("~s:~s/~b: next offset ~b first offset ~b",
                   [?MODULE,
                    ?FUNCTION_NAME,
                    ?FUNCTION_ARITY,
                    element(1, TailInfo),
                    FstChId]),
            {ok, Fd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            {ok, Size} = file:position(Fd, Size),
            ok = file:truncate(Fd),
            {ok, _} = file:position(IdxFd, eof),
            {ok, Size} = file:position(Fd, Size),
            %% truncate segment to size in case there is trailing data
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = TailInfo,
                                current_epoch = Epoch},
                     fd = Fd,
                     index_fd = IdxFd};
        [#seg_info{file = Filename,
                   index = IdxFilename,
                   last = undefined}
         | _] ->
            %% the empty log case
            {ok, Fd} = open(Filename, ?FILE_OPTS_WRITE),
            {ok, IdxFd} = open(IdxFilename, ?FILE_OPTS_WRITE),
            %% TODO: do we potentially need to truncate the segment
            %% here too?
            {ok, _} = file:position(Fd, eof),
            {ok, _} = file:position(IdxFd, eof),
            #?MODULE{cfg = Cfg,
                     mode =
                         #write{type = WriterType,
                                tail_info = {0, empty},
                                current_epoch = Epoch},
                     fd = Fd,
                     index_fd = IdxFd}
    end.

-spec write([osiris:data()], state()) -> state().
write(Entries, State) when is_list(Entries) ->
    Timestamp = erlang:system_time(millisecond),
    write(Entries, ?CHNK_USER, Timestamp, <<>>, State).

-spec write([osiris:data()], integer(), state()) -> state().
write(Entries, Now, #?MODULE{mode = #write{}} = State)
    when is_integer(Now) ->
    write(Entries, ?CHNK_USER, Now, <<>>, State).

-spec write([osiris:data()],
            chunk_type(),
            osiris:timestamp(),
            iodata(),
            state()) ->
               state().
write([_ | _] = Entries,
      ChType,
      Now,
      Trailer,
      #?MODULE{cfg = #cfg{},
               fd = undefined,
               mode = #write{}} =
          State0) ->
    %% we need to open a new segment here to ensure tracking chunk
    %% is made before the one that triggers the new segment to be created
    trigger_retention_eval(
      write(Entries, ChType, Now, Trailer, open_new_segment(State0)));
write([_ | _] = Entries,
      ChType,
      Now,
      Trailer,
      #?MODULE{cfg = #cfg{},
               mode =
                   #write{current_epoch = Epoch, tail_info = {Next, _}} =
                       _Write0} =
          State0)
    when is_integer(Now)
         andalso is_integer(ChType) ->
    %% The osiris writer always pass Entries in the reversed order
    %% in order to avoid unnecessary lists rev|trav|ersals
    {ChunkData, NumRecords} =
        make_chunk(Entries, Trailer, ChType, Now, Epoch, Next),
    write_chunk(ChunkData, ChType, Now, Epoch, NumRecords, State0);
write([], _ChType, _Now, _Writers, State) ->
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
                _Reserved:32,
                Data/binary>>
              | DataParts] =
                 Chunk,
             #?MODULE{cfg = #cfg{}, mode = #write{tail_info = {Next, _}}} =
                 State0) ->
    DataAndTrailer = [Data | DataParts],
    validate_crc(Next, Crc, part(DataSize, DataAndTrailer)),
    %% assertion
    % true = iolist_size(DataAndTrailer) == (DataSize + TrailerSize),
    %% acceptors do no need to maintain writer state in memory so we pass
    %% the empty map here instead of parsing the trailer
    case write_chunk(Chunk, ChType, Timestamp, Epoch, NumRecords, State0) of
        full ->
            trigger_retention_eval(
              accept_chunk(Chunk, open_new_segment(State0)));
        State ->
            State
    end;
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

-spec is_open(state()) -> boolean().
is_open(#?MODULE{mode = #write{}, fd = Fd}) ->
    Fd =/= undefined.

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
    SegInfos = build_log_overview(Dir),
    ?DEBUG("~s: ~s from epoch offsets: ~w range ~w",
           [?MODULE, ?FUNCTION_NAME, EpochOffsets, Range]),
    ok = truncate_to(Name, Range, EpochOffsets, SegInfos),
    %% after truncation we can do normal init
    InitOffset = case Range  of
                     empty -> 0;
                     {O, _} -> O
                 end,
    init(Conf#{initial_offset => InitOffset}, acceptor).

chunk_id_index_scan(IdxFile, ChunkId) when is_list(IdxFile) ->
    Fd = open_index_read(IdxFile),
    chunk_id_index_scan0(Fd, ChunkId).

chunk_id_index_scan0(Fd, ChunkId) ->
    {ok, IdxPos} = file:position(Fd, cur),
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok,
         <<ChunkId:64/unsigned,
           _Timestamp:64/signed,
           Epoch:64/unsigned,
           FilePos:32/unsigned,
           _ChType:8/unsigned>>} ->
            ok = file:close(Fd),
            {ChunkId, Epoch, FilePos, IdxPos};
        {ok, _} ->
            chunk_id_index_scan0(Fd, ChunkId);
        eof ->
            ok = file:close(Fd),
            eof
    end.

delete_segment(#seg_info{file = File, index = Index}) ->
    ?DEBUG("osiris_log: deleting segment ~s in ~s",
           [filename:basename(File), filename:dirname(File)]),
    ok = file:delete(File),
    ok = file:delete(Index),
    ok.

truncate_to(_Name, _Range, _EpochOffsets, []) ->
    %% the target log is empty
    ok;
truncate_to(_Name, _Range, [], SegInfos) ->
    %% ?????  this means the entire log is out
    [begin ok = delete_segment(I) end || I <- SegInfos],
    ok;
truncate_to(Name, Range, [{E, ChId} | NextEOs], SegInfos) ->
    case find_segment_for_offset(ChId, SegInfos) of
        not_found ->
            case lists:last(SegInfos) of
                #seg_info{last = #chunk_info{epoch = E,
                                             id = LastChId,
                                             num = Num}}
                when ChId > LastChId + Num ->
                    %% the last available local chunk id is smaller than the
                    %% sources last chunk id but is in the same epoch
                    %% check if there is any overlap
                    LastOffsLocal = case offset_range_from_segment_infos(SegInfos) of
                                        empty -> 0;
                                        {_, L} -> L
                                    end,
                    FstOffsetRemote = case Range of
                                          empty -> 0;
                                          {F, _} -> F
                                      end,
                    case LastOffsLocal < FstOffsetRemote of
                        true ->
                            %% there is no overlap, need to delete all
                            %% local segments
                            [begin ok = delete_segment(I) end || I <- SegInfos],
                            ok;
                        false ->
                            %% there is overlap
                            %% no truncation needed
                            ok
                    end;
                _ ->
                    truncate_to(Name, Range, NextEOs, SegInfos)
            end;
        {end_of_log, _Info} ->
            ok;
        {found, #seg_info{file = File, index = Idx}} ->
            ?DEBUG("osiris_log: ~s on node ~s truncating to chunk "
                   "id ~b in epoch ~b",
                   [Name, node(), ChId, E]),
            %% this is the inclusive case
            %% next offset needs to be a chunk offset
            %% if it is not found we know the offset requested isn't a chunk
            %% id and thus isn't valid
            case chunk_id_index_scan(Idx, ChId) of
                {ChId, E, Pos, IdxPos} when is_integer(Pos) ->
                    %% the  Chunk id was found and has the right epoch
                    %% lets truncate to this point
                    %% FilePos could be eof here which means the next offset
                    {ok, Fd} = file:open(File, [read, write, binary, raw]),
                    {ok, IdxFd} = file:open(Idx, [read, write, binary, raw]),

                    {_ChType, ChId, E, _Num, Size, TSize} =
                        header_info(Fd, Pos),
                    %% position at end of chunk
                    {ok, _Pos} = file:position(Fd, {cur, Size + TSize}),
                    ok = file:truncate(Fd),

                    {ok, _} =
                        file:position(IdxFd, IdxPos + ?INDEX_RECORD_SIZE_B),
                    ok = file:truncate(IdxFd),
                    ok = file:close(Fd),
                    ok = file:close(IdxFd),
                    %% delete all segments with a first offset larger then ChId
                    [begin ok = delete_segment(I) end
                     || I <- SegInfos, I#seg_info.first#chunk_info.id > ChId],
                    ok;
                _ ->
                    truncate_to(Name, Range, NextEOs, SegInfos)
            end
    end.

-spec init_data_reader(osiris:tail_info(), config()) ->
                          {ok, state()} |
                          {error,
                           {offset_out_of_range,
                            empty | {offset(), offset()}}} |
                          {error,
                           {invalid_last_offset_epoch, offset(), offset()}}.
init_data_reader({StartChunkId, PrevEOT}, #{dir := Dir} = Config) ->
    SegInfos = build_log_overview(Dir),
    Range = offset_range_from_segment_infos(SegInfos),
    ?DEBUG("osiris_segment:init_data_reader/2 at ~b prev "
           "~w local range: ~w",
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
            {ok, init_data_reader_from(
                   StartChunkId,
                   find_segment_for_offset(StartChunkId, SegInfos),
                   Config)};
        _ ->
            {PrevEpoch, PrevChunkId, _PrevTs} = PrevEOT,
            case check_chunk_has_expected_epoch(PrevChunkId, PrevEpoch, SegInfos) of
                ok ->
                    {ok, init_data_reader_from(
                           StartChunkId,
                           find_segment_for_offset(StartChunkId, SegInfos),
                           Config)};
                {error, _} = Err ->
                    Err
            end
    end.

check_chunk_has_expected_epoch(ChunkId, Epoch, SegInfos) ->
    case find_segment_for_offset(ChunkId, SegInfos) of
        not_found ->
            %% this is unexpected and thus an error
            {error,
             {invalid_last_offset_epoch, Epoch, unknown}};
        {found, SegmentInfo = #seg_info{file = _PrevSeg}} ->
            %% prev segment exists, does it have the correct
            %% epoch?
            case scan_idx(ChunkId, SegmentInfo) of
                {ChunkId, Epoch, _PrevPos} ->
                    ok;
                {ChunkId, OtherEpoch, _} ->
                    {error,
                     {invalid_last_offset_epoch, Epoch, OtherEpoch}}
            end
    end.

init_data_reader_at(ChunkId, FilePos, File,
                    #{dir := Dir, name := Name,
                      readers_counter_fun := CountersFun} = Config) ->
    {ok, Fd} = file:open(File, [raw, binary, read]),
    {ok, FilePos} = file:position(Fd, FilePos),
    Cnt = make_counter(Config),
    counters:put(Cnt, ?C_OFFSET, ChunkId - 1),
    CountersFun(1),
    #?MODULE{cfg =
                 #cfg{directory = Dir,
                      counter = Cnt,
                      name = Name,
                      readers_counter_fun = CountersFun,
                      first_offset_fun = fun (_) -> ok end},
             mode =
                 #read{type = data,
                       offset_ref = maps:get(offset_ref, Config, undefined),
                       next_offset = ChunkId,
                       chunk_selector = all,
                       transport = maps:get(transport, Config, tcp)},
             fd = Fd}.

init_data_reader_from(ChunkId,
                      {end_of_log, #seg_info{file = File,
                                             last = LastChunk}},
                      Config) ->
    {ChunkId, AttachPos} = next_location(LastChunk),
    init_data_reader_at(ChunkId, AttachPos, File, Config);
init_data_reader_from(ChunkId,
                      {found, #seg_info{file = File} = SegInfo},
                      Config) ->
    {ChunkId, _Epoch, FilePos} = scan_idx(ChunkId, SegInfo),
    init_data_reader_at(ChunkId, FilePos, File, Config).

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
                            {error, {invalid_chunk_header, term()}}.
init_offset_reader(OffsetSpec, Conf) ->
    try
        init_offset_reader0(OffsetSpec, Conf)
    catch
        missing_file ->
            init_offset_reader0(OffsetSpec, Conf)
    end.

init_offset_reader0({abs, Offs}, #{dir := Dir} = Conf) ->
    Range = offset_range_from_segment_infos(build_log_overview(Dir)),
    case Range of
        empty ->
            {error, {offset_out_of_range, Range}};
        {S, E} when Offs < S orelse Offs > E ->
            {error, {offset_out_of_range, Range}};
        _ ->
            %% it is in range, convert to standard offset
            init_offset_reader0(Offs, Conf)
    end;
init_offset_reader0({timestamp, Ts}, #{dir := Dir} = Conf) ->
    case build_log_overview(Dir) of
        [] ->
            init_offset_reader0(next, Conf);
        [#seg_info{file = SegmentFile,
                   first = #chunk_info{timestamp = Fst,
                                       pos = FilePos,
                                       id = ChunkId}} | _]
            when is_integer(Fst) andalso Fst > Ts ->
            %% timestamp is lower than the first timestamp available
            open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
        SegInfos ->
            case lists:search(fun (#seg_info{first = #chunk_info{timestamp = F},
                                             last = #chunk_info{timestamp = L}})
                                      when is_integer(F)
                                           andalso is_integer(L) ->
                                      Ts >= F andalso Ts =< L;
                                  (_) ->
                                      false
                              end,
                              SegInfos)
            of
                {value, #seg_info{file = SegmentFile} = Info} ->
                    %% segment was found, now we need to scan index to
                    %% find nearest offset
                    {ChunkId, FilePos} = chunk_location_for_timestamp(Info, Ts),
                    open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
                false ->
                    %% segment was not found, attach next
                    %% this should be rare so no need to call the more optimal
                    %% open_offset_reader_at/4 function
                    init_offset_reader0(next, Conf)
            end
    end;
init_offset_reader0(first, #{dir := Dir} = Conf) ->
    case build_log_overview(Dir) of
        [#seg_info{file = File,
                   first = undefined}] ->
            %% empty log, attach at 0
            open_offset_reader_at(File, 0, ?LOG_HEADER_SIZE, Conf);
        [#seg_info{file = File,
                   first = #chunk_info{id = FirstChunkId,
                                       pos = FilePos}} | _] ->
            open_offset_reader_at(File, FirstChunkId, FilePos, Conf);
        _ ->
            exit(no_segments_found)
    end;
init_offset_reader0(next, #{dir := Dir} = Conf) ->
    SegInfos = build_log_overview(Dir),
    case lists:reverse(SegInfos) of
        [#seg_info{file = File,
                   last = LastChunk} | _] ->
            {NextChunkId, FilePos} = next_location(LastChunk),
            open_offset_reader_at(File, NextChunkId, FilePos, Conf);
        _ ->
            exit(no_segments_found)
    end;
init_offset_reader0(last, #{dir := Dir} = Conf) ->
    SegInfos = build_log_overview(Dir),
    case lists:reverse(SegInfos) of
        [#seg_info{file = File,
                   last = undefined}] ->
            %% empty log, attach at 0
            open_offset_reader_at(File, 0, ?LOG_HEADER_SIZE, Conf);
        [#seg_info{file = File,
                   last = #chunk_info{type = ?CHNK_USER,
                                      id = LastChunkId,
                                      pos = FilePos}} | _] ->
            open_offset_reader_at(File, LastChunkId, FilePos, Conf);
        _ ->
            case last_user_chunk_location(SegInfos) of
                not_found ->
                    ?DEBUG("~s:~s use chunk not found, fall back to next",
                           [?MODULE, ?FUNCTION_NAME]),
                    %% no user chunks in stream, this is awkward, fall back to next
                    init_offset_reader0(next, Conf);
                {ChunkId, FilePos, #seg_info{file = File}} ->
                    open_offset_reader_at(File, ChunkId, FilePos, Conf)
            end
    end;
init_offset_reader0(OffsetSpec, #{dir := Dir} = Conf)
  when is_integer(OffsetSpec) ->
    SegInfos = build_log_overview(Dir),
    ChunkRange = chunk_range_from_segment_infos(SegInfos),
    Range = offset_range_from_chunk_range(ChunkRange),
    ?DEBUG("osiris_log:init_offset_reader0/2 spec ~w range ~w ",
           [OffsetSpec, Range]),
    try
        StartOffset =
            case {OffsetSpec, Range} of
                {_, empty} ->
                    0;
                {Offset, {_, LastOffs}}
                  when Offset > LastOffs ->
                    %% out of range, clamp as `next`
                    throw({retry_with, next, Conf});
                {Offset, {FirstOffs, _LastOffs}} ->
                    max(FirstOffs, Offset)
            end,
        %% find the appopriate segment and scan the index to find the
        %% postition of the next chunk to read
        case find_segment_for_offset(StartOffset, SegInfos) of
            not_found ->
                {error, {offset_out_of_range, Range}};
            {end_of_log, #seg_info{file = SegmentFile,
                                   last = LastChunk}} ->
                {ChunkId, FilePos} = next_location(LastChunk),
                open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf);
            {found, SegmentInfo = #seg_info{file = SegmentFile}} ->
                {ChunkId, _Epoch, FilePos} =
                    case scan_idx(StartOffset, SegmentInfo) of
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
                ?DEBUG("osiris_log:init_offset_reader0/2 resolved chunk_id ~b"
                       " at file pos: ~w ", [ChunkId, FilePos]),
                open_offset_reader_at(SegmentFile, ChunkId, FilePos, Conf)
        end
    catch
        missing_file ->
            %% Retention policies are likely being applied, let's try again
            %% TODO: should we limit the number of retries?
            init_offset_reader0(OffsetSpec, Conf);
        {retry_with, NewOffsSpec, NewConf} ->
            init_offset_reader0(NewOffsSpec, NewConf)
    end.

open_offset_reader_at(SegmentFile, NextChunkId, FilePos,
                      #{dir := Dir,
                        name := Name,
                        offset_ref := OffsetRef,
                        readers_counter_fun := ReaderCounterFun,
                        options := Options} =
                      Conf) ->
    {ok, Fd} = open(SegmentFile, [raw, binary, read]),
    {ok, FilePos} = file:position(Fd, FilePos),
    Cnt = make_counter(Conf),
    ReaderCounterFun(1),
    {ok, #?MODULE{cfg = #cfg{directory = Dir,
                             counter = Cnt,
                             name = Name,
                             readers_counter_fun = ReaderCounterFun,
                             first_offset_fun = fun (_) -> ok end
                            },
                  mode = #read{type = offset,
                               chunk_selector = maps:get(chunk_selector, Options,
                                                         user_data),
                               offset_ref = OffsetRef,
                               next_offset = NextChunkId,
                               transport = maps:get(transport, Options, tcp)},
                  fd = Fd}}.

%% Searches the index files backwards for the ID of the last user chunk.
last_user_chunk_location(SegInfos) when is_list(SegInfos) ->
    {Time, Result} = timer:tc(
                       fun() ->
                               last_user_chunk_id0(lists:reverse(SegInfos))
                       end),
    ?DEBUG("~s:~s/~b completed in ~fs", [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Time/1_000_000]),
    Result.

last_user_chunk_id0([]) ->
    %% There are no user chunks in any index files.
    not_found;
last_user_chunk_id0([#seg_info{index = IdxFile} = Info | Rest]) ->
    try
        %% Do not read-ahead since we read the index file backwards chunk by chunk.
        {ok, IdxFd} = open(IdxFile, [read, raw, binary]),
        file:position(IdxFd, eof),
        Last = last_user_chunk_id_in_index(IdxFd),
        _ = file:close(IdxFd),
        case Last of
            {ok, Id, Pos} ->
                {Id, Pos, Info};
            {error, Reason} ->
                ?DEBUG("Could not find user chunk in index file ~s (~p)", [IdxFile, Reason]),
                last_user_chunk_id0(Rest)
        end
    catch
        missing_file ->
            ?DEBUG("Index file does not exist: ~s", [IdxFile]),
            last_user_chunk_id0(Rest)
    end.

%% Searches the index file backwards for the ID of the last user chunk.
last_user_chunk_id_in_index(IdxFd) ->
    case file:position(IdxFd, {cur, -2*?INDEX_RECORD_SIZE_B}) of
        {error, _} = Error ->
            Error;
        {ok, _NewPos} ->
            case file:read(IdxFd, ?INDEX_RECORD_SIZE_B) of
                {ok,
                 <<Offset:64/unsigned,
                   _Timestamp:64/signed,
                   _Epoch:64/unsigned,
                   FilePos:32/unsigned,
                   ?CHNK_USER:8/unsigned>>} ->
                    {ok, Offset, FilePos};
                {ok,
                 <<_Offset:64/unsigned,
                   _Timestamp:64/signed,
                   _Epoch:64/unsigned,
                   _FilePos:32/unsigned,
                   _ChType:8/unsigned>>} ->
                    last_user_chunk_id_in_index(IdxFd);
                {error, _} = Error ->
                    Error
            end
    end.

-spec committed_offset(state()) -> undefined | offset().
committed_offset(#?MODULE{mode = #read{offset_ref = undefined}}) ->
    undefined;
committed_offset(#?MODULE{mode = #read{offset_ref = Ref}}) ->
    atomics:get(Ref, 1).

-spec get_current_epoch(state()) -> non_neg_integer().
get_current_epoch(#?MODULE{mode = #write{current_epoch = Epoch}}) ->
    Epoch.

-spec get_directory(state()) -> file:filename().
get_directory(#?MODULE{cfg = #cfg{directory = Dir}}) ->
    Dir.

-spec get_name(state()) -> string().
get_name(#?MODULE{cfg = #cfg{name = Name}}) ->
    Name.

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
    case read_header0(State0) of
        {ok,
         #{num_records := NumRecords,
           data_size := DataSize,
           trailer_size := TrailerSize} =
             Header,
         #?MODULE{mode = #read{} = Read, fd = Fd} = State} ->
            %% skip data portion
            {ok, _} = file:position(Fd, {cur, DataSize + TrailerSize}),
            {ok, Header,
             State#?MODULE{mode = incr_next_offset(NumRecords, Read)}};
        {end_of_stream, _} = EOF ->
            EOF;
        {error, _} = Err ->
            Err
    end.

-spec read_chunk(state()) ->
                    {ok,
                     {chunk_type(),
                      offset(),
                      epoch(),
                      HeaderData :: iodata(),
                      RecordData :: iodata(),
                      TrailerData :: iodata()},
                     state()} |
                    {end_of_stream, state()} |
                    {error, {invalid_chunk_header, term()}}.
read_chunk(#?MODULE{cfg = #cfg{}} = State0) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    case read_header0(State0) of
        {ok,
         #{type := ChType,
           chunk_id := ChId,
           epoch := Epoch,
           crc := Crc,
           num_records := NumRecords,
           header_data := HeaderData,
           data_size := DataSize,
           trailer_size := TrailerSize},
         #?MODULE{fd = Fd, mode = #read{next_offset = ChId} = Read} = State} ->
            {ok, BlobData} = file:read(Fd, DataSize),
            {ok, TrailerData} = file:read(Fd, TrailerSize),
            validate_crc(ChId, Crc, BlobData),
            {ok, {ChType, ChId, Epoch, HeaderData, BlobData, TrailerData},
             State#?MODULE{mode = incr_next_offset(NumRecords, Read)}};
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
read_chunk_parsed(#?MODULE{mode = #read{type = RType,
                                        chunk_selector = Selector}} = State0,
                 HeaderOrNot) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    case read_header0(State0) of
        {ok,
         #{type := ChType,
           chunk_id := ChId,
           crc := Crc,
           num_records := NumRecords,
           data_size := DataSize,
           trailer_size := TrailerSize} = Header,
         #?MODULE{fd = Fd, mode = #read{next_offset = _ChId} = Read} = State1} ->
            {ok, Data} = file:read(Fd, DataSize),
            {ok, _TrailerData} = file:read(Fd, TrailerSize),
            validate_crc(ChId, Crc, Data),
            State = State1#?MODULE{mode = incr_next_offset(NumRecords, Read)},
            case needs_handling(RType, Selector, ChType) of
                true when HeaderOrNot == no_header ->
                    %% parse data into records
                    {parse_records(ChId, Data, []), State};
                true ->
                    {ok, Header, parse_records(ChId, Data, []), State};
                false ->
                    %% skip
                    read_chunk_parsed(State, HeaderOrNot)
            end;
        Ret ->
            Ret
    end.

-spec send_file(gen_tcp:socket(), state()) ->
                   {ok, state()} |
                   {error, term()} |
                   {end_of_stream, state()}.
send_file(Sock, State) ->
    send_file(Sock, State, fun(_, S) -> S end).

-spec send_file(gen_tcp:socket(), state(),
                fun((header_map(), non_neg_integer()) -> term())) ->
                   {ok, state()} |
                   {error, term()} |
                   {end_of_stream, state()}.
send_file(Sock,
          #?MODULE{cfg = #cfg{}, mode = #read{type = RType,
                                              chunk_selector = Selector,
                                              transport = Transport}} = State0,
          Callback) ->
    case read_header0(State0) of
        {ok,
         #{type := ChType,
           chunk_id := ChId,
           num_records := NumRecords,
           data_size := DataSize,
           trailer_size := TrailerSize,
           position := Pos} =
             Header,
         #?MODULE{fd = Fd, mode = #read{next_offset = ChId} = Read} =
             State1} ->
            %% read header
            %% used to write frame headers to socket
            %% and return the number of bytes to sendfile
            %% this allow users of this api to send all the data
            %% or just header and entry data
            ToSend = case RType of
                         offset ->
                             select_chunk_to_send(Selector, ChType, DataSize, TrailerSize);
                         data ->
                             DataSize + TrailerSize + ?HEADER_SIZE_B
                     end,

            %% sendfile doesn't increment the file descriptor position
            %% so we have to do this manually
            NextFilePos = Pos + DataSize + TrailerSize + ?HEADER_SIZE_B,
            State = State1#?MODULE{mode = incr_next_offset(NumRecords, Read)},
            %% only sendfile if either the reader is a data reader
            %% or the chunk is a user type (for offset readers)
            case needs_handling(RType, Selector, ChType) of
                true ->
                    _ = Callback(Header, ToSend),
                    case sendfile(Transport, Fd, Sock, Pos, ToSend) of
                        ok ->
                            {ok, _} = file:position(Fd, NextFilePos),
                            {ok, State};
                        Err ->
                            %% reset the position to the start of the current
                            %% chunk so that subsequent reads won't error
                            {ok, _} = file:position(Fd, Pos),
                            Err
                    end;
                false ->
                    {ok, _} = file:position(Fd, NextFilePos),
                    %% skip chunk and recurse
                    send_file(Sock, State, Callback)
            end;
        Other ->
            Other
    end.

%% There could be many more selectors in the future
select_chunk_to_send(user_data, ?CHNK_USER, DataSize, _TrailerSize) ->
    DataSize + ?HEADER_SIZE_B;
select_chunk_to_send(_, _, DataSize, TrailerSize) ->
    DataSize + TrailerSize + ?HEADER_SIZE_B.

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
                          readers_counter_fun = Fun}, fd = Fd}) ->
    _ = file:close(Fd),
    Fun(-1),
    case CntId of
        undefined ->
            ok;
        _ ->
            osiris_counters:delete(CntId)
    end.

delete_directory(#{name := Name} = Config) when is_map(Config) ->
    delete_directory(Name);
delete_directory(Name) when is_list(Name) ->
    Dir = directory(Name),
    ?DEBUG("osiris_log: deleting directory ~s", [Dir]),
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

header_info(Fd, Pos) ->
    {ok, Pos} = file:position(Fd, Pos),
    {ok,
     <<?MAGIC:4/unsigned,
       ?VERSION:4/unsigned,
       ChType:8/unsigned,
       _NumEntries:16/unsigned,
       Num:32/unsigned,
       _Timestamp:64/signed,
       Epoch:64/unsigned,
       Offset:64/unsigned,
       _Crc:32/integer,
       Size:32/unsigned,
       TSize:32/unsigned,
       _Reserved:32>>} =
        file:read(Fd, ?HEADER_SIZE_B),
    {ChType, Offset, Epoch, Num, Size, TSize}.

parse_records(_Offs, <<>>, Acc) ->
    %% TODO: this could probably be changed to body recursive
    lists:reverse(Acc);
parse_records(Offs,
              <<0:1, %% simple
                Len:31/unsigned,
                Data:Len/binary,
                Rem/binary>>,
              Acc) ->
    parse_records(Offs + 1, Rem, [{Offs, Data} | Acc]);
parse_records(Offs,
              <<1:1, %% simple
                0:3/unsigned, %% compression type
                _:4/unsigned, %% reserved
                NumRecs:16/unsigned,
                _UncompressedLen:32/unsigned,
                Len:32/unsigned,
                Data:Len/binary,
                Rem/binary>>,
              Acc) ->
    Recs = parse_records(Offs, Data, []),
    parse_records(Offs + NumRecs, Rem, lists:reverse(Recs) ++ Acc);
parse_records(Offs,
              <<1:1, %% simple
                CompType:3/unsigned, %% compression type
                _:4/unsigned, %% reserved
                NumRecs:16/unsigned,
                UncompressedLen:32/unsigned,
                Len:32/unsigned,
                Data:Len/binary,
                Rem/binary>>,
              Acc) ->
    %% return the first offset of the sub batch and the batch, unparsed
    %% as we don't want to decompress on the server
    parse_records(Offs + NumRecs, Rem,
                  [{Offs, {batch, NumRecs, CompType, UncompressedLen, Data}} | Acc]).

build_log_overview(Dir) when is_list(Dir) ->
    {Time, Result} = timer:tc(
                       fun() ->
                               try
                                   IdxFiles =
                                       lists:sort(
                                         filelib:wildcard(
                                           filename:join(Dir, "*.index"))),
                                   build_log_overview0(IdxFiles, [])
                               catch
                                   missing_file ->
                                       build_log_overview(Dir)
                               end
                       end),
    ?DEBUG("~s:~s/~b completed in ~fs", [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Time/1000000]),
    Result.

build_log_overview0([], Acc) ->
    lists:reverse(Acc);
build_log_overview0([IdxFile | IdxFiles], Acc0) ->
    IdxFd = open_index_read(IdxFile),
    case file:position(IdxFd, {eof, -?INDEX_RECORD_SIZE_B}) of
        {error, einval} when IdxFiles == [] andalso Acc0 == [] ->
            %% this would happen if the file only contained a header
            ok = file:close(IdxFd),
            SegFile = segment_from_index_file(IdxFile),
            [#seg_info{file = SegFile, index = IdxFile}];
        {error, einval} ->
            ok = file:close(IdxFd),
            build_log_overview0(IdxFiles, Acc0);
        {ok, Pos} ->
            %% ASSERTION: ensure we don't have rubbish data at end of index
            0 = (Pos - ?IDX_HEADER_SIZE) rem ?INDEX_RECORD_SIZE_B,
            case file:read(IdxFd, ?INDEX_RECORD_SIZE_B) of
                {ok,
                 <<_Offset:64/unsigned,
                   _Timestamp:64/signed,
                   _Epoch:64/unsigned,
                   LastChunkPos:32/unsigned,
                   _ChType:8/unsigned>>} ->
                    ok = file:close(IdxFd),
                    SegFile = segment_from_index_file(IdxFile),
                    Acc = build_segment_info(SegFile,
                                             LastChunkPos,
                                             IdxFile,
                                             Acc0),
                    build_log_overview0(IdxFiles, Acc);
                {error, enoent} ->
                    %% The retention policy could have just been applied
                    ok = file:close(IdxFd),
                    build_log_overview0(IdxFiles, Acc0)
            end
    end.

build_segment_info(SegFile, LastChunkPos, IdxFile, Acc0) ->
    try
        {ok, Fd} = open(SegFile, [read, binary, raw]),
        %% skip header,
        {ok, ?LOG_HEADER_SIZE} = file:position(Fd, ?LOG_HEADER_SIZE),
        case file:read(Fd, ?HEADER_SIZE_B) of
            eof ->
                _ = file:close(Fd),
                Acc0;
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
               FirstTSize:32/unsigned,
               _/binary>>} ->
                {ok, LastChunkPos} = file:position(Fd, LastChunkPos),
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
                   _Reserved:32>>} =
                    file:read(Fd, ?HEADER_SIZE_B),
                Size = LastChunkPos + LastSize + LastTSize + ?HEADER_SIZE_B,
                {ok, Eof} = file:position(Fd, eof),
                ?DEBUG_IF("~s: segment ~s has trailing data ~w ~w",
                          [?MODULE, filename:basename(SegFile),
                           Size, Eof], Size =/= Eof),
                _ = file:close(Fd),
                [#seg_info{file = SegFile,
                           index = IdxFile,
                           size = Size,
                           first =
                               #chunk_info{epoch = FirstEpoch,
                                           timestamp = FirstTs,
                                           id = FirstChId,
                                           num = FirstNumRecords,
                                           type = FirstChType,
                                           size = FirstSize + FirstTSize,
                                           pos = ?LOG_HEADER_SIZE},
                           last =
                               #chunk_info{epoch = LastEpoch,
                                           timestamp = LastTs,
                                           id = LastChId,
                                           num = LastNumRecords,
                                           type = LastChType,
                                           size = LastSize + LastTSize,
                                           pos = LastChunkPos}}
                 | Acc0]
        end
    catch
        missing_file ->
            %% Indexes and segments could be deleted by retention policies while
            %% the log overview is being built. Ignore those segments and keep going
            Acc0
    end.

-spec overview(term()) -> {range(), [{offset(), epoch()}]}.
overview(Dir) ->
    case build_log_overview(Dir) of
        [] ->
            {empty, []};
        SegInfos ->
            Range = offset_range_from_segment_infos(SegInfos),
            OffsEpochs = last_offset_epochs(SegInfos),
            {Range, OffsEpochs}
    end.

-spec format_status(state()) -> map().
format_status(#?MODULE{cfg = #cfg{directory = Dir,
                                  max_segment_size_bytes  = MSSB,
                                  max_segment_size_chunks  = MSSC,
                                  tracking_config = TrkConf,
                                  retention = Retention},
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
      file => filename:basename(File)}.

-spec update_retention([retention_spec()], state()) -> state().
update_retention(Retention,
                 #?MODULE{cfg = #cfg{retention = Retention0} = Cfg} = State0)
    when is_list(Retention) ->
    ?DEBUG("osiris_log: update_retention from: ~w to ~w",
           [Retention0, Retention]),
    State = State0#?MODULE{cfg = Cfg#cfg{retention = Retention}},
    trigger_retention_eval(State).

-spec evaluate_retention(file:filename(), [retention_spec()]) ->
    {range(), non_neg_integer()}.
evaluate_retention(Dir, Specs) ->
    {Time, Result} = timer:tc(
                       fun() ->
                               SegInfos0 = build_log_overview(Dir),
                               SegInfos = evaluate_retention0(SegInfos0, Specs),
                               OffsetRange = offset_range_from_segment_infos(SegInfos),
                               FirstTs = first_timestamp_from_segment_infos(SegInfos),
                               {OffsetRange, FirstTs, length(SegInfos)}
                       end),
    ?DEBUG("~s:~s/~b (~w) completed in ~fs", [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Specs, Time/1_000_000]),
    Result.

evaluate_retention0(Infos, []) ->
    %% we should never hit empty infos as one should always be left
    Infos;
evaluate_retention0(Infos, [{max_bytes, MaxSize} | Specs]) ->
    RemSegs = eval_max_bytes(Infos, MaxSize),
    evaluate_retention0(RemSegs, Specs);
evaluate_retention0(Infos, [{max_age, Age} | Specs]) ->
    RemSegs = eval_age(Infos, Age),
    evaluate_retention0(RemSegs, Specs).

eval_age([#seg_info{last = #chunk_info{timestamp = Ts},
                    size = Size} =
              Old
          | Rem] =
             Infos,
         Age) ->
    Now = erlang:system_time(millisecond),
    case Ts < Now - Age
         andalso length(Rem) > 0
         andalso Size > ?LOG_HEADER_SIZE
    of
        true ->
            %% the oldest timestamp is older than retention
            %% and there are other segments available
            %% we can delete
            ok = delete_segment(Old),
            eval_age(Rem, Age);
        false ->
            Infos
    end;
eval_age(Infos, _Age) ->
    Infos.

eval_max_bytes(SegInfos, MaxSize) ->
    TotalSize =
        lists:foldl(fun(#seg_info{size = Size}, Acc) -> Acc + Size end, 0,
                    SegInfos),
    case SegInfos of
        _ when length(SegInfos) =< 1 ->
            SegInfos;
        [_, #seg_info{size = 0}] ->
            SegInfos;
        _ ->
            case TotalSize > MaxSize of
                true ->
                    %% we can delete at least one segment
                    [Old | Rem] = SegInfos,
                    ok = delete_segment(Old),
                    eval_max_bytes(Rem, MaxSize);
                false ->
                    SegInfos
            end
    end.

%% returns a list of the last offset in each epoch
last_offset_epochs([#seg_info{first = undefined,
                              last = undefined}]) ->
    [];
last_offset_epochs([#seg_info{index = IdxFile,
                              first = #chunk_info{epoch = FstE, id = FstChId}}
                    | SegInfos]) ->
    {Time, Result} = timer:tc(
                       fun() -> 
                               FstFd = open_index_read(IdxFile),
                               {LastE, LastO, Res} =
                               lists:foldl(fun(#seg_info{index = I}, Acc) ->
                                                   Fd = open_index_read(I),
                                                   last_offset_epoch(file:read(Fd, ?INDEX_RECORD_SIZE_B),
                                                                     Fd, Acc)
                                           end,
                                           last_offset_epoch(file:read(FstFd, ?INDEX_RECORD_SIZE_B),
                                                             FstFd, {FstE, FstChId, []}),
                                           SegInfos),
                               lists:reverse([{LastE, LastO} | Res])
                       end),
    ?DEBUG("~s:~s/~b completed in ~fs", [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Time/1000000]),
    Result.

%% aggregates the chunk offsets for each epoch
last_offset_epoch(eof, Fd, Acc) ->
    ok = file:close(Fd),
    Acc;
last_offset_epoch({ok,
                   <<O:64/unsigned,
                     _T:64/signed,
                     CurEpoch:64/unsigned,
                     _:32/unsigned,
                     _ChType:8/unsigned>>},
                  Fd, {CurEpoch, _LastOffs, Acc}) ->
    %% epoch is unchanged
    last_offset_epoch(file:read(Fd, ?INDEX_RECORD_SIZE_B), Fd,
                      {CurEpoch, O, Acc});
last_offset_epoch({ok,
                   <<O:64/unsigned,
                     _T:64/signed,
                     Epoch:64/unsigned,
                     _:32/unsigned,
                     _ChType:8/unsigned>>},
                  Fd, {CurEpoch, LastOffs, Acc})
    when Epoch > CurEpoch ->
    last_offset_epoch(file:read(Fd, ?INDEX_RECORD_SIZE_B), Fd,
                      {Epoch, O, [{CurEpoch, LastOffs} | Acc]}).

segment_from_index_file(IdxFile) ->
    Basename = filename:basename(IdxFile, ".index"),
    BaseDir = filename:dirname(IdxFile),
    SegFile0 = filename:join([BaseDir, Basename]),
    SegFile0 ++ ".segment".

make_chunk(Blobs, TData, ChType, Timestamp, Epoch, Next) ->
    {NumEntries, NumRecords, EData} =
        lists:foldl(fun ({batch, NumRecords, CompType, UncompLen, B},
                         {Entries, Count, Acc}) ->
                            Data =
                                [<<1:1, %% batch record type
                                   CompType:3/unsigned,
                                   0:4/unsigned,
                                   NumRecords:16/unsigned,
                                   UncompLen:32/unsigned,
                                   (iolist_size(B)):32/unsigned>>,
                                 B],
                            {Entries + 1, Count + NumRecords, [Data | Acc]};
                        (B, {Entries, Count, Acc}) ->
                            %% simple record type
                            Data = [<<0:1, (iolist_size(B)):31/unsigned>>, B],
                            {Entries + 1, Count + 1, [Data | Acc]}
                    end,
                    {0, 0, []}, Blobs),

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
        0:32/unsigned>>,
      EData, TData],
     NumRecords}.

write_chunk(_Chunk,
            _ChType,
            _Timestamp,
            _Epoch,
            _NumRecords,
            #?MODULE{fd = undefined} = _State) ->
    full;
write_chunk(Chunk,
            ChType,
            Timestamp,
            Epoch,
            NumRecords,
            #?MODULE{cfg = #cfg{counter = CntRef} = Cfg,
                     fd = Fd,
                     index_fd = IdxFd,
                     mode =
                         #write{segment_size = {SegSizeBytes, SegSizeChunks},
                                % writers = Writers0,
                                tail_info = {Next, _}} =
                             Write} =
                State) ->
    NextOffset = Next + NumRecords,
    Size = iolist_size(Chunk),
    {ok, Cur} = file:position(Fd, cur),
    ok = file:write(Fd, Chunk),

    ok =
        file:write(IdxFd,
                   <<Next:64/unsigned,
                     Timestamp:64/signed,
                     Epoch:64/unsigned,
                     Cur:32/unsigned,
                     ChType:8/unsigned>>),
    %% update counters
    counters:put(CntRef, ?C_OFFSET, NextOffset - 1),
    counters:add(CntRef, ?C_CHUNKS, 1),
    case max_segment_size_reached(Fd, SegSizeChunks, Cfg) of
        true ->
            %% close the current file
            ok = file:close(Fd),
            ok = file:close(IdxFd),
            State#?MODULE{fd = undefined,
                          index_fd = undefined,
                          mode =
                              Write#write{tail_info =
                                          {NextOffset,
                                           {Epoch, Next, Timestamp}},
                                          segment_size = {0, 0}}};
        false ->
            State#?MODULE{mode =
                              Write#write{tail_info =
                                              {NextOffset,
                                               {Epoch, Next, Timestamp}},
                                          segment_size = {SegSizeBytes + Size, SegSizeChunks + 1}}}
    end.

max_segment_size_reached(SegFd, CurrentSizeChunks,
            #cfg{max_segment_size_bytes = MaxSizeBytes,
                 max_segment_size_chunks = MaxSizeChunks}) ->
    {ok, CurrentSizeBytes} = file:position(SegFd, cur),
    CurrentSizeBytes >= MaxSizeBytes orelse CurrentSizeChunks >= MaxSizeChunks - 1.

sendfile(_Transport, _Fd, _Sock, _Pos, 0) ->
    ok;
sendfile(tcp = Transport, Fd, Sock, Pos, ToSend) ->
    case file:sendfile(Fd, Sock, Pos, ToSend, []) of
        {ok, 0} ->
            %% TODO add counter for this?
            sendfile(Transport, Fd, Sock, Pos, ToSend);
        {ok, BytesSent} ->
            sendfile(Transport, Fd, Sock, Pos + BytesSent, ToSend - BytesSent);
        {error, _} = Err ->
            Err
    end;
sendfile(ssl, Fd, Sock, Pos, ToSend) ->
    case file:pread(Fd, Pos, ToSend) of
        {ok, Data} ->
            ssl:send(Sock, Data);
        {error, _} = Err ->
            Err
    end.

first_timestamp_from_segment_infos(
  [#seg_info{first = #chunk_info{timestamp = Ts}} | _ ]) ->
    Ts;
first_timestamp_from_segment_infos(_) ->
    0.

offset_range_from_segment_infos(SegInfos) ->
    ChunkRange = chunk_range_from_segment_infos(SegInfos),
    offset_range_from_chunk_range(ChunkRange).

chunk_range_from_segment_infos([]) ->
    empty;
chunk_range_from_segment_infos([#seg_info{first = undefined,
                                          last = undefined}]) ->
    empty;
chunk_range_from_segment_infos(SegInfos) when is_list(SegInfos) ->
    #seg_info{first = First} = hd(SegInfos),
    #seg_info{last = Last} = lists:last(SegInfos),
    {First, Last}.

offset_range_from_chunk_range(empty) ->
    empty;
offset_range_from_chunk_range({#chunk_info{id = FirstChId},
                               #chunk_info{id = LastChId,
                                           num = LastNumRecs}}) ->
    {FirstChId, LastChId + LastNumRecs - 1}.

%% find the segment the offset is in _or_ if the offset is the very next
%% chunk offset it will return the last segment
find_segment_for_offset(0,
                        [#seg_info{first = undefined, last = undefined} =
                             Info]) ->
    {end_of_log, Info};
find_segment_for_offset(Offset,
                        [#seg_info{last =
                                       #chunk_info{id = LastChId,
                                                   num = LastNumRecs}} =
                             Info])
    when Offset == LastChId + LastNumRecs ->
    %% the last segment and offset is the next offset
    {end_of_log, Info};
find_segment_for_offset(Offset,
                        [#seg_info{first = #chunk_info{id = FirstChId},
                                   last =
                                       #chunk_info{id = LastChId,
                                                   num = LastNumRecs}} =
                             Info
                         | Rem]) ->
    NextChId = LastChId + LastNumRecs,
    case Offset >= FirstChId andalso Offset < NextChId of
        true ->
            %% we found it
            {found, Info};
        false ->
            find_segment_for_offset(Offset, Rem)
    end;
find_segment_for_offset(_Offset, _) ->
    not_found.

can_read_next_offset(#read{type = offset,
                           next_offset = NextOffset,
                           offset_ref = Ref}) ->
    atomics:get(Ref, 1) >= NextOffset;
can_read_next_offset(#read{type = data}) ->
    true.

incr_next_offset(Num, #read{next_offset = NextOffset} = Read) ->
    Read#read{last_offset = NextOffset, next_offset = NextOffset + Num}.

make_file_name(N, Suff) ->
    lists:flatten(
        io_lib:format("~20..0B.~s", [N, Suff])).

open_new_segment(#?MODULE{cfg =
                              #cfg{directory = Dir,
                                   counter = Cnt},
                          fd = undefined,
                          index_fd = undefined,
                          mode =
                              #write{type = _WriterType,
                                     tail_info = {NextOffset, _}}} =
                     State0) ->
    Filename = make_file_name(NextOffset, "segment"),
    IdxFilename = make_file_name(NextOffset, "index"),
    ?DEBUG("~s: ~s : ~s", [?MODULE, ?FUNCTION_NAME, Filename]),
    {ok, Fd} =
        file:open(
            filename:join(Dir, Filename), ?FILE_OPTS_WRITE),
    ok = file:write(Fd, ?LOG_HEADER),
    {ok, IdxFd} =
        file:open(
            filename:join(Dir, IdxFilename), ?FILE_OPTS_WRITE),
    ok = file:write(IdxFd, ?IDX_HEADER),
    %% we always move to the end of the file
    {ok, _} = file:position(Fd, eof),
    {ok, _} = file:position(IdxFd, eof),
    counters:add(Cnt, ?C_SEGMENTS, 1),

    State0#?MODULE{current_file = Filename,
                   fd = Fd,
                   index_fd = IdxFd}.

open_index_read(File) ->
    {ok, Fd} = open(File, [read, raw, binary, read_ahead]),
    %% We can't use the assertion that index header is correct because of a
    %% race condition between opening the file and writing the header
    %% It seems to happen when retention policies are applied
    %% {ok, ?IDX_HEADER} = file:read(Fd, ?IDX_HEADER_SIZE)
    _ = file:read(Fd, ?IDX_HEADER_SIZE),
    Fd.

scan_idx(Offset, #seg_info{index = IndexFile,
                           last = LastChunkInSegment} = SegmentInfo) ->
    {Time, Result} = timer:tc(
                       fun() ->
                               case offset_range_from_segment_infos([SegmentInfo]) of
                                   empty ->
                                       eof;
                                   {SegmentStartOffs, SegmentEndOffs} ->
                                       case Offset < SegmentStartOffs orelse
                                            Offset > SegmentEndOffs of
                                           true ->
                                               offset_out_of_range;
                                           false ->
                                               IndexFd = open_index_read(IndexFile),
                                               Result = scan_idx(IndexFd, Offset,
                                                                 LastChunkInSegment),
                                               _ = file:close(IndexFd),
                                               Result
                                       end
                               end
                       end),
    ?DEBUG("~s:~s/~b completed in ~fs",
           [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, Time/1000000]),
    Result.

scan_idx(Fd, Offset, #chunk_info{id = LastChunkInSegmentId,
                                 num = LastChunkInSegmentNum}) ->
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok, <<ChunkId:64/unsigned,
               _Timestamp:64/signed,
               Epoch:64/unsigned,
               FilePos:32/unsigned,
               _ChType:8/unsigned>>} ->
            LastOffsetInSegment = LastChunkInSegmentId + LastChunkInSegmentNum - 1,
            case Offset < ChunkId orelse Offset > LastOffsetInSegment of
                true ->
                    %% shouldn't really happen as we check the range above
                    offset_out_of_range;
                false ->
                    %% offset is in this segment
                    scan_idx0(Fd, Offset, {ChunkId, Epoch, FilePos})
            end;
        eof ->
            %% this should never happen - offset is in the range and we are reading the first record
            eof;
        {error, Posix} ->
            Posix
    end.

scan_idx0(Fd, Offset, PreviousChunk) ->
    case file:read(Fd, ?INDEX_RECORD_SIZE_B) of
        {ok,
         <<ChunkId:64/unsigned,
           _Timestamp:64/signed,
           Epoch:64/unsigned,
           FilePos:32/unsigned,
           _ChType:8/unsigned>>} ->
            case Offset < ChunkId of
                true ->
                    %% offset we are looking for is higher or equal
                    %% to the start of the previous chunk
                    %% but lower than the start of the current chunk ->
                    %% return the previous chunk
                    PreviousChunk;
                false ->
                    scan_idx0(Fd, Offset, {ChunkId, Epoch, FilePos})
            end;
        eof ->
            %% Offset must be in the last chunk as there is no more data
            PreviousChunk;
        {error, Posix} ->
            Posix
    end.

throw_missing({error, enoent}) ->
    throw(missing_file);
throw_missing(Any) ->
    Any.

open(SegFile, Options) ->
    throw_missing(file:open(SegFile, Options)).

chunk_location_for_timestamp(#seg_info{index = Idx}, Ts) ->
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
recover_tracking(#?MODULE{cfg = #cfg{tracking_config = TrkConfig},
                          fd = Fd}) ->
    %% TODO: if the first chunk in the segment isn't a tracking snapshot and
    %% there are prior segments we could scan at least two segments increasing
    %% the chance of encountering a snapshot and thus ensure we don't miss any
    %% tracking entries
    {ok, 0} = file:position(Fd, 0),
    {ok, ?LOG_HEADER_SIZE} = file:position(Fd, ?LOG_HEADER_SIZE),
    Trk = osiris_tracking:init(undefined, TrkConfig),
    recover_tracking(Fd, Trk).

recover_tracking(Fd, Trk0) ->
    case file:read(Fd, ?HEADER_SIZE_B) of
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
           _Reserved:32>>} ->
            case ChType of
                ?CHNK_TRK_DELTA ->
                    %% tracking is written a single record so we don't
                    %% have to parse
                    {ok, <<0:1, S:31, Data:S/binary>>} = file:read(Fd, Size),
                    Trk = osiris_tracking:append_trailer(ChunkId, Data, Trk0),
                    {ok, _} = file:position(Fd, {cur, TSize}),
                    %% A tracking delta chunk will not have any writer data
                    %% so no need to parse writers here
                    recover_tracking(Fd, Trk);
                ?CHNK_TRK_SNAPSHOT ->
                    {ok, <<0:1, S:31, Data:S/binary>>} = file:read(Fd, Size),
                    {ok, _} = file:read(Fd, TSize),
                    Trk = osiris_tracking:init(Data, Trk0),
                    recover_tracking(Fd, Trk);
                ?CHNK_USER ->
                    {ok, _} = file:position(Fd, {cur, Size}),
                    {ok, TData} = file:read(Fd, TSize),

                    Trk = osiris_tracking:append_trailer(ChunkId, TData, Trk0),
                    recover_tracking(Fd, Trk)
            end;
        eof ->
            Trk0
    end.

read_header0(#?MODULE{cfg = #cfg{directory = Dir,
                                 counter = CntRef},
                      mode = #read{offset_ref = ORef,
                                   next_offset = NextChId0} = Read0,
                      current_file = CurFile,
                      fd = Fd} =
                 State) ->
    %% reads the next header if permitted
    case can_read_next_offset(Read0) of
        true ->
            {ok, Pos} = file:position(Fd, cur),
            case file:read(Fd, ?HEADER_SIZE_B) of
                {ok,
                 <<?MAGIC:4/unsigned,
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
                   _Reserved:32>> =
                     HeaderData} ->
                    counters:put(CntRef, ?C_OFFSET, NextChId0 + NumRecords),
                    counters:add(CntRef, ?C_CHUNKS, 1),
                    {ok,
                     #{chunk_id => NextChId0,
                       epoch => Epoch,
                       type => ChType,
                       crc => Crc,
                       num_records => NumRecords,
                       num_entries => NumEntries,
                       timestamp => Timestamp,
                       data_size => DataSize,
                       trailer_size => TrailerSize,
                       header_data => HeaderData,
                       position => Pos},
                     State};
                {ok, Bin} when byte_size(Bin) < ?HEADER_SIZE_B ->
                    %% partial header read
                    %% this can happen when a replica reader reads ahead
                    %% optimistically
                    %% set the position back and and return end of stream
                    {ok, Pos} = file:position(Fd, Pos),
                    {end_of_stream, State};
                eof ->
                    FirstOffset = atomics:get(ORef, 2),
                    %% open next segment file and start there if it exists
                    NextChId = max(FirstOffset, NextChId0),
                    SegFile = make_file_name(NextChId, "segment"),
                    case SegFile == CurFile of
                        true ->
                            %% the new filename is the same as the old one
                            %% this should only really happen for an empty
                            %% log but would cause an infinite loop if it does
                            {end_of_stream, State};
                        false ->
                            case file:open(
                                     filename:join(Dir, SegFile),
                                     [raw, binary, read])
                            of
                                {ok, Fd2} ->
                                    ok = file:close(Fd),
                                    {ok, _} =
                                        file:position(Fd2, ?LOG_HEADER_SIZE),
                                    Read = Read0#read{next_offset = NextChId},
                                    read_header0(State#?MODULE{current_file =
                                                                   SegFile,
                                                               fd = Fd2,
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
                   _Reserved:32>>} ->
                    %% TODO: we may need to return the new state here if
                    %% we've crossed segments
                    {ok, Pos} = file:position(Fd, Pos),
                    {error, {unexpected_chunk_id, UnexpectedChId, NextChId0}};
                Invalid ->
                    _ = file:position(Fd, Pos),
                    {error, {invalid_chunk_header, Invalid}}
            end;
        false ->
            {end_of_stream, State}
    end.

trigger_retention_eval(#?MODULE{cfg =
                                    #cfg{directory = Dir,
                                         retention = RetentionSpec,
                                         counter = Cnt,
                                         first_offset_fun = Fun}} = State) ->
    ok =
        osiris_retention:eval(Dir, RetentionSpec,
                              %% updates first offset and first timestamp
                              %% after retention has been evaluated
                              fun ({{FstOff, _}, FstTs, Seg}) when is_integer(FstOff),
                                                                   is_integer(FstTs) ->
                                      Fun(FstOff),
                                      counters:put(Cnt, ?C_FIRST_OFFSET, FstOff),
                                      counters:put(Cnt, ?C_FIRST_TIMESTAMP, FstTs),
                                      counters:put(Cnt, ?C_SEGMENTS, Seg);
                                  (_) ->
                                      ok
                              end),
    State.

next_location(undefined) ->
    {0, ?LOG_HEADER_SIZE};
next_location(#chunk_info{id = Id,
                          num = Num,
                          pos = Pos,
                          size = Size}) ->
    {Id + Num, Pos + Size + ?HEADER_SIZE_B}.

-ifdef(TEST).

% -include_lib("eunit/include/eunit.hrl").

part_test() ->
    [<<"ABCD">>] = part(4, [<<"ABCDEF">>]),
    [<<"AB">>, <<"CD">>] = part(4, [<<"AB">>, <<"CDEF">>]),
    [<<"AB">>, <<"CDEF">>] = part(6, [<<"AB">>, <<"CDEF">>]),
    ok.

-endif.
