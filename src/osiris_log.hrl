%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

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

-type chunk_type() ::
    ?CHNK_USER |
    ?CHNK_TRK_DELTA |
    ?CHNK_TRK_SNAPSHOT.

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
%% TODO should this be a opaque instead? Maybe a map {read, ReadMap},
%% and let the backend have its own version of it.
-record(read,
        {type :: data | offset,
         next_offset = 0 :: osiris:offset(),
         transport :: osiris_log:transport(),
         chunk_selector :: all | user_data,
         position = 0 :: non_neg_integer(),
         filter :: undefined | osiris_bloom:mstate()}).
-record(write,
        {type = writer :: writer | acceptor,
         segment_size = {?LOG_HEADER_SIZE, 0} :: {non_neg_integer(), non_neg_integer()},
         current_epoch :: non_neg_integer(),
         tail_info = {0, empty} :: osiris:tail_info()
        }).
-record(osiris_log,
        {cfg :: #cfg{},
         mode :: #read{} | #write{},
         current_file :: undefined | file:filename_all(),
         index_fd :: undefined | file:io_device(),
%% Should the FDs be moved to the write and read config?
         fd :: undefined | file:io_device()
        }).
%% record chunk_info does not map exactly to an index record (field 'num' differs)
-record(chunk_info,
        {id :: osiris:offset(),
         timestamp :: non_neg_integer(),
         epoch :: osiris:epoch(),
         num :: non_neg_integer(),
         type :: chunk_type(),
         %% size of data + filter + trailer
         size :: non_neg_integer(),
         %% position in segment file
         pos :: integer()
        }).
-record(seg_info,
        {file :: file:filename_all(),
         size = 0 :: non_neg_integer(),
         index :: file:filename_all(),
         first :: undefined | #chunk_info{},
         last :: undefined | #chunk_info{}}).
