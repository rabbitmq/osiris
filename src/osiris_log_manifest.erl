-module(osiris_log_manifest).

-include("src/osiris.hrl").

-type log_kind() :: writer | acceptor | data_reader | offset_reader.

-type state() :: term().

-type event() :: {segment_opened,
                  OldSegment :: file:filename_all() | undefined,
                  NewSegment :: file:filename_all()} |
                 {chunk_written, #chunk_info{}, iolist()}.

-export_type([state/0, event/0]).

-callback init_manifest(log_kind(), osiris_log:config()) -> state().

%% Called whenever initialization is finished. Free any resources used during
%% init.
-callback finalize_manifest(state()) -> state().

-callback close_manifest(state()) -> ok.

-callback handle_event(event(), state()) -> state().

-callback fix_corrupted_files(state()) -> state().

-callback first_and_last_seginfos(state()) ->
    none |
    {NumSegments :: non_neg_integer(),
     First :: #seg_info{}, Last :: #seg_info{}}.

-callback truncate_to(osiris_log:range(),
                      EpochOffsets :: [{osiris:offset(), osiris:epoch()}],
                      state()) ->
    state().

-callback find_data_reader_position(osiris:tail_info(), state()) ->
    {ok, osiris:offset(), Pos :: non_neg_integer(),
     Segment :: file:filename_all()} |
    {error,
     {offset_out_of_range,
      empty | {From :: osiris:offset(), To :: osiris:offset()}}} |
    {error, {invalid_last_offset_epoch, osiris:epoch(), osiris:offset()}} |
    {error, term()}.

-callback find_offset_reader_position(osiris:offset_spec(), state()) ->
    {ok, osiris:offset(), Pos :: non_neg_integer(),
     Segment :: file:filename_all()} |
    {error,
     {offset_out_of_range,
      empty | {From :: osiris:offset(), To :: osiris:offset()}}} |
    {error, {invalid_chunk_header, term()}} |
    {error, no_index_file} |
    {error, retries_exhausted}.

-callback delete(osiris_log:config()) -> ok.
