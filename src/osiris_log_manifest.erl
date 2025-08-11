-module(osiris_log_manifest).

-include("src/osiris.hrl").

-type state() :: term().

-type writer_info() ::
    #{num_segments := non_neg_integer(),
      %% These keys may be unset if the log is empty.
      first_offset => osiris:offset(),
      first_timestamp => osiris:timestamp(),
      last_segment_file => file:filename_all(),
      last_segment_size => non_neg_integer(),
      last_chunk => #chunk_info{}
     }.

-type event() :: {segment_opened,
                  OldSegment :: file:filename_all() | undefined,
                  NewSegment :: file:filename_all()} |
                 {chunk_written, #chunk_info{}, iolist()}.

-export_type([state/0, writer_info/0, event/0]).

-callback acceptor_manifest(osiris_log:range(),
                            EpochOffsets :: [{osiris:offset(), osiris:epoch()}],
                            osiris_log:config()) ->
    {writer_info(), state()}.

-callback writer_manifest(osiris_log:config()) ->
    {writer_info(), state()}.

-callback find_data_reader_position(osiris:tail_info(), osiris_log:config()) ->
    {ok, osiris:offset(), Pos :: non_neg_integer(),
     Segment :: file:filename_all()} |
    {error,
     {offset_out_of_range,
      empty | {From :: osiris:offset(), To :: osiris:offset()}}} |
    {error, {invalid_last_offset_epoch, osiris:epoch(), osiris:offset()}} |
    {error, term()}.

-callback find_offset_reader_position(osiris:offset_spec(),
                                      osiris_log:config()) ->
    {ok, osiris:offset(), Pos :: non_neg_integer(),
     Segment :: file:filename_all()} |
    {error,
     {offset_out_of_range,
      empty | {From :: osiris:offset(), To :: osiris:offset()}}} |
    {error, {invalid_chunk_header, term()}} |
    {error, no_index_file} |
    {error, retries_exhausted}.

-callback handle_event(event(), state()) -> state().

-callback close_manifest(state()) -> ok.

-callback delete(osiris_log:config()) -> ok.
