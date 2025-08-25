-module(osiris_log_manifest).

-include("src/osiris.hrl").

-type state() :: term().

-type segment_info() ::
    #{file := file:filename_all(),
      size := non_neg_integer(),
      chunks := non_neg_integer(),
      first => #chunk_info{},
      last => #chunk_info{}
     }.

-type log_info() ::
    #{num_segments := non_neg_integer(),
      %% These keys may be unset if the log is empty.
      first_offset => osiris:offset(),
      first_timestamp => osiris:timestamp(),
      last_segment => segment_info(),
      %% Optional. Included by the default impls of writer_manifest/1 and
      %% acceptor_manifest/3 for the convenience of other impls.
      segment_offsets => [osiris:offset()]
     }.

-type event() :: {segment_opened,
                  OldSegment :: file:filename_all() | undefined,
                  NewSegment :: file:filename_all()} |
                 {chunk_written, #chunk_info{}, iolist()}.

-export_type([state/0, log_info/0, event/0]).

-callback overview(Dir :: file:filename_all()) -> osiris_log:overview().

-callback acceptor_manifest(osiris_log:overview(), osiris_log:config()) ->
    {log_info(), osiris_log:config(), state()}.

-callback writer_manifest(osiris_log:config()) ->
    {log_info(), osiris_log:config(), state()}.

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
