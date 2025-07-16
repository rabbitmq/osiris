-module(osiris_log_reader).

-type state() :: term().

-export_type([state/0]).

-callback open(SegmentFilename :: file:filename_all()) ->
    {ok, state()} | {error, term()}.

-callback pread(state(),
                Offset :: non_neg_integer(),
                Bytes :: non_neg_integer()) ->
    {ok, Data :: binary()} | eof | {error, term()}.

-callback sendfile(state(),
                   gen_tcp:socket(),
                   Offset :: non_neg_integer(),
                   Bytes :: non_neg_integer()) ->
    {ok, BytesWritten :: non_neg_integer()} | {error, term()}.

-callback close(state()) -> ok | {error, term()}.
