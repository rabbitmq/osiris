-module(osiris_log_reader).

-type reader() :: term().

-export_type([reader/0]).

-callback open(SegmentFilename :: file:filename_all()) ->
    {ok, reader()} | {error, term()}.

-callback pread(reader(),
                Offset :: non_neg_integer(),
                Bytes :: non_neg_integer()) ->
    {ok, Data :: binary()} | eof | {error, term()}.

-callback sendfile(reader(),
                   gen_tcp:socket(),
                   Offset :: non_neg_integer(),
                   Bytes :: non_neg_integer()) ->
    {ok, BytesWritten :: non_neg_integer()} | {error, term()}.

-callback close(reader()) -> ok | {error, term()}.
