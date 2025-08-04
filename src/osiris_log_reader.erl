-module(osiris_log_reader).

-export([open/1, pread/3, sendfile/5, close/1]).

-type state() :: term().

-export_type([state/0]).

-callback open(SegmentFilename :: file:filename_all()) ->
    {ok, state()} | {error, term()}.

-callback pread(state(),
                Offset :: non_neg_integer(),
                Bytes :: non_neg_integer()) ->
    {ok, Data :: binary(), state()} | eof | {error, term()}.

-callback sendfile(tcp | ssl,
                   state(),
                   gen_tcp:socket() | ssl:socket(),
                   Pos :: non_neg_integer(),
                   ToSend :: non_neg_integer()) ->
    {ok, BytesWritten :: non_neg_integer(), state()} | {error, term()}.

-callback close(state()) -> ok | {error, term()}.

%% --- Default implementation

open(SegmentFilename) ->
    file:open(SegmentFilename, [raw, binary, read]).

pread(Fd, Offset, Bytes) ->
    case file:pread(Fd, Offset, Bytes) of
        {ok, Data} ->
            {ok, Data, Fd};
        eof ->
            eof;
        {error, _} = Err ->
            Err
    end.

sendfile(tcp, Fd, _Socket, _Pos, 0) ->
    {ok, Fd};
sendfile(tcp, Fd, Socket, Pos, ToSend) ->
    case file:sendfile(Fd, Socket, Pos, ToSend, []) of
        {ok, BytesSent} ->
            sendfile(tcp, Fd, Socket, Pos + BytesSent, ToSend - BytesSent);
        {error, _} = Err ->
            Err
    end;
sendfile(ssl, Fd, Socket, Pos, ToSend) ->
    case file:pread(Fd, Pos, ToSend) of
        {ok, Data} ->
            ok = ssl:send(Socket, Data),
            {ok, Fd};
        {error, _} = Err ->
            Err
    end.

close(Fd) ->
    file:close(Fd).
