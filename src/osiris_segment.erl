-module(osiris_segment).

-export([
         init/2,
         write/2,
         accept_chunk/2,
         next_offset/1,

         init_reader/3,
         read_chunk_parsed/1,
         next_chunk/3,
         close/1


         ]).

-define(DEFAULT_MAX_SEGMENT_SIZE, 500 * 1000 * 1000).

%% Data format
%% Write in "chunks" which are batches of blobs
%%
%% <<
%%   <<"CHK1">>/binary, %% MAGIC
%%   ChunkFirstOffset:64/unsigned,
%%   NumRecords:32/unsigned,
%%   ChunkCrc:32/integer, %% CRC for the records portion of the data
%%   DataLength:32/unsigned, %% length until end of chunk
%%   RecordLength:32/unsigned,
%%   RecordData:RecordLength/binary
%%   ...>>
%%
%%   Chunks is the unit of replication and read
%%
%%   Index format:
%%   Maps each chunk to an offset
%%   | Offset | FileOffset

-type offset() :: non_neg_integer().

%% holds static or rarely changing fields
-record(cfg, {directory :: file:filename(),
              mode :: read | write,
              max_size :: non_neg_integer()
             }).

-record(?MODULE, {cfg :: #cfg{},
                  fd :: file:io_device(),
                  index_fd :: file:io_device(),
                  segment_size = 0 :: non_neg_integer(),
                  next_offset = 0 :: offset()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              offset/0
              ]).

make_file_name(N, Suff) ->
    lists:flatten(io_lib:format("~20..0B.~s", [N, Suff])).



init(Dir, Config) ->
    %% scan directory for segments if in write mode
    %% re-builds segment lookup ETS table (used by readers)
    MaxSize = case Config of
                  #{max_segment_size := M} ->
                      M;
                  _ ->
                      ?DEFAULT_MAX_SEGMENT_SIZE
              end,
    filelib:ensure_dir(Dir),
    file:make_dir(Dir),
    NextOffset = find_last_offset(Dir) + 1,
    Filename = make_file_name(NextOffset, "segment"),
    IdxFilename = make_file_name(NextOffset, "index"),
    {ok, Fd} = file:open(filename:join(Dir, Filename),
                         [raw, binary, append]),
    {ok, IdxFd} = file:open(filename:join(Dir, IdxFilename),
                            [raw, binary, append]),
    #?MODULE{cfg = #cfg{directory = Dir,
                        max_size = MaxSize,
                        mode = write},
             fd = Fd,
             next_offset = NextOffset,
             index_fd = IdxFd}.

write(Blobs, #?MODULE{cfg = #cfg{
                                 },
                      next_offset = Next} = State) ->
    %% assigns indexes to all blobs
    %% checks segment size
    %% rolls over to new segment file if needed
    %% Records range in ETS segment lookup table
    %% Writes every n blob index/offsets to index file
    {_, IoList} = lists:foldr(
                    fun (B, {NextOff, Acc}) ->
                            Data = [
                                    <<(iolist_size(B)):32/unsigned>>,
                                    <<NextOff:64/unsigned>>,
                                    B],
                            {NextOff+1, [Data | Acc]}
                    end, {Next, []}, Blobs),
    Size = erlang:iolist_size(IoList),
    Chunk = [<<"CHNK">>,
             <<Next:64/unsigned,
               (length(Blobs)):32/unsigned,
               0:32/integer,
               Size:32/unsigned>>,
             IoList],
    NextOffset = Next + length(Blobs),
    write_chunk(Chunk, NextOffset, State).

accept_chunk(<<"CHNK", Next:64/unsigned,
               Num:32/unsigned, _/binary>> = Chunk,
             State) ->
    NextOffset = Next + Num,
    write_chunk(Chunk, NextOffset, State).


write_chunk(Chunk, NextOffset,
            #?MODULE{cfg = #cfg{directory = Dir,
                                max_size = MaxSize},
                     fd = Fd,
                     index_fd = IdxFd,
                     segment_size = SegSize,
                     next_offset = Next} = State) ->
    Size = erlang:iolist_size(Chunk),
    {ok, Cur} = file:position(Fd, cur),
    ok = file:write(Fd, Chunk),
    ok = file:write(IdxFd, <<Next:64/unsigned, Cur:32/unsigned>>),
    case file:position(Fd, cur) of
        {ok, After} when After >= MaxSize ->
            %% need a new segment file
            ok = file:close(Fd),
            ok = file:close(IdxFd),
            Filename = make_file_name(NextOffset, "segment"),
            IdxFilename = make_file_name(NextOffset, "index"),
            {ok, Fd2} = file:open(filename:join(Dir, Filename),
                                 [raw, binary, append]),
            {ok, IdxFd2} = file:open(filename:join(Dir, IdxFilename),
                                    [raw, binary, append]),
            State#?MODULE{fd = Fd2,
                          index_fd = IdxFd2,
                          next_offset = NextOffset,
                          segment_size = SegSize + Size};
        _ ->
            State#?MODULE{next_offset = NextOffset,
                          segment_size = SegSize + Size}
    end.

next_offset(#?MODULE{next_offset = Next}) ->
    Next.

init_reader(StartOffset, Dir, _Config) ->
    %% find the appopriate segment and scan the index to find the
    %% postition of the next chunk to read
    SegFiles = lists:sort(filelib:wildcard(filename:join(Dir, "*.segment"))),

    %% find nearest segment file to StartOffset
    Smaller = lists:takewhile(
                fun (F) ->
                        {O, _} = string:to_integer(filename:basename(F)),
                        O =< StartOffset
                end, SegFiles),

    case lists:reverse(Smaller) of
        [StartSegment | _] ->
            {ok, Fd} = file:open(StartSegment, [raw, binary, read]),
            IndexFile = filename:rootname(StartSegment) ++ ".index",
            ct:pal("index file ~s", [IndexFile]),
            {ok, Data} = file:read_file(IndexFile),
            FilePos = scan_index(Data, StartOffset),

            %% scan the index to find nearest chunk offset to position
            %% the file cursor at
            {ok, FilePos} = file:position(Fd, FilePos),
            #?MODULE{cfg = #cfg{directory = Dir,
                                mode = write},
                     next_offset = StartOffset,
                     fd = Fd};
        _ ->
            exit(no_segment)
    end.


read_chunk_parsed(#?MODULE{cfg = #cfg{directory = Dir},
                           fd = Fd,
                           next_offset = Next} = State) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    case file:read(Fd, 4 + 8 + 4 + 4 + 4) of
        {ok, <<"CHNK",
               Offs:64/unsigned,
               NumBlobs:32/unsigned,
               _Crc:32/integer,
               DataSize:32/unsigned>>} ->
            {ok, BlobData} = file:read(Fd, DataSize),
            %% parse blob data into records
            Records = parse_records(Offs, BlobData, []),
            {Records, State#?MODULE{next_offset = Offs + NumBlobs}};
        eof ->
            ok = file:close(Fd),
            %% open next segment file and start there if it exists
            SegFile = make_file_name(Next, "segment"),
            {ok, Fd2} = file:open(filename:join(Dir, SegFile),
                                  [raw, binary, read]),
            read_chunk_parsed(State#?MODULE{fd = Fd2});
        Invalid ->
            {error, {invalid_chunk_header, Invalid}}
    end.


next_chunk(HandleFun, HandlerState,
           #?MODULE{fd = Fd} = State) ->
    %% passes the file handle, HandlerState Offset and Total Chunk Length
    %% to the HandlerFun
    %% This is to be used by replicator readers that use file:sendfile/3 to
    %% replicate the data
    %% updates it's own state for the offset of the next one
    %% If there is no more data in the file and the current file is the last
    %% one it returns {eof, State}
    %% the reader can then register with the osiris writer process to be notified
    %% next time a write happens
    {ok, Pos} = file:position(Fd, cur),
    %% read the header
    {HandlerState, State}.


close(State) ->
    %% close fd
    ok.

%% Internal

scan_index(<<_:64/unsigned, Pos:32/unsigned>>, _Offset) ->
    Pos;
scan_index(<<O:64/unsigned, Pos:32/unsigned, Rem/binary>>, Offset)  ->
    <<ONext:64/unsigned, _/binary>> = Rem,
     case Offset >= O andalso Offset < ONext of
         true ->
             Pos;
         false ->
             scan_index(Rem, Offset)
     end.

parse_records(_Offs, <<>>, Acc) ->
    lists:reverse(Acc);
parse_records(Offs, <<Len:32/unsigned, _:64/unsigned,
                      Data:Len/binary, Rem/binary>>, Acc) ->
    parse_records(Offs+1, Rem, [{Offs, Data} | Acc]).

find_last_offset(Dir) ->
    SegFiles = lists:reverse(lists:sort(
                               filelib:wildcard(
                                 filename:join(Dir, "*.index")))),
    case SegFiles of
        [LastIdxFile | _ ] ->
            {ok, Data} = file:read_file(LastIdxFile),
            Pos = byte_size(Data) - 12,
            <<_:64/unsigned, FilePos:32/unsigned>> = binary:part(Data, Pos, 12),
            FilePos;
        _ ->
            -1
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
