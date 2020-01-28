-module(osiris_segment).

-export([
         init/2,
         write/2,
         accept_chunk/2,
         next_offset/1,
         send_file/2,
         send_file/3,

         init_reader/2,
         read_chunk_parsed/1,
         close/1,

         chunk/2
         ]).

-define(DEFAULT_MAX_SEGMENT_SIZE, 500 * 1000 * 1000).
-define(INDEX_RECORD_SIZE, 16).

%% Data format
%% Write in "chunks" which are batches of blobs
%%
%% <<
%%   <<"CHNK">>/binary, %% MAGIC
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

-type config() :: #{}.

-type record() :: {offset(), iodata()}.

%% holds static or rarely changing fields
-record(cfg, {directory :: file:filename(),
              mode :: read | write,
              max_size = ?DEFAULT_MAX_SEGMENT_SIZE :: non_neg_integer()
             }).

-record(?MODULE, {cfg :: #cfg{},
                  fd :: file:io_device(),
                  index_fd :: undefined | file:io_device(),
                  offset_ref :: undefined | file:io_device(),
                  segment_size = 0 :: non_neg_integer(),
                  next_offset = 0 :: offset()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              offset/0,
              record/0,
              config/0
              ]).

-spec init(file:filename(), config()) -> state().
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
    {NextOffset, File} = find_next_offset(Dir),
    error_logger:info_msg("osiris_segment:init/2: next offset ~b",
                          [NextOffset]),
    {Filename, IdxFilename} =
        case File of
            undefined ->
                {make_file_name(NextOffset, "segment"),
                 make_file_name(NextOffset, "index")};
            F ->
                {F ++".segment", F ++ ".index"}
        end,
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

-spec write([iodata()], state()) -> state().
write([], #?MODULE{cfg = #cfg{}} = State) ->
    State;
write(Blobs, #?MODULE{cfg = #cfg{},
                      next_offset = Next} = State) ->
    %% assigns indexes to all blobs
    %% checks segment size
    %% rolls over to new segment file if needed
    %% Records range in ETS segment lookup table
    %% Writes every n blob index/offsets to index file
    Chunk = chunk(Blobs, Next),
    NextOffset = Next + length(Blobs),
    write_chunk(Chunk, NextOffset, State).

-spec accept_chunk(iodata(), state()) -> state().
accept_chunk([<<"CHNK", Next:64/unsigned,
                Num:32/unsigned, _/binary>> | _] = Chunk,
             #?MODULE{next_offset = Next} = State) ->
    NextOffset = Next + Num,
    write_chunk(Chunk, NextOffset, State);
accept_chunk(Binary, State)
  when is_binary(Binary) ->
    accept_chunk([Binary], State);
accept_chunk([<<"CHNK", Next:64/unsigned,
                _Num:32/unsigned, _/binary>> | _] = _Chunk,
             #?MODULE{next_offset = ExpectedNext}) ->
    exit({accept_chunk_out_of_order, Next, ExpectedNext}).


-spec next_offset(state()) -> offset().
next_offset(#?MODULE{next_offset = Next}) ->
    Next.

-spec init_reader(undefined | offset(), config()) -> state().
init_reader(StartOffset0, #{dir := Dir} = Config) ->
    StartOffset = case StartOffset0 of
                      undefined ->
                          {NextOffs, _} = find_next_offset(Dir),
                          maps:get(committed_offset, Config, NextOffs);
                      _ ->
                          StartOffset0
                  end,
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
            {ok, Data} = file:read_file(IndexFile),
            FilePos = scan_index(Data, StartOffset),
            Ref = maps:get(offset_ref, Config, undefined),

            %% scan the index to find nearest chunk offset to position
            %% the file cursor at
            {ok, Pos} = file:position(Fd, FilePos),
            error_logger:info_msg("init_reader: at ~b file pos ~b/~w",
                                  [StartOffset, Pos, FilePos]),
            #?MODULE{cfg = #cfg{directory = Dir,
                                mode = write},
                     offset_ref = Ref,
                     next_offset = StartOffset,
                     fd = Fd};
        _ ->
            exit({no_segment, Dir})
    end.


-spec read_chunk_parsed(state()) ->
    {ok, [record()], state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk_parsed(#?MODULE{cfg = #cfg{directory = Dir},
                           fd = Fd,
                           offset_ref = Ref,
                           next_offset = Next} = State) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    COffs = case Ref of
                undefined -> undefined;
                _ ->
                    atomics:get(Ref, 1)
            end,
    case file:read(Fd, 4 + 8 + 4 + 4 + 4) of
        {ok, <<"CHNK",
               Offs:64/unsigned,
               NumBlobs:32/unsigned,
               _Crc:32/integer,
               DataSize:32/unsigned>>}
          when Ref == undefined orelse COffs >= Offs ->
            {ok, BlobData} = file:read(Fd, DataSize),
            %% parse blob data into records
            Records = parse_records(Offs, BlobData, []),
            {Records, State#?MODULE{next_offset = Offs + NumBlobs}};
        {ok, _} ->
            %% set the position back for the next read
            % error_logger:info_msg("osiris_segment end coff~w", [COffs]),
            {ok, _} = file:position(Fd, {cur, -24}),
            {end_of_stream, State};
        eof ->
            %% open next segment file and start there if it exists
            SegFile = make_file_name(Next, "segment"),
            %% TODO check for error and return end_of_stream if the file
            %% does not exist
            case file:open(filename:join(Dir, SegFile),
                           [raw, binary, read]) of
                {ok, Fd2} ->
                    ok = file:close(Fd),
                    read_chunk_parsed(State#?MODULE{fd = Fd2});
                {error, enoent} ->
                    {end_of_stream, State}
            end;
        Invalid ->
            {error, {invalid_chunk_header, Invalid}}
    end.

-spec send_file(gen_tcp:socket(), state()) ->
    {ok, state()} | {end_of_stream, state()}.
send_file(Sock, State) ->
    send_file(Sock, State, undefined).

-spec send_file(gen_tcp:socket(), state(),
                non_neg_integer() | undefined) ->
    {ok, state()} | {end_of_stream, state()}.
send_file(Sock, #?MODULE{cfg = #cfg{directory = Dir},
                         fd = Fd,
                         next_offset = Next} = State,
         MaxOffset) ->
    {ok, Pos} = file:position(Fd, cur),
    case file:read(Fd, 24) of
        {ok, <<"CHNK",
               Offs:64/unsigned,
               NumRecords:32/unsigned,
               _Crc:32/integer,
               DataSize:32/unsigned>>}
        %% MaxOffset can be undefined, when that is the case this guard
        %% will return true which is what we expect
          when Offs =< MaxOffset ->
            %% read header
            ToSend = DataSize + 24,
            ok = sendfile(Fd, Sock, Pos, ToSend),
            FilePos = Pos + ToSend,
            {ok, FilePos} = file:position(Fd, FilePos),
            {ok, State#?MODULE{next_offset = Offs + NumRecords}};
        {ok, _} ->
            %% there is data but the committed offset isn't high enough
            %% reset file pos
            {ok, Pos} = file:position(Fd, Pos),
            {end_of_stream, State};
        eof ->
            %% open next segment file and start there if it exists
            SegFile = make_file_name(Next, "segment"),
            case file:open(filename:join(Dir, SegFile),
                           [raw, binary, read]) of
                {ok, Fd2} ->
                    error_logger:info_msg("sendfile eof ~w", [Next]),
                    ok = file:close(Fd),
                    send_file(Sock, State#?MODULE{fd = Fd2}, MaxOffset);
                {error, enoent} ->
                    {end_of_stream, State}
            end
    end.

close(_State) ->
    %% close fd
    ok.

%% Internal

scan_index(<<>>, _Offset) ->
    0;
scan_index(<<O:64/unsigned,
             Num:32/unsigned,
             Pos:32/unsigned>>, Offset) ->
    %% TODO: the requested offset may not be found in the last batch as it
    %% may not have been written yet
    case Offset >= O andalso  Offset < (O + Num) of
        true ->
            Pos;
        false ->
            eof
    end;
scan_index(<<O:64/unsigned,
             _:32/unsigned,
             Pos:32/unsigned, Rem/binary>>, Offset)  ->
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

find_next_offset(Dir) ->
    IdxFiles = lists:reverse(lists:sort(
                               filelib:wildcard(
                                 filename:join(Dir, "*.index")))),
    find_next_offset0(0, IdxFiles).

find_next_offset0(NextOffs, []) ->
    {NextOffs, undefined};
find_next_offset0(NextOffs, [IdxFile | Rem]) ->
    {ok, Data} = file:read_file(IdxFile),
    case Data of
        <<>> ->
            %% read prevous if exists
            find_next_offset0(NextOffs, Rem);
        Data ->
            %% get last batch offset
            Pos = byte_size(Data) - ?INDEX_RECORD_SIZE,
            <<Offset:64/unsigned,
              Num:32/unsigned,
              FilePos:32/unsigned>> = binary:part(Data, Pos,
                                                  ?INDEX_RECORD_SIZE),
            Basename = filename:basename(IdxFile, ".index"),
            BaseDir = filename:dirname(IdxFile),
            SegFile0 = filename:join([BaseDir, Basename]),
            SegFile = SegFile0 ++ ".segment",
            {ok, Fd} = file:open(SegFile, [read, binary, raw]),
            {ok, FilePos} = file:position(Fd, FilePos),
            %% reader batch header
            {Offset + Num, filename:join(BaseDir, Basename)}
    end.


chunk(Blobs, Next) ->
    {_, IoList} = lists:foldr(
                    fun (B, {NextOff, Acc}) ->
                            Data = [
                                    <<(iolist_size(B)):32/unsigned>>,
                                    <<NextOff:64/unsigned>>,
                                    B],
                            {NextOff+1, [Data | Acc]}
                    end, {Next, []}, Blobs),
    % Bin = term_to_binary(IoList, [{compressed, 9}]),
    Bin = IoList,
    Size = erlang:iolist_size(Bin),
    [<<"CHNK">>,
     <<Next:64/unsigned,
       (length(Blobs)):32/unsigned,
       0:32/integer,
       Size:32/unsigned>>,
     Bin].

write_chunk(Chunk, NextOffset,
            #?MODULE{cfg = #cfg{directory = Dir,
                                max_size = MaxSize},
                     fd = Fd,
                     index_fd = IdxFd,
                     segment_size = SegSize,
                     next_offset = Next} = State) ->
    Size = erlang:iolist_size(Chunk),
    Num = NextOffset - Next,
    {ok, Cur} = file:position(Fd, cur),
    ok = file:write(Fd, Chunk),
    ok = file:write(IdxFd, <<Next:64/unsigned,
                             Num:32/unsigned,
                             Cur:32/unsigned>>),
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
        {ok, _} ->
            State#?MODULE{next_offset = NextOffset,
                          segment_size = SegSize + Size}
    end.

make_file_name(N, Suff) ->
    lists:flatten(io_lib:format("~20..0B.~s", [N, Suff])).


sendfile(_Fd, _Sock, _Pos, 0) ->
    ok;
sendfile(Fd, Sock, Pos, ToSend) ->
    case file:sendfile(Fd, Sock, Pos, ToSend, []) of
        {ok, 0} ->
            % error_logger:info_msg("sendfile sent 0 out of ~b bytes~n",
            %                       [ToSend]),
            % timer:sleep(1),
            % erlang:yield(),
            sendfile(Fd, Sock, Pos, ToSend);
        {ok, BytesSent} ->
            sendfile(Fd, Sock, Pos + BytesSent, ToSend - BytesSent)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
