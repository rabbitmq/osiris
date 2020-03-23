-module(osiris_log).

-export([
         init/2,
         write/2,
         accept_chunk/2,
         next_offset/1,
         tail_info/1,
         send_file/2,
         send_file/3,

         init_data_reader/2,
         init_offset_reader/2,
         read_chunk/1,
         read_chunk_parsed/1,
         get_offset_ref/1,
         get_current_epoch/1,
         close/1,

         directory/1,
         delete_directory/1

         ]).

-define(DEFAULT_MAX_SEGMENT_SIZE_B, 500 * 1000 * 1000).
-define(INDEX_RECORD_SIZE_B, 16).
-define(MAGIC, 5).
%% format version
-define(VERSION, 0).
-define(HEADER_SIZE_B, 31).

%% Data format
%% Write in "chunks" which are batches of blobs
%%
%% <<
%%   Magic=5:4/unsigned,
%%   ProtoVersion:4/unsigned,
%%   NumEntries:16/unsigned, %% need some kind of limit on chunk sizes 64k is a good start
%%   NumRecords:32/unsigned, %% total including all sub batch entries
%%   Epoch:64/unsigned,
%%   ChunkFirstOffset:64/unsigned,
%%   ChunkCrc:32/integer, %% CRC for the records portion of the data
%%   DataLength:32/unsigned, %% length until end of chunk
%%   [Entry]
%%   ...>>
%%
%%   Entry Format
%%   <<0=SimpleEntryType:1,
%%     Size:31/unsigned,
%%     Data:Size/binary>> |
%%
%%   <<1=SubBatchEntryType:1,
%%     CompressionType:3,
%%     Reserved:4,
%%     NumRecords:16/unsigned,
%%     Size:32/unsigned,
%%     Data:Size/binary>>
%%
%%   Chunks is the unit of replication and read
%%
%%   Index format:
%%   Maps each chunk to an offset
%%   | Offset | FileOffset

-type offset() :: osiris:offset().
-type epoch() :: osiris:epoch().

-type config() :: osiris:config() |
                  #{dir := file:filename(),
                    epoch => non_neg_integer(),
                    max_segment_size => non_neg_integer()}.

-type record() :: {offset(), iodata()}.
-type offset_spec() :: osiris:offset_spec().

%% holds static or rarely changing fields
-record(cfg, {directory :: file:filename(),
              max_size = ?DEFAULT_MAX_SEGMENT_SIZE_B :: non_neg_integer()
             }).


-record(read, {type :: data | offset,
               offset_ref :: undefined | atomics:atomics_ref(),
               next_offset = 0 :: offset()
              }).

-record(write, {segment_size = 0 :: non_neg_integer(),
                current_epoch :: non_neg_integer(),
                tail_info = {0, undefined} :: osiris:tail_info()
               }).

-record(?MODULE, {cfg :: #cfg{},
                  mode :: #read{} | #write{},
                  fd :: undefined | file:io_device(),
                  index_fd :: undefined | file:io_device()}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              % offset/0,
              % record/0,
              config/0
              ]).

-spec directory(osiris:config()) -> file:filename().
directory(#{name := Name} = Config) ->
    Dir = case Config of
              #{dir := D} -> D;
              _ ->
                  {ok, D} = application:get_env(osiris, data_dir),
                  D
          end,
    filename:join(Dir, Name).

-spec init(file:filename(), config()) -> state().
init(Dir, #{epoch := Epoch} = Config) ->
    %% scan directory for segments if in write mode
    %% re-builds segment lookup ETS table (used by readers)
    MaxSize = case Config of
                  #{max_segment_size := M} ->
                      M;
                  _ ->
                      ?DEFAULT_MAX_SEGMENT_SIZE_B
              end,
    ok = filelib:ensure_dir(Dir),
    case file:make_dir(Dir) of
        ok -> ok;
        {error, eexist} -> ok;
        E -> throw(E)
    end,
    {{NextOffset, LastEO} = TailInfo, File} = recover_tail_info(Dir),

    %% assert epoch is same or larger than last known epoch
    case LastEO of
        {LastE, _} when LastE > Epoch ->
            exit(invalid_epoch);
        _ -> ok
    end,

    error_logger:info_msg("~s:~s/~b: next offset ~b",
                          [?MODULE, ?FUNCTION_NAME,
                           ?FUNCTION_ARITY, NextOffset]),
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
    %% we need to manuall move the position to the end of the file else
    %% we'd write an incorrect file position into the index on next write
    {ok, _Pos} = file:position(Fd, eof),
    % error_logger:info_msg("osiris_segment:init/ scanning ~b Pos ~b",
    %                       [NextOffset, Pos]),
    #?MODULE{cfg = #cfg{directory = Dir,
                        max_size = MaxSize},
             mode = #write{tail_info = TailInfo,
                           current_epoch = Epoch
                          },
             fd = Fd,
             index_fd = IdxFd}.

-spec write([iodata() | {batch, non_neg_integer(), 0, iodata()}], state()) ->
    state().
write([], #?MODULE{mode = #write{}} = State) ->
    State;
write(Entries, #?MODULE{cfg = #cfg{},
                        mode = #write{current_epoch = Epoch,
                                      tail_info = {Next, _}}} = State) ->
    {ChunkData, NumRecords} = make_chunk(Entries, Epoch, Next),
    write_chunk(ChunkData, NumRecords, State).

-spec accept_chunk(iodata(), state()) -> state().
accept_chunk([<<?MAGIC:4/unsigned,
                ?VERSION:4/unsigned,
                _NumEntries:16/unsigned,
                NumRecords:32/unsigned,
                _Epoch:64/unsigned,
                Next:64/unsigned,
                _Crc:32/integer,
                _DataSize:32/unsigned,
                _/binary>> | _] = Chunk,
             #?MODULE{cfg = #cfg{},
                      mode = #write{tail_info = {Next, _}}} = State) ->
    write_chunk(Chunk, NumRecords, State);
accept_chunk(Binary, State)
  when is_binary(Binary) ->
    accept_chunk([Binary], State);
accept_chunk([<<?MAGIC:4/unsigned,
                ?VERSION:4/unsigned,
                _NumEntries:16/unsigned,
                _NumRecords:32/unsigned,
                _Epoch:64/unsigned,
                Next:64/unsigned,
                _Crc:32/integer,
                _DataSize:32/unsigned,
                _/binary>>
              | _] = _Chunk,
             #?MODULE{cfg = #cfg{},
                      mode = #write{tail_info = {ExpectedNext, _}}}) ->
    %% TODO if the expected next is higher than next we should truncate the tail
    %% until the entry can be written in the correct place
    %% when accepting we always trust the source to have negotiated the correct
    %% data
    exit({accept_chunk_out_of_order, Next, ExpectedNext}).


-spec next_offset(state()) -> offset().
next_offset(#?MODULE{mode = #write{tail_info = {Next, _}}}) ->
    Next;
next_offset(#?MODULE{mode = #read{next_offset = Next}}) ->
    Next.

-spec tail_info(state()) -> osiris:tail_info().
tail_info(#?MODULE{mode = #write{tail_info = TailInfo}}) ->
    TailInfo.

-spec init_data_reader(osiris:tail_info(), config()) ->
    {ok, state()} |
    {error, {offset_out_of_range, empty | {offset(), offset()}}} |
    {error, {invalid_last_offset_epoch, offset(), offset()}}.
init_data_reader({StartOffset, PrevEO}, #{dir := Dir} = _Config) ->
    SegInfo = build_log_overview(Dir),
    Range = range_from_segment_infos(SegInfo),
    error_logger:info_msg(
      "osiris_segment:init_data_reader/2 at ~b prev ~w range: ~w seg infos ~p",
      [StartOffset, PrevEO, Range, SegInfo]),
    %% Invariant:  there is always at least one segment left on disk
    case Range of
        {F, _L} when StartOffset < F ->
            %% if a lower than exisiting is request simply forward
            %% it to the first offset of the log
            %% in this case we cannot validate PrevEO - instead
            %% the replica should truncate all of it's exisiting log
            case find_segment_for_offset(F, SegInfo) of
                undefined ->
                    %% this is unexpected and thus an error
                    exit({segment_not_found, F, SegInfo});
                StartSegment ->
                    {ok, init_data_reader_from_segment(Dir, StartSegment, F)}
            end;
        empty when StartOffset > 0 ->
            {error, {offset_out_of_range, Range}};
        {_F, L} when StartOffset > L + 1 ->
            %% if we are trying to attach to anything larger than
            %% the next offset (i.e last +1) this is in out of range
            %% error
            {error, {offset_out_of_range, Range}};
        _ ->
            %% this assumes the offset is in range
            %% first we need to validate PrevEO
            case PrevEO of
                empty when StartOffset == 0 ->
                    case find_segment_for_offset(StartOffset, SegInfo) of
                        undefined ->
                            %% this is unexpected and thus an error
                            exit({segment_not_found, StartOffset, SegInfo});
                        StartSegment ->
                            {ok, init_data_reader_from_segment(Dir, StartSegment, StartOffset)}
                    end;
                {PrevE, PrevO} ->
                    case find_segment_for_offset(PrevO, SegInfo) of
                        undefined ->
                            %% this is unexpected and thus an error
                            {error, {invalid_last_offset_epoch, PrevE, unknown}};
                        PrevSeg ->
                            %% prev segment exists, does it have the correct
                            %% epoch?
                            {ok, Fd} = file:open(PrevSeg, [raw, binary, read]),
                            PrevIndexFile = filename:rootname(PrevSeg) ++ ".index",
                            {ok, Data} = file:read_file(PrevIndexFile),
                            %% TODO: next offset needs to be a chunk offset
                            {_, FilePos} = scan_index(Data, PrevO),
                            {ok, FilePos} = file:position(Fd, FilePos),
                            case file:read(Fd, ?HEADER_SIZE_B) of
                                {ok, <<?MAGIC:4/unsigned,
                                       ?VERSION:4/unsigned,
                                       _NumEntries:16/unsigned,
                                       _NumRecords:32/unsigned,
                                       PrevE:64/unsigned,
                                       PrevO:64/unsigned,
                                       _Crc:32/integer,
                                       _DataSize:32/unsigned>>} ->
                                    ok = file:close(Fd),
                                    {ok, init_data_reader_from_segment(
                                           Dir, find_segment_for_offset(StartOffset, SegInfo), StartOffset)};
                                {ok, <<?MAGIC:4/unsigned,
                                       ?VERSION:4/unsigned,
                                       _NumEntries:16/unsigned,
                                       _NumRecords:32/unsigned,
                                       OtherE:64/unsigned,
                                       PrevO:64/unsigned,
                                       _Crc:32/integer,
                                       _DataSize:32/unsigned>>} ->
                                    ok = file:close(Fd),
                                    {error, {invalid_last_offset_epoch, PrevE, OtherE}}
                            end
                    end
            end
    end.
    %% TODO we only need to validate if the attach request is in range
    %% if NextOffs has been forwarded there is no point in validating as the
    %% entry will not exist
    %% also when start offset is the at the exact lower bound of the range
    %% we cannot validate as the prior entry did not exist
    %% in this case the replica should truncate but how do we signal that?
    %% perhaps we should fail validation in this scenario

init_data_reader_from_segment(Dir, StartSegment, NextOffs) ->
    {ok, Fd} = file:open(StartSegment, [raw, binary, read]),
    IndexFile = filename:rootname(StartSegment) ++ ".index",
    {ok, Data} = file:read_file(IndexFile),
    %% TODO: next offset needs to be a chunk offset
    {_, FilePos} = scan_index(Data, NextOffs),
    {ok, Pos} = file:position(Fd, FilePos),
    error_logger:info_msg(
      "osiris_segment:init_data_reader/2 at ~b file pos ~b/~w",
      [NextOffs, Pos, FilePos]),
    #?MODULE{cfg = #cfg{directory = Dir},
             mode = #read{type = data,
                          next_offset = NextOffs},
             fd = Fd}.


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
    {error, {offset_out_of_range,
             empty | {From :: offset(), To :: offset()}}}.
init_offset_reader({abs, Offs}, #{dir := Dir} = Conf) ->
    %% TODO: some unnecessary computation here
    build_log_overview(Dir),
    Range = range_from_segment_infos(build_log_overview(Dir)),
    case Range of
        empty ->
            {error, {offset_out_of_range, Range}};
        {S, E} when Offs < S orelse Offs > E ->
            {error, {offset_out_of_range, Range}};
        _ ->
            %% it is in range, convert to standard offset
            init_offset_reader(Offs, Conf)
    end;
init_offset_reader(OffsetSpec, #{dir := Dir,
                                 offset_ref := OffsetRef}) ->
    SegInfo = build_log_overview(Dir),
    Range = range_from_segment_infos(SegInfo),
    StartOffset = case {OffsetSpec, Range} of
                      {_, empty} ->
                          0;
                      {first, {F, _}} ->
                          F;
                      {last, {_, L}}  ->
                          L;
                      {next, {_, L}}  ->
                          L + 1;
                      {Offset, {S, E}} when is_integer(Offset) ->
                          max(S, min(Offset, E + 1))
                  end,
    %% find the appopriate segment and scan the index to find the
    %% postition of the next chunk to read
    error_logger:info_msg(
      "osiris_segment:init_offset_reader/2 spec requested ~w at ~b seg infos ~p",
      [OffsetSpec, StartOffset, SegInfo]),
    case find_segment_for_offset(StartOffset, SegInfo) of
        undefined ->
            {error, {offset_out_of_range, Range}};
        StartSegment ->
            {ok, Fd} = file:open(StartSegment, [raw, binary, read]),
            IndexFile = filename:rootname(StartSegment) ++ ".index",
            {ok, Data} = file:read_file(IndexFile),
            {ChOffs, FilePos} = scan_index(Data, StartOffset),
            {ok, Pos} = file:position(Fd, FilePos),
            error_logger:info_msg(
                    "osiris_segment:init_offset_reader/2 at ~b file pos ~b/~w",
                    [StartOffset, Pos, FilePos]),
            {ok, #?MODULE{cfg = #cfg{directory = Dir},
                          mode = #read{type = offset,
                                       offset_ref = OffsetRef,
                                       next_offset = ChOffs},
                          fd = Fd}}
    end.

-spec get_offset_ref(state()) -> atomics:atomics_ref().
get_offset_ref(#?MODULE{mode = #read{offset_ref = Ref}}) ->
    Ref.

-spec get_current_epoch(state()) -> non_neg_integer().
get_current_epoch(#?MODULE{mode = #write{current_epoch = Epoch}}) ->
    Epoch.

-spec read_chunk(state()) ->
    {ok, {offset(), epoch(),
          HeaderData :: iodata(),
          RecordData :: iodata()}, state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk(#?MODULE{cfg = #cfg{directory = Dir},
                    mode = #read{next_offset = Next} = Read,
                    fd = Fd
                   } = State) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    case can_read_next_offset(Read) of
        true ->
            case file:read(Fd, ?HEADER_SIZE_B) of
                {ok, <<?MAGIC:4/unsigned,
                       ?VERSION:4/unsigned,
                       _NumEntries:16/unsigned,
                       NumRecords:32/unsigned,
                       Epoch:64/unsigned,
                       Offs:64/unsigned,
                       _Crc:32/integer,
                       DataSize:32/unsigned>> = HeaderData} ->
                    {ok, BlobData} = file:read(Fd, DataSize),
                    {ok, {Offs, Epoch, HeaderData, BlobData},
                     State#?MODULE{mode = incr_next_offset(NumRecords, Read)}};
                {ok, _} ->
                    %% set the position back for the next read
                    % error_logger:info_msg("osiris_segment end coff~w", [COffs]),
                    {ok, _} = file:position(Fd, {cur, -?HEADER_SIZE_B}),
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
            end;
        false ->
            {end_of_stream, State}
    end.

-spec read_chunk_parsed(state()) ->
    {[record()], state()} |
    {end_of_stream, state()} |
    {error, {invalid_chunk_header, term()}}.
read_chunk_parsed(#?MODULE{} = State0) ->
    %% reads the next chunk of entries, parsed
    %% NB: this may return records before the requested index,
    %% that is fine - the reading process can do the appropriate filtering
    case read_chunk(State0) of
        {ok, {Offs, _Epoch, _Header, Data}, State} ->
            %% parse blob data into records
            Records = parse_records(Offs, Data, []),
            {Records, State};
        Ret ->
            Ret
    end.

-spec send_file(gen_tcp:socket(), state()) ->
    {ok, state()} | {end_of_stream, state()}.
send_file(Sock, State) ->
    send_file(Sock, State, fun empty_function/1).

-spec send_file(gen_tcp:socket(), state(),
                fun((non_neg_integer()) -> none())) ->
    {ok, state()} | {end_of_stream, state()}.
send_file(Sock, #?MODULE{cfg = #cfg{directory = Dir},
                         mode = #read{next_offset = NextOffs} = Read,
                         fd = Fd} = State,
         Callback) ->
    case can_read_next_offset(Read) of
        true ->
            {ok, Pos} = file:position(Fd, cur),
            case file:read(Fd, ?HEADER_SIZE_B) of
                {ok, <<?MAGIC:4/unsigned,
                       ?VERSION:4/unsigned,
                       _NumEntries:16/unsigned,
                       NumRecords:32/unsigned,
                       _Epoch:64/unsigned,
                       NextOffs:64/unsigned,
                       _Crc:32/integer,
                       DataSize:32/unsigned>>} ->
                    %% read header
                    ToSend = DataSize + ?HEADER_SIZE_B,
                    %% used to write frame headers to socket
                    Callback(ToSend),
                    ok = sendfile(Fd, Sock, Pos, ToSend),
                    FilePos = Pos + ToSend,
                    %% sendfile doesn't increment the file descriptor position
                    {ok, FilePos} = file:position(Fd, FilePos),
                    {ok, State#?MODULE{mode = incr_next_offset(NumRecords, Read)}};
                {ok, B} when byte_size(B) < ?HEADER_SIZE_B ->
                    %% partial data available
                    %% reset and wait for update
                    {ok, Pos} = file:position(Fd, Pos),
                    {end_of_stream, State};
                eof ->
                    %% open next segment file and start there if it exists
                    SegFile = make_file_name(NextOffs, "segment"),
                    case file:open(filename:join(Dir, SegFile),
                                   [raw, binary, read]) of
                        {ok, Fd2} ->
                            % error_logger:info_msg("sendfile eof ~w", [NextOffs]),
                            ok = file:close(Fd),
                            send_file(Sock, State#?MODULE{fd = Fd2}, Callback);
                        {error, enoent} ->
                            {end_of_stream, State}
                    end
            end;
        false ->
            {end_of_stream, State}
    end.


close(_State) ->
    %% close fd
    ok.

delete_directory(Config) ->
    Dir = directory(Config),
    {ok, Files} = file:list_dir(Dir),
    [ok = file:delete(filename:join(Dir, F)) || F <- Files],
    ok = file:del_dir(Dir).

%% Internal

scan_index(<<>>, Offset) ->
    %% if the index is empty do we really know the offset will be next
    %% this relies on us always reducing the Offset to within the log range
    {Offset, 0};
scan_index(<<O:64/unsigned,
             Num:32/unsigned,
             Pos:32/unsigned>>, Offset) ->
    %% TODO: the requested offset may not be found in the last batch as it
    %% may not have been written yet
    error_logger:info_msg("scan_index scanning ~b Pos ~b", [O, Pos]),
    case Offset >= O andalso Offset < (O + Num) of
        true ->
            {O, Pos};
        false ->
            {O + Num, eof}
    end;
scan_index(<<O:64/unsigned,
             _:32/unsigned,
             Pos:32/unsigned, Rem/binary>>, Offset)  ->
    <<ONext:64/unsigned, _/binary>> = Rem,
    error_logger:info_msg("scan_index scanning ~b Pos ~b", [O, Pos]),
     case Offset >= O andalso Offset < ONext of
         true ->
             {O, Pos};
         false ->
             scan_index(Rem, Offset)
     end.

parse_records(_Offs, <<>>, Acc) ->
    %% TODO: this could probably be changed to body recursive
    lists:reverse(Acc);
parse_records(Offs, <<0:1, %% simple
                      Len:31/unsigned,
                      Data:Len/binary,
                      Rem/binary>>, Acc) ->
    parse_records(Offs+1, Rem, [{Offs, Data} | Acc]);
parse_records(Offs, <<1:1, %% simple
                      0:3/unsigned, %% compression type
                      _:4/unsigned, %% reserved
                      NumRecs:16/unsigned,
                      Len:32/unsigned,
                      Data:Len/binary,
                      Rem/binary>>, Acc) ->
    Recs = parse_records(Offs, Data, []),
    parse_records(Offs+NumRecs, Rem, lists:reverse(Recs) ++ Acc).

recover_tail_info(Dir) when is_list(Dir) ->
    IdxFiles = lists:reverse(lists:sort(
                               filelib:wildcard(
                                 filename:join(Dir, "*.index")))),
    recover_tail_info0(0, IdxFiles).

recover_tail_info0(NextOffs, []) ->
    {{NextOffs, empty}, undefined};
recover_tail_info0(NextOffs, [IdxFile | Rem]) ->
    {ok, Data} = file:read_file(IdxFile),
    case Data of
        <<>> ->
            %% read prevous if exists
            recover_tail_info0(NextOffs, Rem);
        Data ->
            %% get last batch offset
            %% %% TODO validate byte size is a multiple of index record size
            Pos = byte_size(Data) - ?INDEX_RECORD_SIZE_B,
            <<Offset:64/unsigned,
              Num:32/unsigned,
              FilePos:32/unsigned>> = binary:part(Data, Pos,
                                                  ?INDEX_RECORD_SIZE_B),
            Basename = filename:basename(IdxFile, ".index"),
            BaseDir = filename:dirname(IdxFile),
            SegFile0 = filename:join([BaseDir, Basename]),
            SegFile = SegFile0 ++ ".segment",
            {ok, Fd} = file:open(SegFile, [read, binary, raw]),
            {ok, FilePos} = file:position(Fd, FilePos),
            {ok, <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   _NumEntries:16/unsigned,
                   _NumRecords:32/unsigned,
                   Epoch:64/unsigned>>} = file:read(Fd, 15),
            file:close(Fd),
            %% reader batch header
            {{Offset + Num, {Epoch, Offset}},
             filename:join(BaseDir, Basename)}
    end.

build_log_overview(Dir) when is_list(Dir) ->
    IdxFiles = lists:sort(filelib:wildcard(
                            filename:join(Dir, "*.index"))),
    build_log_overview0(IdxFiles, []).


build_log_overview0([], Acc) ->
    lists:reverse(Acc);
build_log_overview0([IdxFile | IdxFiles], Acc0) ->
    %% TODO: make this more efficient, index files could become very large
    {ok, Data} = file:read_file(IdxFile),
    case Data of
        <<>> when IdxFiles == [] andalso Acc0 == [] ->
            %% special case for the very first empty segment
            SegFile = segment_from_index_file(IdxFile),
            [{SegFile, undefined, undefined}];
        <<>>->
            %% read prevous if exists
            build_log_overview0(IdxFiles, Acc0);
        Data ->
            %% get last batch offset
            IdxSize = byte_size(Data),
            %% ASSERTION: byte size is a multiple of index record size
            0 = IdxSize rem ?INDEX_RECORD_SIZE_B,
            %% TODO: truncate partial index entry or rebuild from segment file
            %% when byte size has a remainder
            LastIdxRecordPos = IdxSize - ?INDEX_RECORD_SIZE_B,
            <<_Offset:64/unsigned,
              _Num:32/unsigned,
              LastChunkPos:32/unsigned>> = binary:part(Data, LastIdxRecordPos,
                                                       ?INDEX_RECORD_SIZE_B),
            SegFile = segment_from_index_file(IdxFile),
            {ok, Fd} = file:open(SegFile, [read, binary, raw]),
            {ok, <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   _NumEntries:16/unsigned,
                   FirstNumRecords:32/unsigned,
                   FirstEpoch:64/unsigned,
                   FirstOffs:64/unsigned, _/binary>>} = file:read(Fd, ?HEADER_SIZE_B),
            {ok, LastChunkPos} = file:position(Fd, LastChunkPos),
            {ok, <<?MAGIC:4/unsigned,
                   ?VERSION:4/unsigned,
                   _LastNumEntries:16/unsigned,
                   LastNumRecords:32/unsigned,
                   LastEpoch:64/unsigned,
                   LastOffs:64/unsigned, _/binary>>} = file:read(Fd, ?HEADER_SIZE_B),
            ok = file:close(Fd),
            Acc = [{SegFile,
                    {FirstEpoch, FirstOffs, FirstNumRecords},
                    {LastEpoch, LastOffs, LastNumRecords}} | Acc0],
            build_log_overview0(IdxFiles, Acc)
    end.

segment_from_index_file(IdxFile) ->
    Basename = filename:basename(IdxFile, ".index"),
    BaseDir = filename:dirname(IdxFile),
    SegFile0 = filename:join([BaseDir, Basename]),
    SegFile0 ++ ".segment".

make_chunk(Blobs, Epoch, Next) ->
    {NumRecords, IoList} =
    lists:foldr(fun ({batch, NumRecords, CompType, B}, {Count, Acc}) ->
                        Data = [<<1:1, %% batch record type
                                  CompType:3/unsigned,
                                  0:4/unsigned,
                                  NumRecords:16/unsigned,
                                  (iolist_size(B)):32/unsigned>>, B],
                        {Count+NumRecords, [Data | Acc]};
                    (B, {Count, Acc}) ->
                        Data = [<<0:1, %% simple record type
                                  (iolist_size(B)):31/unsigned>>, B],
                        {Count+1, [Data | Acc]}
                end, {0, []}, Blobs),
    Bin = IoList,
    Size = erlang:iolist_size(Bin),
    {[<<?MAGIC:4/unsigned,
        ?VERSION:4/unsigned,
        (length(Blobs)):16/unsigned,
        NumRecords:32/unsigned,
        Epoch:64/unsigned,
        Next:64/unsigned,
        0:32/integer,
        Size:32/unsigned>>,
      Bin], NumRecords}.

write_chunk(Chunk, NumRecords,
            #?MODULE{fd = undefined} = State) ->
    write_chunk(Chunk, NumRecords, open_new_segment(State));
write_chunk(Chunk, NumRecords,
            #?MODULE{cfg = #cfg{max_size = MaxSize},
                     fd = Fd,
                     index_fd = IdxFd,
                     mode = #write{segment_size = SegSize,
                                   current_epoch = Epoch,
                                   tail_info = {Next, _}} = Write} = State) ->
    NextOffset = Next + NumRecords,
    Size = erlang:iolist_size(Chunk),
    {ok, Cur} = file:position(Fd, cur),
    ok = file:write(Fd, Chunk),
    ok = file:write(IdxFd, <<Next:64/unsigned,
                             NumRecords:32/unsigned,
                             Cur:32/unsigned>>),
    case file:position(Fd, cur) of
        {ok, After} when After >= MaxSize ->
            %% close the current file
            ok = file:close(Fd),
            ok = file:close(IdxFd),
            State#?MODULE{fd = undefined,
                          index_fd = undefined,
                          mode = Write#write{
                                   tail_info = {NextOffset, {Epoch, Next}},
                                   segment_size = 0}};
        {ok, _} ->
            State#?MODULE{mode = Write#write{tail_info = {NextOffset, {Epoch, Next}},
                                             segment_size = SegSize + Size}}
    end.


sendfile(_Fd, _Sock, _Pos, 0) ->
    ok;
sendfile(Fd, Sock, Pos, ToSend) ->
    case file:sendfile(Fd, Sock, Pos, ToSend, []) of
        {ok, 0} ->
            %% TODO add counter for this?
            % error_logger:info_msg("sendfile sent 0 out of ~b bytes~n",
            %                       [ToSend]),
            % timer:sleep(1),
            % erlang:yield(),
            sendfile(Fd, Sock, Pos, ToSend);
        {ok, BytesSent} ->
            sendfile(Fd, Sock, Pos + BytesSent, ToSend - BytesSent)
    end.

range_from_segment_infos([{_SegFile,
                           undefined, undefined}]) ->
    empty;
range_from_segment_infos([{_SegFile,
                           {_E, FirstOff, _},
                           {_, LastOff, LastNumRecs}}]) ->
    {FirstOff, LastOff + LastNumRecs - 1};
range_from_segment_infos([{_SegFile,
                           {_E, FirstOff, _},
                           {_, _, _}} | Rem]) ->
    {_, _, {_, LastOff, LastNumRecs}} = hd(lists:reverse(Rem)),
    {FirstOff, LastOff + LastNumRecs - 1}.

%% find the segment the offset is in _or_ if the offset is the very next
%% chunk offset it will return the last segment
find_segment_for_offset(0, [{SegFile, undefined, undefined}]) ->
    SegFile;
find_segment_for_offset(Offset,
                        [{SegFile,
                          {_E, _FirstOff, _},
                          {_, LastChOff, LastNumRecs}}])
  when Offset == LastChOff + LastNumRecs ->
    %% the last segment and offset is the next offset
    SegFile;
find_segment_for_offset(Offset,
                         [{SegFile,
                           {_E, FirstOff, _},
                           {_, LastChOff, LastNumRecs}} | Rem]) ->
    NextOffset = LastChOff + LastNumRecs,
    case Offset >= FirstOff andalso Offset < NextOffset  of
        true ->
            %% we found it
            SegFile;
        false ->
            find_segment_for_offset(Offset, Rem)
    end;
find_segment_for_offset(_Offset, _) ->
    undefined.

empty_function(_) ->
  ok.

can_read_next_offset(#read{type = offset,
                           next_offset = NextOffset,
                           offset_ref = Ref}) ->
    atomics:get(Ref, 1) >= NextOffset;
can_read_next_offset(#read{type = data}) ->
    true.

incr_next_offset(Num, #read{next_offset = NextOffset} = Read) ->
    Read#read{next_offset = NextOffset + Num}.

make_file_name(N, Suff) ->
    lists:flatten(io_lib:format("~20..0B.~s", [N, Suff])).

open_new_segment(#?MODULE{cfg = #cfg{directory = Dir},
                          fd = undefined,
                          index_fd = undefined,
                          mode = #write{segment_size = _SegSize,
                                        tail_info = {NextOffset, _}}} = State) ->
            Filename = make_file_name(NextOffset, "segment"),
            IdxFilename = make_file_name(NextOffset, "index"),
            {ok, Fd} = file:open(filename:join(Dir, Filename),
                                 [raw, binary, append]),
            {ok, IdxFd} = file:open(filename:join(Dir, IdxFilename),
                                     [raw, binary, append]),
            State#?MODULE{fd = Fd,
                          index_fd = IdxFd}.
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
