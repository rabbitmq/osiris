-module(osiris_file).

-export([
         advise/4,
         close/1,
         copy/2,
         del_dir/1,
         delete/1,
         ensure_dir/1,
         list_dir/1,
         make_dir/1,
         open/2,
         position/2,
         pread/3,
         prim_delete/1,
         read/2,
         read_file_info/1,
         sendfile/5,
         truncate/1,
         write/2
        ]).

-callback advise(Handle :: term(), Offset :: non_neg_integer(),
                 Length :: non_neg_integer(), Advise :: term()) ->
    ok | {error, Reason :: term()}.

-callback close(Handle :: term()) ->
    ok | {error, Reason :: term()}.

-callback copy(Source :: file:filename_all(), Destination :: file:filename_all()) ->
    {ok, BytesCopied :: non_neg_integer()} | {error, Reason :: term()}.

-callback del_dir(Dir :: file:filename_all()) ->
    ok | {error, Reason :: term()}.

-callback delete(File :: file:filename_all()) ->
    ok | {error, Reason :: term()}.

-callback make_dir(Dir :: file:filename_all()) ->
    ok | {error, Reason :: term()}.

-callback ensure_dir(Dir :: file:filename_all()) ->
    ok | {error, Reason :: term()}.

-callback list_dir(Dir :: file:filename_all()) ->
    {ok, [file:filename()]} | {error, Reason :: term()}.

-callback open(File :: file:filename_all(), Options :: list()) ->
    {ok, Handle :: term()} | {error, Reason :: term()}.

-callback position(Handle :: term(), Position :: file:position()) ->
    {ok, NewPosition :: non_neg_integer()} | {error, Reason :: term()}.

-callback pread(Handle :: term(), Position :: non_neg_integer(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | eof | {error, Reason :: term()}.

-callback read(Handle :: term(), Size :: non_neg_integer()) ->
    {ok, Data :: binary()} | eof | {error, Reason :: term()}.

-callback read_file_info(File :: file:filename_all()) ->
    {ok, file:file_info()} | {error, Reason :: term()}.

-callback sendfile(Handle :: term(), Socket :: term(),
                   Offset :: non_neg_integer(), Length :: non_neg_integer(),
                   Options :: list()) ->
    {ok, BytesSent :: non_neg_integer()} | {error, Reason :: term()}.

-callback truncate(Handle :: term()) ->
    ok | {error, Reason :: term()}.

-callback write(Handle :: term(), Data :: iodata()) ->
    ok | {error, Reason :: term()}.

-optional_callbacks([write/2]).

-type file_handle() :: {module(), term()} | file:io_device().

-type posix_file_advise() :: normal |
                             sequential |
                             random |
                             no_reuse |
                             will_need |
                             dont_need.

-spec advise(Handle, Offset, Length, Advise) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Offset :: integer(),
      Length :: integer(),
      Advise :: posix_file_advise(),
      Reason :: file:posix() | badarg.

advise({Mod, Handle}, Offset, Length, Advise) ->
    Mod:advise(Handle, Offset, Length, Advise);
advise(Handle, Offset, Length, Advise) ->
    file:advise(Handle, Offset, Length, Advise).


-spec close(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

close({Mod, Handle}) ->
    Mod:close(Handle);
close(Handle) ->
    file:close(Handle).


-spec copy(file:filename_all(), file:filename_all()) ->
          {ok, non_neg_integer()} | {error, term()}.
%% TODO
copy(Source, Destination) ->
    file:copy(Source, Destination).


-spec del_dir(file:filename_all()) ->
          ok | {error, term()}.
%% TODO
%% Used when a queue is deleted, should perhaps move the entire osiris_log:delete_directory, and
%% let the Mod handle deletion of storage on its side too.
del_dir(Dir) ->
    Mod = get_mod(),
    Mod:del_dir(Dir).


-spec delete(file:filename_all()) ->
          ok | {error, term()}.
%% Do we need the prim_* function calls?
delete(File) ->
    Mod = get_mod(),
    Mod:delete(File).

-spec prim_delete(file:filename_all()) ->
          ok | {error, term()}.

prim_delete(File) ->
    Mod = get_mod(prim_file),
    Mod:delete(File).


-spec ensure_dir(file:filename_all()) ->
          ok | {error, term()}.
%% Only used for local files
ensure_dir(Dir) ->
    filelib:ensure_dir(Dir).


-spec list_dir(file:filename_all()) ->
          {ok, [file:filename()]} | {error, term()}.
%% TODO
list_dir(Dir) ->
    Mod = get_mod(prim_file),
    Mod:list_dir(Dir).


-spec make_dir(file:filename_all()) ->
          ok | {error, term()}.
%% Only used for the local segment file, no need to change it.
make_dir(Dir) ->
    file:make_dir(Dir).


-spec open(File, Modes) -> {ok, file_handle()} | {error, Reason} when
      File :: Filename | file:iodata(),
      Filename :: file:name_all(),
      Modes :: [file:mode() | ram | directory],
      Reason :: file:posix() | badarg | system_limit.
open(File, Options) ->
    case lists:member(write, Options) of
        true ->
            %% We do not use tiered storage for writes
            file:open(File, Options);
        false ->
            %% Here we will get the correct Mod based on config/manifest file etc.
            Mod = get_mod(),
            {ok, Fd} = Mod:open(File, Options),
            {ok, {Mod, Fd}}
    end.


-spec position(Handle, Location) -> {ok, NewPosition} | {error, Reason} when
      Handle :: file_handle(),
      Location :: file:location(),
      NewPosition :: integer(),
      Reason :: file:posix() | badarg | terminated.

position({Mod, Handle}, Position) ->
    Mod:position(Handle, Position);
position(Handle, Position) ->
    file:position(Handle, Position).


-spec pread(Handle, Location, Number) ->
          {ok, Data} | eof | {error, Reason} when
      Handle :: file_handle(),
      Location :: file:location(),
      Number :: non_neg_integer(),
      Data :: string() | binary(),
      Reason :: file:posix() | badarg | terminated.

pread({Mod, Handle}, Position, Size) ->
    Mod:pread(Handle, Position, Size);
pread(Handle, Position, Size) ->
    file:pread(Handle, Position, Size).


-spec read(Handle, Number) -> {ok, Data} | eof | {error, Reason} when
      Handle :: file_handle() | io:device(),
      Number :: non_neg_integer(),
      Data :: string() | binary(),
      Reason :: file:posix()
              | badarg
              | terminated
              | {no_translation, unicode, latin1}.

read({Mod, Handle}, Size) ->
    Mod:read(Handle, Size);
read(Handle, Size) ->
    file:read(Handle, Size).


-spec read_file_info(file:filename_all()) ->
          {ok, file:file_info()} | {error, term()}.
%% Todo
read_file_info(File) ->
    Mod = get_mod(prim_file),
    Mod:read_file_info(File).


-type sendfile_option() :: {chunk_size, non_neg_integer()}
                         | {use_threads, boolean()}.
-spec sendfile(Handle, Socket, Offset, Bytes, Opts) ->
          {'ok', non_neg_integer()} | {'error', inet:posix() |
                                       closed | badarg | not_owner} when
      Handle :: file_handle(),
      Socket :: inet:socket() | socket:socket() |
                fun ((iolist()) -> ok | {error, inet:posix() | closed}),
      Offset :: non_neg_integer(),
      Bytes :: non_neg_integer(),
      Opts :: [sendfile_option()].

sendfile({Mod, Handle}, Socket, Offset, Length, Options) ->
    Mod:sendfile(Handle, Socket, Offset, Length, Options);
sendfile(Handle, Socket, Offset, Length, Options) ->
    file:sendfile(Handle, Socket, Offset, Length, Options).


-spec truncate(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

truncate({Mod, Handle}) ->
    Mod:truncate(Handle);
truncate(Handle) ->
    file:truncate(Handle).


-spec write(Handle, Bytes) -> ok | {error, Reason} when
      Handle :: file_handle() | io:device(),
      Bytes :: iodata(),
      Reason :: file:posix() | badarg | terminated.
write({Mod, Handle}, Data) ->
    Mod:write(Handle, Data);
write(Handle, Data) ->
    file:write(Handle, Data).

%% -spec try_write(module(), term(), iodata()) ->
%%     ok | {error, term()}.
%% try_write(Mod, Handle, Data) ->
%%     case erlang:function_exported(Mod, write, 2) of
%%         true ->
%%             Mod:write(Handle, Data);
%%         false ->
%%             file:write(Handle, Data)
%%     end.


-spec get_mod() -> module().

get_mod() ->
    get_mod(file).

get_mod(file) ->
    %% TODO. This will figure out the correct module to use, based on
    %% info in the magical manifest file.
    file;
get_mod(prim_file) ->
    %% Just temporary solutin till I figure out why
    %% we even use prim_file?
    prim_file.
