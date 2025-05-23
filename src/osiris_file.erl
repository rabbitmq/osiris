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


-type file_handle() :: {module(), term()} | file:io_device().

-type posix_file_advise() :: normal |
                             sequential |
                             random |
                             no_reuse |
                             will_need |
                             dont_need.

-type sendfile_option() :: {chunk_size, non_neg_integer()}
                         | {use_threads, boolean()}.


-callback advise(Handle, Offset, Length, Advise) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Offset :: integer(),
      Length :: integer(),
      Advise :: posix_file_advise(),
      Reason :: file:posix() | badarg.

-callback close(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

-callback copy(Source, Destination) -> {ok, BytesCopied} | {error, Reason} when
      Source :: file_handle() | Filename | {Filename, Modes},
      Destination :: file_handle() | Filename | {Filename, Modes},
      Filename :: file:name_all(),
      Modes :: [file:mode()],
      BytesCopied :: non_neg_integer(),
      Reason :: file:posix() | badarg | terminated.

-callback del_dir(Dir) -> ok | {error, Reason} when
      Dir :: file:name_all(),
      Reason :: file:posix() | badarg.

-callback delete(Filename) -> ok | {error, Reason} when
      Filename :: file:name_all(),
      Reason :: file:posix() | badarg.

-callback list_dir(Dir) -> {ok, Filenames} | {error, Reason} when
      Dir :: file:name_all(),
      Filenames :: [file:filename()],
      Reason :: file:posix()
              | badarg
              | {no_translation, Filename :: unicode:latin1_binary()}.

-callback open(File, Modes) -> {ok, file_handle()} | {error, Reason} when
      File :: Filename | file:iodata(),
      Filename :: file:name_all(),
      Modes :: [file:mode() | ram | directory],
      Reason :: file:posix() | badarg | system_limit.

-callback position(Handle, Location) -> {ok, NewPosition} | {error, Reason} when
      Handle :: file_handle(),
      Location :: file:location(),
      NewPosition :: integer(),
      Reason :: file:posix() | badarg | terminated.

-callback pread(Handle, Location, Number) ->
    {ok, Data} | eof | {error, Reason} when
      Handle :: file_handle(),
      Location :: file:location(),
      Number :: non_neg_integer(),
      Data :: string() | binary(),
      Reason :: file:posix() | badarg | terminated.

-callback read(Handle, Number) -> {ok, Data} | eof | {error, Reason} when
      Handle :: file_handle() | io:device(),
      Number :: non_neg_integer(),
      Data :: string() | binary(),
      Reason :: file:posix()
              | badarg
              | terminated
              | {no_translation, unicode, latin1}.

-callback read_file_info(File) -> {ok, FileInfo} | {error, Reason} when
      File :: file:name_all() | file_handle(),
      FileInfo :: file:file_info(),
      Reason :: file:posix() | badarg.

-callback sendfile(Handle, Socket, Offset, Bytes, Opts) ->
    {ok, non_neg_integer()} | {error, inet:posix() |
                               closed | badarg | not_owner} when
      Handle :: file_handle(),
      Socket :: inet:socket() | socket:socket() |
                fun ((iolist()) -> ok | {error, inet:posix() | closed}),
                    Offset :: non_neg_integer(),
                    Bytes :: non_neg_integer(),
                    Opts :: [sendfile_option()].

-callback truncate(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

-callback write(Handle, Bytes) -> ok | {error, Reason} when
      Handle :: file_handle() | io:device(),
      Bytes :: iodata(),
      Reason :: file:posix() | badarg | terminated.

-optional_callbacks([write/2]).

-define(DEFAULT_FILE, osiris_file_default).

-spec advise(Handle, Offset, Length, Advise) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Offset :: integer(),
      Length :: integer(),
      Advise :: posix_file_advise(),
      Reason :: file:posix() | badarg.

advise({Mod, Handle}, Offset, Length, Advise) ->
    Mod:advise(Handle, Offset, Length, Advise);
advise(Handle, Offset, Length, Advise) ->
    ?DEFAULT_FILE:advise(Handle, Offset, Length, Advise).


-spec close(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

close({Mod, Handle}) ->
    Mod:close(Handle);
close(Handle) ->
    ?DEFAULT_FILE:close(Handle).


-spec copy(Source, Destination) -> {ok, BytesCopied} | {error, Reason} when
      Source :: file_handle() | Filename | {Filename, Modes},
      Destination :: file_handle() | Filename | {Filename, Modes},
      Filename :: file:name_all(),
      Modes :: [file:mode()],
      BytesCopied :: non_neg_integer(),
      Reason :: file:posix() | badarg | terminated.
%% TODO
copy(Source, Destination) ->
    ?DEFAULT_FILE:copy(Source, Destination).

-spec del_dir(Dir) -> ok | {error, Reason} when
      Dir :: file:name_all(),
      Reason :: file:posix() | badarg.
%% TODO
%% Used when a queue is deleted, should perhaps move the entire osiris_log:delete_directory, and
%% let the Mod handle deletion of storage on its side too.
del_dir(Dir) ->
    Mod = get_mod(),
    Mod:del_dir(Dir).


-spec delete(Filename) -> ok | {error, Reason} when
      Filename :: file:name_all(),
      Reason :: file:posix() | badarg.
%% Do we need the prim_* function calls?
delete(File) ->
    Mod = get_mod(),
    Mod:delete(File).


-spec prim_delete(Filename) -> ok | {error, Reason} when
      Filename :: file:name_all(),
      Reason :: file:posix() | badarg.

prim_delete(File) ->
    Mod = get_mod(prim_file, File),
    Mod:delete(File).


-spec ensure_dir(Name) -> ok | {error, Reason} when
      Name :: file:name_all(),
      Reason :: file:posix().
%% Only used for local files
ensure_dir(Dir) ->
    ?DEFAULT_FILE:ensure_dir(Dir).


-spec list_dir(Dir) -> {ok, Filenames} | {error, Reason} when
      Dir :: file:name_all(),
      Filenames :: [file:filename()],
      Reason :: file:posix()
              | badarg
              | {no_translation, Filename :: unicode:latin1_binary()}.
%% TODO
list_dir(Dir) ->
    Mod = get_mod(prim_file),
    Mod:list_dir(Dir).


-spec make_dir(Dir) -> ok | {error, Reason} when
      Dir :: file:name(),
      Reason :: file:posix() | badarg.
%% Only used for the local segment file, no need to change it.
make_dir(Dir) ->
    ?DEFAULT_FILE:make_dir(Dir).


-spec open(File, Modes) -> {ok, file_handle()} | {error, Reason} when
      File :: Filename | file:iodata(),
      Filename :: file:name_all(),
      Modes :: [file:mode() | ram | directory],
      Reason :: file:posix() | badarg | system_limit.
open(File, Options) ->
    case lists:member(write, Options) of
        true ->
            %% We do not use tiered storage for writes
            ?DEFAULT_FILE:open(File, Options);
        false ->
            %% Here we will get the correct Mod based on config/manifest file etc.
            Mod = get_mod(File),
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
    ?DEFAULT_FILE:position(Handle, Position).


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
    ?DEFAULT_FILE:read(Handle, Size).


-spec read_file_info(File) -> {ok, FileInfo} | {error, Reason} when
      File :: file:name_all() | file_handle(),
      FileInfo :: file:file_info(),
      Reason :: file:posix() | badarg.
%% Todo
read_file_info(File) ->
    Mod = get_mod(prim_file, File),
    Mod:read_file_info(File).


-spec sendfile(Transport, Handle, Socket, Offset, Bytes) ->
          ok | {error, inet:posix() |
                closed | badarg | not_owner} when
      Transport :: tcp | ssl,
      Handle :: file_handle(),
      Socket :: inet:socket() | socket:socket() |
                fun ((iolist()) -> ok | {error, inet:posix() | closed}),
      Offset :: non_neg_integer(),
      Bytes :: non_neg_integer().

sendfile(Transport, {Mod, Handle}, Socket, Offset, Length) ->
    Mod:sendfile(Transport, Handle, Socket, Offset, Length);
sendfile(Transport, Handle, Socket, Offset, Length) ->
    ?DEFAULT_FILE:sendfile(Transport, Handle, Socket, Offset, Length).


-spec truncate(Handle) -> ok | {error, Reason} when
      Handle :: file_handle(),
      Reason :: file:posix() | badarg | terminated.

truncate({Mod, Handle}) ->
    Mod:truncate(Handle);
truncate(Handle) ->
    ?DEFAULT_FILE:truncate(Handle).


-spec write(Handle, Bytes) -> ok | {error, Reason} when
      Handle :: file_handle() | io:device(),
      Bytes :: iodata(),
      Reason :: file:posix() | badarg | terminated.
write({Mod, Handle}, Data) ->
    Mod:write(Handle, Data);
write(Handle, Data) ->
    ?DEFAULT_FILE:write(Handle, Data).

%% -spec try_write(module(), term(), iodata()) ->
%%     ok | {error, term()}.
%% try_write(Mod, Handle, Data) ->
%%     case erlang:function_exported(Mod, write, 2) of
%%         true ->
%%             Mod:write(Handle, Data);
%%         false ->
%%             file:write(Handle, Data)
%%     end.


%% TODO code below just hack to make it work for now.
-spec get_mod() -> module().

get_mod() ->
    ?DEFAULT_FILE;

get_mod(prim_file) ->
    %% Just temporary solutin till I figure out why
    %% we even use prim_file?
    %% prim_file;
    ?DEFAULT_FILE;
get_mod(File) ->
    case filelib:is_file(File) of
        true ->
            ?DEFAULT_FILE;
        false ->
            application:get_env(osiris, io_segment_module, ?DEFAULT_FILE)
    end.


get_mod(prim_file, File) ->
    %% Just temporary solutin till I figure out why
    %% we even use prim_file?
    case filelib:is_file(File) of
        true ->
            ?DEFAULT_FILE;
        false ->
            application:get_env(osiris, io_segment_module, ?DEFAULT_FILE)
    end.
