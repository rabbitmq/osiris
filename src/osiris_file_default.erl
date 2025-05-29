%% Really need a better name...
-module(osiris_file_default).
-behaviour(osiris_file).

%% Standard file operations
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
         read/2,
         read_file_info/1,
         sendfile/5,
         truncate/1,
         write/2
        ]).

%% Simple wrappers around file module
advise(Handle, Offset, Length, Advise) ->
    file:advise(Handle, Offset, Length, Advise).

close(Handle) ->
    file:close(Handle).

copy(Source, Destination) ->
    file:copy(Source, Destination).

del_dir(Dir) ->
    prim_file:del_dir(Dir).

delete(File) ->
    prim_file:delete(File).

ensure_dir(Dir) ->
    filelib:ensure_dir(Dir).

list_dir(Dir) ->
    prim_file:list_dir(Dir).

make_dir(Dir) ->
    file:make_dir(Dir).

open(File, Options) ->
    file:open(File, Options).

position(Handle, Position) ->
    file:position(Handle, Position).

pread(Handle, Position, Size) ->
    file:pread(Handle, Position, Size).

read(Handle, Size) ->
    file:read(Handle, Size).

read_file_info(File) ->
    prim_file:read_file_info(File).

sendfile(_Transport, _Handle, _Sock, _Pos, 0) ->
    ok;
sendfile(tcp = Transport, Handle, Sock, Pos, ToSend) ->
    case file:sendfile(Handle, Sock, Pos, ToSend, []) of
        {ok, 0} ->
            %% TODO add counter for this?
            sendfile(Transport, Handle, Sock, Pos, ToSend);
        {ok, BytesSent} ->
            sendfile(Transport, Handle, Sock, Pos + BytesSent, ToSend - BytesSent);
        {error, _} = Err ->
            Err
    end;
sendfile(ssl, Handle, Sock, Pos, ToSend) ->
    case file:pread(Handle, Pos, ToSend) of
        {ok, Data} ->
            ssl:send(Sock, Data);
        {error, _} = Err ->
            Err
    end.

truncate(Handle) ->
    file:truncate(Handle).

write(Handle, Data) ->
    file:write(Handle, Data).
