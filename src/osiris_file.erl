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

advise({Mod, Handle}, Offset, Length, Advise) ->
    Mod:advise(Handle, Offset, Length, Advise);
advise(Handle, Offset, Length, Advise) ->
    file:advise(Handle, Offset, Length, Advise).

close({Mod, Handle}) ->
    Mod:close(Handle);
close(Handle) ->
    file:close(Handle).


%% TODO
copy(Source, Destination) ->
    file:copy(Source, Destination).
%% TODO
del_dir(Dir) ->
    file:del_dir(Dir).

%% TODO
delete(File) ->
    prim_file:delete(File).

%% TODO
ensure_dir(Dir) ->
    filelib:ensure_dir(Dir).

%% TODO
list_dir(Dir) ->
    prim_file:list_dir(Dir).

%% TODO
make_dir(Dir) ->
    file:make_dir(Dir).

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


position({Mod, Handle}, Position) ->
    Mod:position(Handle, Position);
position(Handle, Position) ->
    file:position(Handle, Position).


pread({Mod, Handle}, Position, Size) ->
    Mod:pread(Handle, Position, Size);
pread(Handle, Position, Size) ->
    file:pread(Handle, Position, Size).


read({Mod, Handle}, Size) ->
    Mod:read(Handle, Size);
read(Handle, Size) ->
    file:read(Handle, Size).


%% Todo
read_file_info(File) ->
    prim_file:read_file_info(File).

sendfile({Mod, Handle}, Socket, Offset, Length, Options) ->
    Mod:sendfile(Handle, Socket, Offset, Length, Options);
sendfile(Handle, Socket, Offset, Length, Options) ->
    file:sendfile(Handle, Socket, Offset, Length, Options).

truncate({Mod, Handle}) ->
    Mod:truncate(Handle);
truncate(Handle) ->
    file:truncate(Handle).


write({Mod, Handle}, Data) ->
    Mod:write(Handle, Data);
write(Handle, Data) ->
    file:write(Handle, Data).

%% try_write(Mod, Handle, Data) ->
%%     case erlang:function_exported(Mod, write, 2) of
%%         true ->
%%             Mod:write(Handle, Data);
%%         false ->
%%             file:write(Handle, Data)
%%     end.

get_mod() ->
    file.
