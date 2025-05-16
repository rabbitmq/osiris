-module(osiris_file).

%% -export([
%%          advise/4,
%%          close/1,
%%          copy/2,
%%          del_dir/1,
%%          delete/1,
%%          ensure_dir/1,
%%          list_dir/1,
%%          make_dir/1,
%%          open/2,
%%          position/2,
%%          pread/3,
%%          read/2,
%%          read_file_info/1,
%%          sendfile/5,
%%          truncate/1,
%%          write/2
%%         ]).

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

%% Might not be needed.
-callback write(Handle :: term(), Data :: iodata()) ->
    ok | {error, Reason :: term()}.
