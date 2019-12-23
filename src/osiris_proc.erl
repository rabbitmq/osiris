%% @hidden
-module(osiris_proc).
-behaviour(gen_batch_server).

-export([start_link/1,
         init/1,
         handle_batch/2,
         terminate/2,
         format_status/1
        ]).

%% primary osiris process
%% batch writes incoming data
%% notifies replicator and reader processes of the new max index
%% manages incoming max index

-define(SYNC_INTERVAL, 5).

-record(?MODULE, {}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec init(file:filename()) -> {ok, state()}.
init(Dir) ->
    process_flag(trap_exit, true),
    ok = ra_lib:make_dir(Dir),
    % MetaFile = filename:join(Dir, "meta.dets"),
    {ok, #?MODULE{}}.

handle_batch(_Commands, #?MODULE{} = State) ->
    %% write to log and index files
    {ok, [], State}.

terminate(_, #?MODULE{}) ->
    ok.

format_status(State) ->
    State.
