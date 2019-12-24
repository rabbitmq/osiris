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

-record(?MODULE, {segment = osiris_segment:state()}).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

-spec start_link(Config :: map()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Config) ->
    gen_batch_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec init(file:filename()) -> {ok, state()}.
init(Dir) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    ok = ra_lib:make_dir(Dir),
    Segment = osiris_segment:init(Dir, #{}),
    {ok, #?MODULE{segment = Segment}}.

handle_batch(Commands, #?MODULE{segment = Seg0} = State) ->
    %% filter write commands
    Records = [R || {write, R} <- Commands],

    Seg = osiris_segment:write(Records, Seg0),
    %% write to log and index files
    {ok, [], State#?MODULE{segment = Seg}}.

terminate(_, #?MODULE{}) ->
    ok.

format_status(State) ->
    State.
