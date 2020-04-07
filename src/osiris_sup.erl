-module(osiris_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    osiris_counters:init(),
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 5},
    %% todo put under own sup
    Retention = #{id => osiris_retention,
                  type => worker,
                  start => {osiris_retention, start_link, []}},
    ServerSup = #{id => osiris_server_sup,
                  type => supervisor,
                  start => {osiris_server_sup, start_link, []}},
    ReplicaReader = #{id => osiris_replica_reader_sup,
                      type => supervisor,
                      start => {osiris_replica_reader_sup, start_link, []}},
    {ok, {SupFlags, [Retention, ServerSup, ReplicaReader]}}.
