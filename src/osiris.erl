-module(osiris).

-export([
         start_cluster/1,
         stop_cluster/1,
         write/3,
         restart_cluster/1,
         restart_server/1,
         restart_replica/2,
         delete_cluster/1
         ]).

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-type config() :: #{name := string(),
                    atom() => term()}.
-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              config/0
              ]).

-spec start_cluster(config()) -> {ok, config()} | {error, term()}.
start_cluster(Config0 = #{name := Name}) ->
    true = osiris_util:validate_base64uri(Name),
    case osiris_writer:start(Config0) of
        {ok, Pid} ->
            Config = Config0#{leader_pid => Pid},
            ReplicaPids = [begin
                               {ok, P} = osiris_replica:start(N, Config),
                               P
                           end || N <- maps:get(replica_nodes, Config)],
            {ok, Config#{replica_pids => ReplicaPids}};
        Error ->
            Error
    end.

stop_cluster(Config) ->
    ok = osiris_writer:stop(Config),
    [ok = osiris_replica:stop(N, Config) || N <- maps:get(replica_nodes, Config)],
    ok.

-spec delete_cluster(config()) -> ok.
delete_cluster(Config) ->
    [ok = osiris_replica:delete(R, Config) || R <- maps:get(replica_nodes, Config)],
    ok = osiris_writer:delete(Config).

restart_cluster(Config0 = #{name := Name}) ->
    true = osiris_utils:validate_base64uri(Name),
    {ok, Pid} = osiris_writer:start(Config0),
    Config = Config0#{leader_pid => Pid},
    ReplicaPids = [begin
                       element(2, osiris_replica:start(N, Config))
                   end
                   || N <- maps:get(replica_nodes, Config)],
    {ok, Config#{replica_pids => ReplicaPids}}.

restart_server(Config) ->
    osiris_writer:start(Config).

restart_replica(Replica, Config) ->
    osiris_replica:start(Replica, Config).

write(Pid, Corr, Data) ->
    osiris_writer:write(Pid, self(), Corr, Data).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
