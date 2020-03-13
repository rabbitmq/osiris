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
                    reference => term(),
                    atom() => term()}.
-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              config/0
              ]).

-spec start_cluster(config()) -> {ok, config()} | {error, term()} | {error, term(), config()}.
start_cluster(Config00 = #{name := Name}) ->
    true = osiris_util:validate_base64uri(Name),
    Config0 = Config00#{external_ref => maps:get(reference, Config00, Name)},
    case osiris_writer:start(Config0) of
        {ok, Pid} ->
            Config = Config0#{leader_pid => Pid},
            case start_replicas(Config) of
                {ok, ReplicaPids} ->
                    {ok, Config#{replica_pids => ReplicaPids}};
                {error, Reason, ReplicaPids} ->
                    %% Let the user decide what to do if cluster is only partially started
                    {error, Reason, Config#{replica_pids => ReplicaPids}}
            end;
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


start_replicas(Config) ->
    start_replicas(Config, maps:get(replica_nodes, Config), []).

start_replicas(_Config, [], ReplicaPids) ->
    {ok, ReplicaPids};
start_replicas(Config, [Node | Nodes], ReplicaPids) ->
    try
        case osiris_replica:start(Node, Config) of
            {ok, Pid} ->
                start_replicas(Config, Nodes, [Pid | ReplicaPids]);
            {ok, Pid, _} ->
                start_replicas(Config, Nodes, [Pid | ReplicaPids]);
            {error, _Reason} ->
                %% coordinator might try to start this replica in the future
                start_replicas(Config, Nodes, ReplicaPids)
        end
    catch
        _:_ ->
            %% coordinator might try to start this replica in the future
            start_replicas(Config, Nodes, ReplicaPids)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
