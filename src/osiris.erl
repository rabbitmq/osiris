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

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-=").

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-type config() :: #{}.
-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-spec start_cluster(config()) -> {ok, config()}.
start_cluster(Config0 = #{name := Name})
  when is_atom(Name) ->
    true = validate_base64uri(atom_to_list(Name)),
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

-spec delete_cluster(#{}) -> ok.
delete_cluster(Config) ->
    [ok = osiris_replica:delete(R, Config) || R <- maps:get(replica_nodes, Config)],
    ok = osiris_writer:delete(Config).

restart_cluster(Config0 = #{name := Name})
  when is_atom(Name) ->
    true = validate_base64uri(atom_to_list(Name)),
    {ok, Pid} = osiris_writer:start(Config0),
    Config = Config0#{leader_pid => Pid},
    ReplicaPids = [element(2, osiris_replica:start(N, Config))
                   || N <- maps:get(replica_nodes, Config)],
    {ok, Config#{replica_pids => ReplicaPids}}.

restart_server(Config) ->
    osiris_writer:start(Config).

restart_replica(Replica, Config) ->
    osiris_replica:start(Replica, Config).

write(Pid, Corr, Data) ->
    osiris_writer:write(Pid, self(), Corr, Data).


-spec validate_base64uri(string()) -> boolean().
validate_base64uri(Str) when is_list(Str) ->
    catch
    begin
        [begin
             case lists:member(C, ?BASE64_URI_CHARS) of
                 true -> ok;
                 false -> throw(false)
             end
         end || C <- string:to_graphemes(Str)],
        string:is_empty(Str) == false
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
