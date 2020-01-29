-module(osiris_SUITE).

-compile(export_all).

-export([
         ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     single_node_write,
     cluster_write,
     cluster_batch_write,
     read_validate_single_node,
     read_validate,
     single_node_offset_listener,
     cluster_offset_listener,
     cluster_restart,
     cluster_delete
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    application:load(osiris),
    application:set_env(osiris, data_dir, Dir),
    {ok, Apps} = application:ensure_all_started(osiris),
    file:make_dir(Dir),
    [{data_dir, Dir},
     {cluster_name, TestCase},
     {started_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    [application:stop(App) || App <- lists:reverse(?config(started_apps, Config))],
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

single_node_write(Config) ->
    Name = atom_to_list(?FUNCTION_NAME),
    {ok, Leader, _Replicas} = osiris:start_cluster(Name, [],
                                                   #{dir => ?config(data_dir, Config)}),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_written, _Name, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    ok.

cluster_write(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, _Replicas} = rpc:call(LeaderNode, osiris, start_cluster,
                                       [atom_to_list(Name), Replicas, OConf ]),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    Self = self(),
    _ = spawn(LeaderNode,
              fun () ->
                      Seg0 = osiris_writer:init_reader(Leader, 0),
                      {[{0, <<"mah-data">>}], _Seg} = osiris_segment:read_chunk_parsed(Seg0),
                      Self ! read_data_ok
              end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

cluster_batch_write(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir)
                                       || N <- [s1, s2, s3]],
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, _Replicas} = rpc:call(LeaderNode, osiris, start_cluster,
                                       [atom_to_list(Name), Replicas, OConf]),
    Batch = {batch, 1, 0, <<0:1, 8:31/unsigned, "mah-data">>},
    ok = osiris:write(Leader, 42, Batch),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    Self = self(),
    _ = spawn(LeaderNode,
              fun () ->
                      Seg0 = osiris_writer:init_reader(Leader, 0),
                      {[{0, <<"mah-data">>}], _Seg} = osiris_segment:read_chunk_parsed(Seg0),
                      Self ! read_data_ok
              end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

single_node_offset_listener(Config) ->
    Name = atom_to_list(?FUNCTION_NAME),
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, _Replicas} = osiris:start_cluster(Name, [], OConf),
    Seg0 = osiris_writer:init_reader(Leader, 0),
    osiris_writer:register_offset_listener(Leader),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_offset, _Name, _} ->
            {[{0, <<"mah-data">>}], Seg} = osiris_segment:read_chunk_parsed(Seg0),
            {end_of_stream, _} = osiris_segment:read_chunk_parsed(Seg),
            ok
    after 2000 ->
              flush(),
              exit(osiris_offset_timeout)
    end,
    ok.

cluster_offset_listener(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = atom_to_list(?FUNCTION_NAME),
    [_ | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    {ok, Leader, _Replicas} = osiris:start_cluster(Name, Replicas),
    Seg0 = osiris_writer:init_reader(Leader, 0),
    osiris_writer:register_offset_listener(Leader),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_offset, _Name, O} when O > -1 ->
            ct:pal("got offset ~w", [O]),
            {[{0, <<"mah-data">>}], Seg} = osiris_segment:read_chunk_parsed(Seg0),
            slave:stop(hd(Replicas)),
            ok = osiris:write(Leader, 43, <<"mah-data2">>),
            timer:sleep(10),
            {end_of_stream, _} = osiris_segment:read_chunk_parsed(Seg),
            ok
    after 2000 ->
              flush(),
              exit(osiris_offset_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

read_validate_single_node(Config) ->
    _PrivDir = ?config(data_dir, Config),
    Name = atom_to_list(?FUNCTION_NAME),
    Num = 1000000,
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, []} = osiris:start_cluster(Name, [], OConf),
    timer:sleep(500),
    write_n(Leader, Num, #{}),
    Seg0 = osiris_writer:init_reader(Leader, 0),

    {Time, _} = timer:tc(fun() -> validate_read(Num, Seg0) end),
    MsgSec = Num / ((Time / 1000) / 1000),
    ct:pal("validate read of ~b entries took ~wms ~w msg/s", [Num, Time div 1000, MsgSec]),
    ok.


read_validate(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = atom_to_list(?FUNCTION_NAME),
    Num = 500000,
    OConf = #{dir => ?config(data_dir, Config)},
    [_LNode | Replicas] = Nodes =  [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    {ok, Leader, _} = rpc:call(node(), osiris, start_cluster,
                               [Name, Replicas, OConf]),
    timer:sleep(500),
    {Time, _} = timer:tc(fun () ->
                                 Self = self(),
                                 spawn(fun () ->
                                               write_n(Leader, Num div 2, #{}),
                                               Self ! done
                                       end),
                                 write_n(Leader, Num div 2, #{}),
                                 receive
                                     done -> ok
                                 after 1000 * 60 ->
                                           exit(blah)
                                 end
                         end),

    MsgSec = Num / (Time / 1000 / 1000),
    ct:pal("~b writes took ~wms ~w msg/s",
           [Num, trunc(Time div 1000), trunc(MsgSec)]),
    ct:pal("counters ~p", [osiris_counters:overview()]),
    Seg0 = osiris_writer:init_reader(Leader, 0),
    {_, _} = timer:tc(fun() -> validate_read(Num, Seg0) end),

    [slave:stop(N) || N <- Nodes],
    ok.

cluster_restart(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, _Replicas} = rpc:call(LeaderNode, osiris, start_cluster,
                                       [atom_to_list(Name), Replicas, OConf ]),
    ok = osiris:write(Leader, 42, <<"before-restart">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,

    ok = rpc:call(LeaderNode, osiris, stop_cluster, [atom_to_list(Name), Replicas]),
    
    {ok, Leader1} = rpc:call(LeaderNode, osiris, restart_server, [Name, Replicas, OConf]),
    [{ok, _Replica} = rpc:call(LeaderNode, osiris, restart_replica,
                               [Name, Leader1, Replica, OConf])
     || Replica <- Replicas],

    ok = osiris:write(Leader1, 43, <<"after-restart">>),
    receive
        {osiris_written, _, [43]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    
    Self = self(),
    _ = spawn(LeaderNode,
              fun () ->
                      Seg0 = osiris_writer:init_reader(Leader1, 0),
                      {[{0, <<"before-restart">>}], Seg1} = osiris_segment:read_chunk_parsed(Seg0),
                      {[{1, <<"after-restart">>}], _Seg2} = osiris_segment:read_chunk_parsed(Seg1),
                      Self ! read_data_ok
              end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

cluster_delete(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    OConf = #{dir => ?config(data_dir, Config)},
    {ok, Leader, ReplicaPids} = rpc:call(LeaderNode, osiris, start_cluster,
                                          [atom_to_list(Name), Replicas, OConf ]),
    ok = osiris:write(Leader, 42, <<"before-restart">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,

    ok = rpc:call(LeaderNode, osiris, delete_cluster, [Name, Leader, ReplicaPids]),
    [slave:stop(N) || N <- Nodes],
    ok.

%% Utility

write_n(Pid, N, Written) ->
    write_n(Pid, N, 0, Written).

write_n(_Pid, N, N, Written) ->
    %% wait for all written events;
    wait_for_written(Written),
    ok;
write_n(Pid, N, Next, Written) ->
    ok = osiris:write(Pid, Next, <<Next:800/integer>>),
    write_n(Pid, N, Next + 1, Written#{Next => ok}).

wait_for_written(Written0) ->
    receive
        {osiris_written, _Name, Corrs} ->
            Written = maps:without(Corrs, Written0),
            % ct:pal("written ~w", [length(Corrs)]),
            case maps:size(Written) of
                0 ->
                    ok;
                _ ->
                    wait_for_written(Written)
            end
    after 1000 * 60 ->
              flush(),
              exit(osiris_written_timeout)
    end.


validate_read(N, Seg) ->
    validate_read(N, 0, Seg).

validate_read(N, N, Seg0) ->
    {end_of_stream, _Seg} = osiris_segment:read_chunk_parsed(Seg0),
    ok;
validate_read(Max, Next, Seg0) ->
    {[{Offs, _} | _] = Recs, Seg} = osiris_segment:read_chunk_parsed(Seg0),
    case Offs == Next of
        false ->
            ct:fail("validate_read failed Offs ~b not eqial to ~b",
                    [Offs, Next]);
        true ->
            validate_read(Max, Next + length(Recs), Seg)
    end.

start_slave(N, PrivDir) ->
    Dir0 = filename:join(PrivDir, N),
    Host = get_current_host(),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Pa = string:join(["-pa" | search_paths()] ++ ["-osiris data_dir", Dir], " "),
    ct:pal("starting slave node with ~s~n", [Pa]),
    {ok, S} = slave:start_link(Host, N, Pa),
    ct:pal("started slave node ~w ~w~n", [S, Host]),
    Res = rpc:call(S, application, ensure_all_started, [osiris]),
    ct:pal("application start result ~p", [Res]),
    S.

flush() ->
    receive
        Any ->
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.

get_current_host() ->
    {ok, H} = inet:gethostname(),
    list_to_atom(H).

make_node_name(N) ->
    {ok, H} = inet:gethostname(),
    list_to_atom(lists:flatten(io_lib:format("~s@~s", [N, H]))).

search_paths() ->
    Ld = code:lib_dir(),
    lists:filter(fun (P) -> string:prefix(P, Ld) =:= nomatch end,
                 code:get_path()).

