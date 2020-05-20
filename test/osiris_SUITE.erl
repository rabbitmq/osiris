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
     quorum_write,
     cluster_batch_write,
     read_validate_single_node,
     read_validate,
     single_node_offset_listener,
     cluster_offset_listener,
     replica_offset_listener,
     cluster_restart,
     cluster_delete,
     start_cluster_invalid_replicas,
     diverged_replica,
     retention
    ].

-define(BIN_SIZE, 800).

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    osiris:configure_logger(logger),
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
    application:stop(osiris),
    application:load(osiris),
    application:set_env(osiris, data_dir, Dir),
    {ok, Apps} = application:ensure_all_started(osiris),
    ok = logger:set_primary_config(level, all),
    % file:make_dir(Dir),
    [{data_dir, Dir},
     {test_case, TestCase},
     {cluster_name, atom_to_list(TestCase)},
     {started_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    [application:stop(App) || App <- lists:reverse(?config(started_apps, Config))],
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

single_node_write(Config) ->
    Name = ?config(cluster_name, Config),
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => [],
              dir => ?config(priv_dir, Config)},
    {ok, #{leader_pid := Leader}} = osiris:start_cluster(Conf0),
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
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir)
                                       || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => LeaderNode,
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader}} = osiris:start_cluster(Conf0),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    Self = self(),
    _ = spawn_link(
          LeaderNode,
          fun () ->
                  {ok, Log0} = osiris_writer:init_data_reader(Leader, {0, empty}),
                  {[{0, <<"mah-data">>}], _Log} = osiris_log:read_chunk_parsed(Log0),
                  Self ! read_data_ok
          end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

quorum_write(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => LeaderNode,
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader}} = osiris:start_cluster(Conf0),
    slave:stop(hd(Replicas)),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,
    Self = self(),
    _ = spawn_link(
          LeaderNode,
          fun () ->
                  {ok, Log0} = osiris_writer:init_data_reader(Leader, {0, empty}),
                  {[{0, <<"mah-data">>}], _Log} = osiris_log:read_chunk_parsed(Log0),
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
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => LeaderNode,
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader,
           replica_pids := [ReplicaPid | _]}} = osiris:start_cluster(Conf0),
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
                      {ok, Log0} = osiris:init_reader(Leader, first),
                      {[{0, <<"mah-data">>}], _Log} = osiris_log:read_chunk_parsed(Log0),
                      Self ! read_data_ok
              end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,
    timer:sleep(1000),
    _ = spawn(node(ReplicaPid),
              fun () ->
                      {ok, Log0} = osiris:init_reader(ReplicaPid, first),
                      {[{0, <<"mah-data">>}], _Log} = osiris_log:read_chunk_parsed(Log0),
                      Self ! read_data_ok2
              end),
    receive
        read_data_ok2 -> ok
    after 2000 ->
              exit(read_data_ok_timeout2)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

single_node_offset_listener(Config) ->
    Name = ?config(cluster_name, Config),
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => []},
    {ok, #{leader_pid := Leader}} = osiris:start_cluster(Conf0),
    {error, {offset_out_of_range, empty}} =
        osiris:init_reader(Leader, {abs, 0}),
    osiris:register_offset_listener(Leader, 0),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_offset, _Name, 0} ->
            {ok, Log0} = osiris:init_reader(Leader, {abs, 0}),
            {[{0, <<"mah-data">>}], Log} = osiris_log:read_chunk_parsed(Log0),
            {end_of_stream, _} = osiris_log:read_chunk_parsed(Log),
            ok
    after 2000 ->
              flush(),
              exit(osiris_offset_timeout)
    end,
    flush(),
    ok.

cluster_offset_listener(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [_ | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader}} = osiris:start_cluster(Conf0),
    {ok, Log0} = osiris:init_reader(Leader, 0),
    osiris:register_offset_listener(Leader, 0),
    ok = osiris:write(Leader, 42, <<"mah-data">>),
    receive
        {osiris_offset, _Name, O} when O > -1 ->
            ct:pal("got offset ~w", [O]),
            {[{0, <<"mah-data">>}], Log} = osiris_log:read_chunk_parsed(Log0),
            %% stop all replicas
            [slave:stop(N) || N <- Replicas],
            ok = osiris:write(Leader, 43, <<"mah-data2">>),
            timer:sleep(10),
            {end_of_stream, _} = osiris_log:read_chunk_parsed(Log),
            ok
    after 2000 ->
              flush(),
              exit(osiris_offset_timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

replica_offset_listener(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [_ | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader,
           replica_pids := ReplicaPids}} = osiris:start_cluster(Conf0),
    Self = self(),
    R = hd(ReplicaPids),
    _ = spawn(node(R),
              fun () ->
                      {ok, Log0} = osiris:init_reader(R, 0),
                      osiris:register_offset_listener(R, 0),
                      receive
                          {osiris_offset, _Name, O} when O > -1 ->
                              ct:pal("got offset ~w", [O]),
                              {[{0, <<"mah-data">>}], Log} = osiris_log:read_chunk_parsed(Log0),
                              osiris_log:close(Log),
                              Self ! test_passed,
                              ok
                      after 2000 ->
                                flush(),
                                exit(osiris_offset_timeout)
                      end
              end),
    ok = osiris:write(Leader, 42, <<"mah-data">>),

    receive
        test_passed -> ok
    after 5000 ->
              flush(),
              [slave:stop(N) || N <- Nodes],
              exit(timeout)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

read_validate_single_node(Config) ->
    _PrivDir = ?config(data_dir, Config),
    Num = 100000,
    Name = ?config(cluster_name, Config),
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => []},
    {ok, #{leader_pid := Leader,
           replica_pids := []}} = osiris:start_cluster(Conf0),
    timer:sleep(500),
    % start_profile(Config, [osiris_writer, gen_batch_server,
    %                        osiris_log, lists, file]),
    write_n(Leader, Num, #{}),
    % stop_profile(Config),
    {ok, Log0} = osiris_writer:init_data_reader(Leader, {0, empty}),

    {Time, _} = timer:tc(fun() -> validate_read(Num, Log0) end),
    MsgSec = Num / ((Time / 1000) / 1000),
    ct:pal("validate read of ~b entries took ~wms ~w msg/s", [Num, Time div 1000, MsgSec]),
    ok.


read_validate(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    Num = 1000000,
    Replicas = [start_slave(N, PrivDir) || N <- [r1, r2]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader,
           replica_pids := ReplicaPids}} = osiris:start_cluster(Conf0),
    {_, GarbBefore} = erlang:process_info(Leader, garbage_collection),
    {_, MemBefore} = erlang:process_info(Leader, memory),
    {_, BinBefore} = erlang:process_info(Leader, binary),
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
    {_, BinAfter} = erlang:process_info(Leader, binary),
    {_, GarbAfter} = erlang:process_info(Leader, garbage_collection),
    {_, MemAfter} = erlang:process_info(Leader, memory),
    {reductions, _RedsAfter} = erlang:process_info(Leader, reductions),

    ct:pal("Binary:~n~w~n~w~n", [length(BinBefore), length(BinAfter)]),
    ct:pal("Garbage:~n~w~n~w~n", [GarbBefore, GarbAfter]),
    ct:pal("Memory:~n~w~n~w~n", [MemBefore, MemAfter]),
    MsgSec = Num / (Time / 1000 / 1000),
    ct:pal("~b writes took ~wms ~w msg/s",
           [Num, trunc(Time div 1000), trunc(MsgSec)]),
    ct:pal("~w counters ~p", [node(), osiris_counters:overview()]),
    [begin
         ct:pal("~w counters ~p", [N, rpc:call(N, osiris_counters, overview, [])])
     end || N <- Replicas],
    {ok, Log0} = osiris_writer:init_data_reader(Leader, {0, empty}),
    {_, _} = timer:tc(fun() -> validate_read(Num, Log0) end),

    %% test reading on slave
    R = hd(ReplicaPids),
    Self = self(),
    _ = spawn(node(R),
          fun() ->
                  {ok, RLog0} = osiris_writer:init_data_reader(R, {0, empty}),
                  {_, _} = timer:tc(fun() -> validate_read(Num, RLog0) end),
                  Self ! validate_read_done
          end),
    receive
        validate_read_done -> ok
    after 30000 ->
              exit(validate_read_done_timeout)
    end,

    [slave:stop(N) || N <- Replicas],
    ok.

cluster_restart(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir)
                                       || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              replica_nodes => Replicas,
              leader_node => LeaderNode},
    {ok, #{leader_pid := Leader} = Conf} = osiris:start_cluster(Conf0),
    ok = osiris:write(Leader, 42, <<"before-restart">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,

    osiris:stop_cluster(Conf),
    {ok, #{leader_pid := Leader1}} = osiris:start_cluster(Conf0#{epoch => 1}),
    %% give leader some time to discover the committed offset
    timer:sleep(1000),

    Self = self(),
    _ = spawn(LeaderNode,
              fun () ->
                      {ok, Log0} = osiris:init_reader(Leader1, 0),
                      {[{0, <<"before-restart">>}], Log1} = osiris_log:read_chunk_parsed(Log0),
                      osiris_log:close(Log1),
                      Self ! read_data_ok
              end),
    receive
        read_data_ok -> ok
    after 2000 ->
              exit(read_data_ok_timeout)
    end,

    ok = osiris:write(Leader1, 43, <<"after-restart">>),
    receive
        {osiris_written, _, [43]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,

    _ = spawn(LeaderNode,
              fun () ->
                      {ok, Log0} = osiris_writer:init_data_reader(Leader1, {0, empty}),
                      {[{0, <<"before-restart">>}], Log1} = osiris_log:read_chunk_parsed(Log0),
                      {[{1, <<"after-restart">>}], Log2} = osiris_log:read_chunk_parsed(Log1),
                      osiris_log:close(Log2),
                      Self ! read_data_ok1
              end),
    receive
        read_data_ok1 -> ok
    after 2000 ->
              exit(read_data_ok_timeout1)
    end,
    [slave:stop(N) || N <- Nodes],
    ok.

cluster_delete(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    [LeaderNode | Replicas] = Nodes = [start_slave(N, PrivDir) || N <- [s1, s2, s3]],
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => LeaderNode,
              replica_nodes => Replicas},
    {ok, #{leader_pid := Leader} = Conf} = osiris:start_cluster(Conf0),
    ok = osiris:write(Leader, 42, <<"before-restart">>),
    receive
        {osiris_written, _, [42]} ->
            ok
    after 2000 ->
              flush(),
              exit(osiris_written_timeout)
    end,

    osiris:delete_cluster(Conf),
    [slave:stop(N) || N <- Nodes],
    ok.

start_cluster_invalid_replicas(Config) ->
    Name = ?config(cluster_name, Config),
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              replica_nodes => ['zen@rabbit'],
              dir => ?config(priv_dir, Config)},
    {ok, #{leader_pid := _Leader,
           replica_pids := []}} = osiris:start_cluster(Conf0).

diverged_replica(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    Nodes = [s1, s2, s3],
    [LeaderE1, LeaderE2, LeaderE3] =
        [start_slave(N, PrivDir) || N <- Nodes],
    ConfE1 = #{name => Name,
               external_ref => Name,
               epoch => 1,
               leader_node => LeaderE1,
               replica_nodes => [LeaderE2, LeaderE3]},
    {ok, #{leader_pid := LeaderE1Pid}} = osiris:start_cluster(ConfE1),
    %% write some records in e1
    [osiris:write(LeaderE1Pid, N, [<<N:64/integer>>])
     || N <- lists:seq(1, 100)],
    wait_for_written(lists:seq(1, 100)),

    %% shut down cluster and start only LeaderE2 in epoch 2
    ok = osiris:stop_cluster(ConfE1),
    ConfE2 = ConfE1#{leader_node => LeaderE2,
                     epoch => 2,
                     replica_nodes => [LeaderE1, LeaderE2]},
    {ok, LeaderE2Pid} = osiris_writer:start(ConfE2),
    %% write some entries that won't be committedo
    [osiris:write(LeaderE2Pid, N, [<<N:64/integer>>])
     || N <- lists:seq(101, 200)],
    %% we can't wait for osiris_written here
    timer:sleep(500),
    %% shut down LeaderE2
    ok = osiris_writer:stop(ConfE2),

    ConfE3 = ConfE1#{leader_node => LeaderE3,
                     epoch => 3,
                     replica_nodes => [LeaderE1, LeaderE2]},
    %% start the cluster in E3 with E3 as leader
    {ok, #{leader_pid := LeaderE3Pid}} = osiris:start_cluster(ConfE3),
    %% write some more in this epoch
    [osiris:write(LeaderE3Pid, N, [<<N:64/integer>>])
     || N <- lists:seq(201, 300)],
    wait_for_written(lists:seq(201, 300)),

    ok = osiris:stop_cluster(ConfE3),

    %% validate replication etc takes place
    [Idx1, Idx2, Idx3] =
    [begin
         {ok, D} = file:read_file(filename:join([PrivDir, N,
                                                 ?FUNCTION_NAME,
                                                 "00000000000000000000.index"])),
         D
     end || N <- Nodes],
    ?assertEqual(Idx1, Idx2),
    ?assertEqual(Idx1, Idx3),

    [Seg1, Seg2, Seg3] =
    [begin
         {ok, D} = file:read_file(filename:join([PrivDir, N,
                                                 ?FUNCTION_NAME,
                                                 "00000000000000000000.segment"])),
         D
     end || N <- Nodes],
    ?assertEqual(Seg1, Seg2),
    ?assertEqual(Seg1, Seg3),
    ok.

retention(Config) ->
    _PrivDir = ?config(data_dir, Config),
    Num = 150000,
    Name = ?config(cluster_name, Config),
    SegSize = 50000 * 1000,
    Conf0 = #{name => Name,
              epoch => 1,
              leader_node => node(),
              retention => [{max_bytes, SegSize}],
              max_segment_size => SegSize,
              replica_nodes => []},
    {ok, #{leader_pid := Leader,
           replica_pids := []}} = osiris:start_cluster(Conf0),
    timer:sleep(500),
    write_n(Leader, Num, 0, 1000 * 8, #{}),
    timer:sleep(1000),
    %% assert on num segs
    ok.


%% Utility

write_n(Pid, N, Written) ->
    write_n(Pid, N, 0, ?BIN_SIZE, Written).

write_n(_Pid, N, N, _BinSize, Written) ->
    %% wait for all written events;
    wait_for_written(Written),
    ok;
write_n(Pid, N, Next, BinSize, Written) ->
    ok = osiris:write(Pid, Next, <<Next:BinSize/integer>>),
    write_n(Pid, N, Next + 1, BinSize, Written#{Next => ok}).

wait_for_written(Written0) when is_list(Written0) ->
    wait_for_written(lists:foldl(fun(N, Acc) ->
                                         maps:put(N, ok, Acc)
                                 end, #{}, Written0));
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


validate_read(N, Log) ->
    validate_read(N, 0, Log).

validate_read(N, N, Log0) ->
    {end_of_stream, _Log} = osiris_log:read_chunk_parsed(Log0),
    ok;
validate_read(Max, Next, Log0) ->
    {[{Offs, _} | _] = Recs, Log} = osiris_log:read_chunk_parsed(Log0),
    case Offs == Next of
        false ->
            ct:fail("validate_read failed Offs ~b not eqial to ~b",
                    [Offs, Next]);
        true ->
            validate_read(Max, Next + length(Recs), Log)
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
    ok = rpc:call(S, logger, set_primary_config, [level, all]),
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

start_profile(Config, Modules) ->
    Dir = ?config(priv_dir, Config),
    Case = ?config(test_case, Config),
    GzFile = filename:join([Dir, "lg_" ++ atom_to_list(Case) ++ ".gz"]),
    ct:pal("Profiling to ~p~n", [GzFile]),

    lg:trace(Modules, lg_file_tracer,
             GzFile, #{running => false, mode => profile}).

stop_profile(Config) ->
    Case = ?config(test_case, Config),
    ct:pal("Stopping profiling for ~p~n", [Case]),
    lg:stop(),
    Dir = ?config(priv_dir, Config),
    Name = filename:join([Dir, "lg_" ++ atom_to_list(Case)]),
    lg_callgrind:profile_many(Name ++ ".gz.*", Name ++ ".out",#{}),
    ok.

