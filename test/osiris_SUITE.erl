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
     test1
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

init_per_testcase(_TestCase, Config) ->
    PrivDir = ?config(data_dir, Config),
    ok = osiris:start(PrivDir),
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok = osiris:stop(),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

single_node_write(Config) ->
    _PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    {ok, _Started, []} = osiris:start_cluster(Name, [{Name, node()}]),
    ok = osiris:write({Name, node()}, 42, <<"mah-data">>),
    receive
        {osiris_written, Name, [42]} ->
            ok
    after 2000 ->
              exit(osiris_written_timeout)
    end,
    ok.

cluster_write(Config) ->
    PrivDir = ?config(data_dir, Config),
    Name = ?config(cluster_name, Config),
    NodeIds = [{Name, start_slave(N, PrivDir)} || N <- [s1, s2, s3]],
    {ok, _Started, []} = osiris:start_cluster(Name, NodeIds),
    ok = osiris:write({Name, s1}, 42, <<"mah-data">>),
    receive
        {osiris_written, Name, [42]} ->
            ok
    after 2000 ->
              exit(osiris_written_timeout)
    end,
    ok.

%% Utility

start_slave(N, PrivDir) ->
    Dir0 = filename:join(PrivDir, N),
    Host = get_current_host(),
    Dir = "'\"" ++ Dir0 ++ "\"'",
    Pa = string:join(["-pa" | search_paths()] ++ ["-s ra -ra data_dir", Dir], " "),
    ct:pal("starting slave node with ~s~n", [Pa]),
    {ok, S} = slave:start_link(Host, N, Pa),
    _ = rpc:call(S, ra, start, []),
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

