-module(osiris_replica_SUITE).

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
     init_replica
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
    Apps = application:ensure_all_started(osiris),
    [{started_apps, Apps} | Config].

end_per_testcase(_TestCase, Config) ->
    [application:stop(App) || App <- lists:reverse(?config(started_apps, Config))],
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_replica(_Config) ->
    {ok, Pid} = osiris_writer:start(replica, #{}),
    {ok, Child} = osiris_replica:start(node(), replica, Pid),

    Port = osiris_replica:get_port(Child),
    {ok, Sock} = gen_tcp:connect("localhost", Port,
                                 [binary, {packet, 0}]),
    
    Chunk = osiris_segment:chunk([<<"Some">>, <<"Data">>], 0),

    ok = gen_tcp:send(Sock, Chunk),
    ok = gen_tcp:close(Sock),

    ok.
