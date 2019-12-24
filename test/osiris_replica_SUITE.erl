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

init_per_testcase(TestCase, Config) ->
    application:ensure_all_started(osiris),
    Config.

end_per_testcase(_TestCase, _Config) ->
    %% TODO ensure to stop all deps
    ok = application:stop(osiris),
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

init_replica(Config) ->
    Port = osiris_replica:start(node(), replica, {10, 100}),
    ?assert(erlang:is_integer(Port)),
    
    {ok, Sock} = gen_tcp:connect("localhost", Port, 
                                 [binary, {packet, 0}]),
    ok = gen_tcp:send(Sock, "Some Data"),
    ok = gen_tcp:close(Sock),

    ok.
