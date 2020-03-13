-module(osiris_util_SUITE).

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
     to_base64uri_test
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
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

to_base64uri_test(Config) ->
    ?assertEqual("Myqueue", osiris_util:to_base64uri("Myqueue")),
    ?assertEqual("my__queue", osiris_util:to_base64uri("my%*queue")),
    ?assertEqual("my99___queue", osiris_util:to_base64uri("my99_[]queue")).
