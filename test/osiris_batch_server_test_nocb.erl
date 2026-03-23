%% Test callback module for osiris_batch_server WITHOUT batch_item_size/1
-module(osiris_batch_server_test_nocb).
-behaviour(osiris_batch_server).

-export([init/1,
         handle_batch/2,
         terminate/2]).

init(TestPid) ->
    {ok, #{test_pid => TestPid}}.

handle_batch(Batch, #{test_pid := TestPid} = State) ->
    TestPid ! {batch, Batch},
    {ok, State}.

terminate(_Reason, _State) ->
    ok.
