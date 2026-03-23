%% Test callback module for osiris_batch_server with batch_item_size/1
-module(osiris_batch_server_test_cb).
-behaviour(osiris_batch_server).

-export([init/1,
         handle_batch/2,
         batch_item_size/1,
         terminate/2]).

init({TestPid, _MaxBytes}) ->
    {ok, #{test_pid => TestPid}}.

handle_batch(Batch, #{test_pid := TestPid} = State) ->
    TestPid ! {batch, Batch},
    {ok, State}.

batch_item_size({cast, {write, Data}}) ->
    iolist_size(Data);
batch_item_size(_) ->
    0.

terminate(_Reason, _State) ->
    ok.
