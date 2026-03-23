%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_batch_server_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [batch_by_count,
     batch_by_bytes,
     batch_by_bytes_no_callback,
     batch_by_bytes_mixed_ops].

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% -- Test callback module helpers --

%% A simple callback module that records batches it receives.
%% We use the process dictionary to communicate with the test process.

%% -- Tests --

batch_by_count(_Config) ->
    {ok, Pid} = osiris_batch_server:start_link(
                  osiris_batch_server_test_cb, {self(), infinity}),
    [osiris_batch_server:cast(Pid, {write, <<"msg">>})
     || _ <- lists:seq(1, 5)],
    Batch = receive_batch(1000),
    ?assert(length(Batch) =:= 5),
    osiris_batch_server:stop(Pid).

batch_by_bytes(_Config) ->
    %% Each {write, Data} message has Data = 1000 bytes.
    %% With max_batch_bytes = 2500, we expect batches of at most ~2 messages
    %% (2000 bytes) before the byte limit triggers a flush.
    MaxBytes = 2500,
    {ok, Pid} = osiris_batch_server:start_link(
                  undefined,
                  osiris_batch_server_test_cb,
                  {self(), MaxBytes},
                  [{max_batch_bytes, MaxBytes}]),
    Data = binary:copy(<<"x">>, 1000),
    %% Send 6 messages rapidly. With max_batch_bytes=2500 and each msg=1000 bytes,
    %% we should get multiple batches rather than one big batch of 6.
    [osiris_batch_server:cast(Pid, {write, Data})
     || _ <- lists:seq(1, 6)],
    Batches = receive_all_batches(2000),
    TotalMsgs = lists:sum([length(B) || B <- Batches]),
    ?assertEqual(6, TotalMsgs),
    %% Each batch should have at most 3 messages (3000 bytes > 2500,
    %% but the check is >= so 3 msgs at 3000 bytes triggers flush)
    [?assert(length(B) =< 3) || B <- Batches],
    ?assert(length(Batches) >= 2),
    osiris_batch_server:stop(Pid).

batch_by_bytes_no_callback(_Config) ->
    %% When the callback module does not export batch_item_size/1,
    %% max_batch_bytes has no effect and batching is count-only.
    MaxBytes = 100,
    {ok, Pid} = osiris_batch_server:start_link(
                  undefined,
                  osiris_batch_server_test_nocb,
                  self(),
                  [{max_batch_bytes, MaxBytes}]),
    Data = binary:copy(<<"x">>, 1000),
    [osiris_batch_server:cast(Pid, {write, Data})
     || _ <- lists:seq(1, 5)],
    Batch = receive_batch(1000),
    %% All 5 should arrive in one batch since there's no size callback
    ?assertEqual(5, length(Batch)),
    osiris_batch_server:stop(Pid).

batch_by_bytes_mixed_ops(_Config) ->
    %% Non-write ops (calls, other casts) should not count toward byte limit.
    MaxBytes = 2500,
    {ok, Pid} = osiris_batch_server:start_link(
                  undefined,
                  osiris_batch_server_test_cb,
                  {self(), MaxBytes},
                  [{max_batch_bytes, MaxBytes}]),
    Data = binary:copy(<<"x">>, 1000),
    osiris_batch_server:cast(Pid, {write, Data}),
    osiris_batch_server:cast(Pid, {ack, some_node, 42}),
    osiris_batch_server:cast(Pid, {write, Data}),
    osiris_batch_server:cast(Pid, {ack, some_node, 43}),
    osiris_batch_server:cast(Pid, {write, Data}),
    Batches = receive_all_batches(2000),
    TotalMsgs = lists:sum([length(B) || B <- Batches]),
    ?assertEqual(5, TotalMsgs),
    osiris_batch_server:stop(Pid).

%% -- Helpers --

receive_batch(Timeout) ->
    receive
        {batch, Batch} -> Batch
    after Timeout ->
              ct:fail("Timed out waiting for batch")
    end.

receive_all_batches(Timeout) ->
    receive_all_batches(Timeout, []).

receive_all_batches(Timeout, Acc) ->
    receive
        {batch, Batch} ->
            receive_all_batches(Timeout, [Batch | Acc])
    after Timeout ->
              lists:reverse(Acc)
    end.
