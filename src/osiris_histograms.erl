%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_histograms).

-export([init/0,
         observe_entry/1,
         observe_chunk/1,
         overview/0,
         cleanup/0]).

-define(GROUP, osiris).
-define(HISTOGRAMS, [simple_entry_bytes, batch_bytes, chunk_bytes]).
-define(PT_KEY(Histogram), {?MODULE, Histogram}).

-define(BUCKET_BOUNDS, [100, 1_000, 10_000, 100_000,
                        1_000_000, 10_000_000, 50_000_000,
                        100_000_000, infinity]).

-type histogram() :: simple_entry_bytes | batch_bytes | chunk_bytes.
-type overview() :: #{histogram() => seshat_histogram:overview()}.

-export_type([histogram/0, overview/0]).

-spec init() -> ok.
init() ->
    _ = [begin
             Ref = seshat_histogram:new(?GROUP, H, H, ?BUCKET_BOUNDS,
                                        #{help => help(H)}),
             ok = persistent_term:put(?PT_KEY(H), Ref)
         end || H <- ?HISTOGRAMS],
    ok.

-spec observe_entry(osiris:data()) -> ok.
observe_entry({FilterValue, Inner}) when is_binary(FilterValue) ->
    observe_entry(Inner);
observe_entry({batch, _NumRecords, _CompType, UncompLen, _Body}) ->
    observe(batch_bytes, UncompLen);
observe_entry(Data) ->
    observe(simple_entry_bytes, iolist_size(Data)).

-spec observe_chunk(non_neg_integer()) -> ok.
observe_chunk(Size) ->
    observe(chunk_bytes, Size).

-spec overview() -> overview().
overview() ->
    seshat_histogram:fold(fun(Id, Info, Acc) -> Acc#{Id => Info} end,
                          #{}, ?GROUP).

%%% Internal

observe(Histogram, Value) ->
    case persistent_term:get(?PT_KEY(Histogram), undefined) of
        undefined ->
            ok;
        Ref ->
            seshat_histogram:observe(Ref, Value)
    end.

help(simple_entry_bytes) ->
    "Size in bytes of individual entries written to streams";
help(batch_bytes) ->
    "Uncompressed size in bytes of sub-batches written to streams";
help(chunk_bytes) ->
    "Size in bytes of chunks written to stream segment files".

-spec cleanup() -> ok.
cleanup() ->
    _ = [begin
             _ = persistent_term:erase(?PT_KEY(H)),
             ok = seshat_histogram:delete(?GROUP, H)
         end || H <- ?HISTOGRAMS],
    ok.
