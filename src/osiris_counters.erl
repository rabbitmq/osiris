%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_counters).

-export([init/0,
         new/2,
         fetch/1,
         overview/0,
         overview/1,
         delete/1
        ]).

-type name() :: term().

-spec init() -> ok.
init() ->
    _ = seshat:new_group(osiris),
    ok.

-spec new(name(), [{Name :: atom(), Position :: non_neg_integer(),
                    Type :: atom(), Description :: term()}]) ->
                 counters:counters_ref().
new(Name, Fields) ->
    seshat:new(osiris, Name, Fields).

-spec fetch(name()) -> undefined | counters:counters_ref().
fetch(Name) ->
    seshat:fetch(osiris, Name).

-spec delete(term()) -> ok.
delete(Name) ->
    seshat:delete(osiris, Name).

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    seshat:counters(osiris).

-spec overview(name()) -> #{atom() => non_neg_integer()} | undefined.
overview(Name) ->
    seshat:counters(osiris, Name).
