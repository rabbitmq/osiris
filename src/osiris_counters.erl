%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_counters).

-export([init/0,
         new/2,
         fetch/1,
         overview/0,
         delete/1
        ]).

%% holds static or rarely changing fields
-record(cfg, {}).
-record(?MODULE, {cfg :: #cfg{}}).

-opaque state() :: #?MODULE{}.

-type name() :: term().

-export_type([state/0]).

-spec init() -> ok.
init() ->
    seshat_counters:new_group(osiris).

-spec new(name(), [{Name :: atom(), Position :: non_neg_integer(),
                    Type :: atom(), Description :: term()}]) ->
                 counters:counters_ref().
new(Name, Fields) ->
    seshat_counters:new(osiris, Name, Fields).

-spec fetch(name()) -> undefined | counters:counters_ref().
fetch(Name) ->
    seshat_counters:fetch(osiris, Name).

-spec delete(term()) -> ok.
delete(Name) ->
    seshat_counters:delete(osiris, Name).

-spec overview() -> #{name() => #{atom() => non_neg_integer()}}.
overview() ->
    seshat_counters:overview(osiris).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

-endif.
