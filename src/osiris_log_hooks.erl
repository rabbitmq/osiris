%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log_hooks).

%% Behaviour for hooking into log lifecycle events.
%%
%% Discovered via `application:get_env(osiris, log_hooks, undefined)`. When
%% `undefined`, no hooks fire. A plugin sets the env on boot to receive
%% callbacks at lifecycle boundaries.

-callback on_init(writer | acceptor, pid(), osiris_log:config()) ->
    osiris_log:config().

-callback on_retention_updated([osiris:retention_spec()], map()) ->
    [osiris:retention_spec()].

-callback on_retention_evaluated(counters:counters_ref(), map()) ->
    ok.

-optional_callbacks([on_retention_evaluated/2]).
