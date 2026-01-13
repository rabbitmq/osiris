%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log_manifest).

-type state() :: term().

-type chunk_info() ::
    #{id := osiris:offset(),
      timestamp := osiris:timestamp(),
      epoch := osiris:epoch(),
      num := non_neg_integer(),
      type := osiris_log:chunk_type(),
      %% size of data + filter + trailer
      size := non_neg_integer(),
      pos := integer()}.

-type event() :: {segment_opened,
                  OldSegment :: file:filename_all() | undefined,
                  NewSegment :: file:filename_all()} |
                 {chunk_written, chunk_info(), iodata()} |
                 {retention_updated, [osiris:retention_spec()]}.

-export_type([state/0,
              chunk_info/0,
              event/0]).

-callback overview(Dir :: file:filename_all()) ->
    {osiris_log:range(), osiris_log:epoch_offsets()}.

-callback init_manifest(osiris_log:config(), writer | acceptor) ->
    {osiris_log:config(), state()}.

-callback handle_event(event(), state()) -> state().

-callback close_manifest(state()) -> ok.

-callback delete(osiris_log:config()) -> ok.
