%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_log_reader).

-export([next_offset/1,
         committed_offset/1,
         committed_chunk_id/1,
         close/1,
         send_file/2,
         send_file/3,
         chunk_iterator/2,
         chunk_iterator/3,
         iterator_next/1]).

%% Exported for internal usage
-export([module/0]).

-type options() :: #{transport => tcp | ssl,
                     chunk_selector => all | user_data,
                     filter_spec => osiris_bloom:filter_spec(),
                     read_ahead => boolean() | non_neg_integer()
                    }.
-type config() ::
    osiris:config() |
    #{counter_spec := osiris_log:counter_spec(),
      options := options()}.
-type state() :: term().
-type chunk_iterator() :: term().
-type send_file_callback() ::
    fun((osiris_log:header_map(), BytesToSend :: non_neg_integer()) ->
        PrefixData :: binary()).

%% Error type that means that an operation is temporarily unavailable and that
%% retrying later is expected to eventually work.
-type transient_error() :: unavailable.

-export_type([options/0,
              config/0,
              state/0,
              chunk_iterator/0,
              send_file_callback/0,
              transient_error/0]).

-callback init_offset_reader(osiris:offset_spec(), config()) ->
    {ok, state()} |
    {error, transient_error() | term()}.
-callback resolve_offset_spec(osiris:offset_spec(), config()) ->
    {ok, osiris:offset()} |
    {error, transient_error() | term()}.

-callback next_offset(state()) -> osiris:offset().
-callback committed_offset(state()) -> osiris:offset().
-callback committed_chunk_id(state()) -> osiris:offset().
-callback close(state()) -> ok.

-callback send_file(gen_tcp:socket() | ssl:socket(), state(),
                    send_file_callback()) ->
    {ok, state()} |
    {error, term()} |
    {end_of_stream, state()}.

-callback chunk_iterator(state(),
                         Credit :: pos_integer() | all,
                         PrevIter :: chunk_iterator() | undefined) ->
    {ok, osiris_log:header_map(), chunk_iterator(), state()} |
    {end_of_stream, state()} |
    {error, term()}.

-callback iterator_next(chunk_iterator()) ->
    {{osiris:offset(), osiris:entry()}, chunk_iterator()} |
    end_of_chunk.

next_offset(State) ->
    (module()):?FUNCTION_NAME(State).
committed_offset(State) ->
    (module()):?FUNCTION_NAME(State).
committed_chunk_id(State) ->
    (module()):?FUNCTION_NAME(State).
close(State) ->
    (module()):?FUNCTION_NAME(State).

send_file(Socket, State) ->
    send_file(Socket, State, fun(_, _) -> <<>> end).
send_file(Socket, State0, Callback) ->
    Mod = module(),
    case Mod:?FUNCTION_NAME(Socket, State0, Callback) of
        {offset_not_found, State1} when Mod =:= osiris_log ->
            case osiris_log:open_next_segment(State1) of
                {ok, State} ->
                    send_file(Socket, State, Callback);
                not_found ->
                    {end_of_stream, State1}
            end;
        Other ->
            Other
    end.

chunk_iterator(State, Credit) ->
    chunk_iterator(State, Credit, undefined).
chunk_iterator(State0, Credit, PrevChunkIterator) ->
    Mod = module(),
    case Mod:?FUNCTION_NAME(State0, Credit, PrevChunkIterator) of
        {offset_not_found, State1} when Mod =:= osiris_log ->
            case osiris_log:open_next_segment(State1) of
                {ok, State} ->
                    ?FUNCTION_NAME(State, Credit, undefined);
                not_found ->
                    {end_of_stream, State1}
            end;
        Other ->
            Other
    end.

iterator_next(Iterator) ->
    (module()):?FUNCTION_NAME(Iterator).

%% @private
module() ->
    application:get_env(osiris, log_reader, osiris_log).
