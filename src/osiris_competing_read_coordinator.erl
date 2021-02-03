%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(osiris_competing_read_coordinator).

-behaviour(gen_server).

%% API
-export([start_link/2]).
%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,
         format_status/2]).

-define(SERVER, ?MODULE).
%% The batch size defines the number of chunks delivered at once to each reader,
%% within their credit limits. If the chunks are small with few messages, it might
%% be more efficient to deliver several chunks together.
-define(DEFAULT_BATCH_SIZE, 3).

-type reader_group() ::
        #{monitor_ref => reference(),
          tags => [binary()]}.

-record(reader_state,
        {formatter :: function(),
          credit :: integer(),
          in_flight :: sets:new()}).

-record(state,
        {log :: 'undefined' | osiris_log:state(),
         config :: osiris:config(),
         sup :: pid(),
         readers :: #{{pid(), Tag :: binary()} => #reader_state{}},
         service_queue :: queue:queue(),
         reader_groups :: #{pid() => reader_group()},
         pending :: [ChunkId :: integer()],
         batch_size :: integer()}).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link(osiris:config(), pid()) ->
                    {ok, Pid :: pid()} |
                    {error, Error :: {already_started, pid()}} |
                    {error, Error :: term()} |
                    ignore.
start_link(Config, SupPid) ->
    gen_server:start_link(?MODULE, [Config, SupPid], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Config, SupPid]) ->
    process_flag(trap_exit, true),
    gen_server:cast(self(), init),
    {ok,
     #state{config = Config,
            sup = SupPid,
            readers = #{},
            reader_groups = #{},
            service_queue = queue:new(),
            pending = [],
            batch_size =
                application:get_env(osiris, crc_batch_size,
                                    ?DEFAULT_BATCH_SIZE)}}.

handle_call({register, Pid, Tag, Credit, Fmt}, _From,
            #state{readers = Readers,
                   reader_groups = Groups0,
                   service_queue = SQ} =
                State) ->
    Reader = {Pid, Tag},
    case maps:get(Reader, Readers, undefined) of
        undefined ->
            ReaderState =
                #reader_state{formatter = Fmt,
                              credit = Credit,
                              in_flight = sets:new()},
            Groups =
                case maps:get(Pid, Groups0, undefined) of
                    undefined ->
                        MRef = monitor(process, Pid),
                        Groups0#{Pid => #{monitor_ref => MRef, tags => [Tag]}};
                    #{tags := Tags} = Group ->
                        Groups0#{Pid => Group#{tags => [Tag | Tags]}}
                end,
            {reply, ok,
             notify_readers(State#state{readers =
                                            maps:put(Reader, ReaderState,
                                                     Readers),
                                        service_queue = queue:in(Reader, SQ),
                                        reader_groups = Groups})};
        _ ->
            %% Duplicated call, do nothing
            {reply, ok, State}
    end.

handle_cast(init, #state{config = Config, sup = SupPid} = State) ->
    {ok, Writer} = osiris_server_sup:get_writer(SupPid, Config),
    {ok, Ctx} = gen:call(Writer, '$gen_call', get_reader_context),
    {ok, Seg} = osiris_log:init_offset_reader(first, Ctx),
    {noreply, State#state{log = Seg}};
handle_cast({ack, Reader, ChunkId},
            #state{readers = Readers0, service_queue = SQ0} = State) ->
    case maps:get(Reader, Readers0, undefined) of
        undefined ->
            %% Reader has probably crashed just after sending the message,
            %% chunks might now be pending or processed by another reader.
            %% TODO: Not sure we should act on this?
            {noreply, State};
        #reader_state{credit = C0, in_flight = InFlight0} = Cfg0 ->
            Cfg = Cfg0#reader_state{in_flight = sets:del_element(ChunkId, InFlight0),
                                    credit = C0 + 1},
            Readers = Readers0#{Reader => Cfg},
            case C0 of
                0 ->
                    {noreply,
                     notify_readers(State#state{readers = Readers,
                                                service_queue =
                                                    queue:in(Reader, SQ0)})};
                _ ->
                    {noreply, notify_readers(State#state{readers = Readers})}
            end
    end.

handle_info({'DOWN', _, _, Pid, _},
            #state{readers = Readers0,
                   service_queue = SQ0,
                   reader_groups = Groups0,
                   pending = Pending0} =
                State) ->
    {#{tags := Tags}, Groups} = maps:take(Pid, Groups0),
    {InFlight, Readers} =
        lists:foldl(fun(Tag, {InF, Rs}) ->
                            {#reader_state{in_flight = InF0}, Rs0} = maps:take({Pid, Tag}, Rs),
                            {sets:to_list(InF0) ++ InF, Rs0}
                    end,
                    {[], Readers0}, Tags),
    Pending = Pending0 ++ lists:sort(InFlight),
    SQ = queue:from_list(
             lists:foldl(fun(Tag, Acc) -> lists:delete({Pid, Tag}, Acc) end,
                         queue:to_list(SQ0), Tags)),

    {noreply,
     notify_readers(State#state{readers = Readers,
                                service_queue = SQ,
                                reader_groups = Groups,
                                pending = Pending})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
notify_readers(#state{log = Seg0,
                      service_queue = SQ0,
                      pending = Pending,
                      batch_size = BatchSize,
                      readers = Readers0} =
                   State) ->
    case queue:peek(SQ0) of
        {value, Reader} ->
            #reader_state{credit = Credit} = maps:get(Reader, Readers0),
            case Pending of
                [] ->
                    case read_chunks_from_segment(min(BatchSize, Credit), Seg0)
                    of
                        {[], Seg} ->
                            State#state{log = Seg};
                        {ChunkIds, Seg} ->
                            deliver_chunks(Reader, ChunkIds,
                                           State#state{log = Seg})
                    end;
                _ ->
                    {ChunkIds, Rest} =
                        take_chunks_from_pending(min(BatchSize, Credit),
                                                 Pending),
                    deliver_chunks(Reader, ChunkIds,
                                   State#state{pending = Rest})
            end;
        empty ->
            State
    end.

take_chunks_from_pending(BatchSize, Pending) ->
    lists:split(min(BatchSize, length(Pending)), Pending).

read_chunks_from_segment(BatchSize, Seg) ->
    read_chunks_from_segment(BatchSize, Seg, []).

read_chunks_from_segment(0, Seg, Acc) ->
    {lists:reverse(Acc), Seg};
read_chunks_from_segment(BatchSize, Seg0, Acc) ->
    case osiris_log:read_header(Seg0) of
        {ok, #{chunk_id := ChunkId}, Seg} ->
            read_chunks_from_segment(BatchSize - 1, Seg, [ChunkId | Acc]);
        {end_of_stream, Seg} ->
            {lists:reverse(Acc), Seg}
    end.

wrap_event(Fmt, Evt) ->
    Fmt(Evt).

deliver_chunks({Pid, Tag} = Reader, ChunkIds,
               #state{readers = Readers0, service_queue = SQ0} = State) ->
    #reader_state{formatter = Fmt,
                  credit = C0,
                  in_flight = InFlight0} =
        Cfg0 = maps:get(Reader, Readers0),
    Pid ! wrap_event(Fmt, {osiris_chunk, self(), Tag, ChunkIds}),
    C = C0 - length(ChunkIds),
    Cfg = Cfg0#reader_state{in_flight = add_in_flight_chunks(ChunkIds, InFlight0),
                            credit = C},
    Readers = Readers0#{Reader => Cfg},
    case C of
        _ when C =< 0 ->
            notify_readers(State#state{readers = Readers,
                                       service_queue = queue:drop(SQ0)});
        _ ->
            %% Reader has a higher credit but we deliver a limited number
            %% of chunks every time. It has to move to the tail of the queue
            notify_readers(State#state{readers = Readers,
                                       service_queue =
                                           queue:in(Reader, queue:drop(SQ0))})
    end.

add_in_flight_chunks(ChunkIds, InFlight) ->
    lists:foldl(fun(ChunkId, Acc) -> sets:add_element(ChunkId, Acc)
                end,
                InFlight, ChunkIds).
