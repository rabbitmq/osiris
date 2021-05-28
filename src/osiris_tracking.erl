-module(osiris_tracking).

-export([
         init/1,
         add/5,
         flush/1,
         snapshot/2,
         query/3,
         append_trailer/3,
         needs_flush/1,
         is_empty/1,
         overview/1
         ]).


-define(TRK_TYPE_SEQUENCE, 0).
-define(TRK_TYPE_OFFSET, 1).
-define(MAX_WRITERS, 255).
%% holds static or rarely changing fields
-record(cfg, {}).

-type tracking_id() :: binary().
-type tracking_type() :: sequence | offset.
-type tracking() :: osiris:offset() | osiris:milliseconds() | non_neg_integer().

-record(?MODULE, {cfg = #cfg{} :: #cfg{},
                  pending = #{} :: #{sequences | offsets =>
                                     #{tracking_id() =>
                                       {tracking_type(), tracking()}}},
                  sequences = #{} :: #{osiris:writer_id() => {osiris:offset(),
                                                              non_neg_integer()}},
                  offsets = #{} :: #{tracking_id() => osiris:offset()}
                 }).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              tracking_type/0,
              tracking_id/0
              ]).

-spec init(undefined | binary()) -> state().
init(undefined) ->
    #?MODULE{};
init(Bin) when is_binary(Bin) ->
    parse_snapshot(Bin, #?MODULE{}).

-spec add(tracking_id(), tracking_type(), tracking(), osiris:offset() | undefined,
          state()) -> state().
add(TrkId, TrkType, Tracking, ChunkId,
    #?MODULE{pending = Pend0} = State)
  when is_integer(Tracking) andalso
       byte_size(TrkId) =< 256 ->
    Pend = Pend0#{TrkId => {TrkType, Tracking}},
    update_tracking(TrkId, TrkType, Tracking,
                    ChunkId, State#?MODULE{pending = Pend}).


-spec flush(state()) -> {iodata(), state()}.
flush(#?MODULE{pending = Pending} = State) ->
    TData = maps:fold(fun(K, {Type, V}, Acc) ->
                              T = case Type of
                                      sequence ->
                                          ?TRK_TYPE_SEQUENCE;
                                      offset ->
                                          ?TRK_TYPE_OFFSET
                                  end,
                              [<<T:8/unsigned,
                                 (byte_size(K)):8/unsigned,
                                 K/binary,
                                 V:64/unsigned>> | Acc]
                      end,
                      [], Pending),
    {TData, State#?MODULE{pending = #{}}}.

-spec snapshot(osiris:offset(), state()) ->
    {iodata(), state()}.
snapshot(FirstOffset, #?MODULE{sequences = Seqs0,
                               offsets = Offsets0} = State) ->
    %% discard any tracking info with offsets lower than the first offset
    %% in the stream
    Offsets = maps:filter(fun(_, O) -> O >= FirstOffset end, Offsets0),
    Seqs = trim_writers(?MAX_WRITERS, Seqs0),

    SeqData = maps:fold(fun(TrkId, {Ts, Seq} , Acc) ->
                                [<<?TRK_TYPE_SEQUENCE:8/unsigned,
                                   (byte_size(TrkId)):8/unsigned,
                                   TrkId/binary,
                                   Ts:64/unsigned,
                                   Seq:64/unsigned>>
                                 | Acc]
                        end, [], Seqs),
    Data = maps:fold(fun(TrkId, Offs, Acc) ->
                             [<<?TRK_TYPE_OFFSET:8/unsigned,
                                (byte_size(TrkId)):8/unsigned,
                                TrkId/binary,
                                Offs:64/unsigned>>
                              | Acc]
                     end, SeqData, Offsets),
    {Data, State#?MODULE{pending = #{},
                         sequences = Seqs,
                         offsets = Offsets}}.

-spec query(tracking_id(), TrkType :: tracking_type(), state()) ->
    {ok, term()} | {error, not_found}.
query(TrkId, sequence, #?MODULE{sequences = Seqs})
  when is_binary(TrkId) ->
    case Seqs of
        #{TrkId := Tracking} ->
            {ok, Tracking};
        _ ->
            {error, not_found}
    end;
query(TrkId, offset, #?MODULE{offsets = Offs})
  when is_binary(TrkId) ->
    case Offs of
        #{TrkId := Tracking} ->
            {ok, Tracking};
        _ ->
            {error, not_found}
    end.

-spec append_trailer(osiris:milliseconds(), binary(), state()) ->
    state().
append_trailer(Ts, Bin, State) ->
    parse_trailer(Bin, Ts, State).

-spec needs_flush(state()) -> boolean().
needs_flush(#?MODULE{pending = Pend}) ->
    map_size(Pend) > 0.

-spec is_empty(state()) -> boolean().
is_empty(#?MODULE{sequences = Seqs, offsets = Offs}) ->
    map_size(Seqs) + map_size(Offs) == 0.

-spec overview(state()) -> map(). %% TODO refine
overview(#?MODULE{sequences = Seqs, offsets = Offs}) ->
    #{offsets => Offs,
      sequences => Seqs}.

%% INTERNAL
update_tracking(TrkId, sequence, Tracking, ChId,
                #?MODULE{sequences = Seqs0} = State) when is_integer(ChId) ->
    State#?MODULE{sequences = Seqs0#{TrkId => {ChId, Tracking}}};
update_tracking(TrkId, offset, Tracking, _ChId,
                #?MODULE{offsets = Offs} = State) ->
    State#?MODULE{offsets = Offs#{TrkId => Tracking}}.

parse_snapshot(<<>>, State) ->
    State;
parse_snapshot(<<?TRK_TYPE_SEQUENCE:8/unsigned,
                 TrkIdSize:8/unsigned,
                 TrkId:TrkIdSize/binary,
                 Ts:64/unsigned,
                 Seq:64/unsigned, Rem/binary>>,
               #?MODULE{sequences = Seqs} = State) ->
    parse_snapshot(Rem, State#?MODULE{sequences = Seqs#{TrkId => {Ts, Seq}}});
parse_snapshot(<<?TRK_TYPE_OFFSET:8/unsigned,
                 TrkIdSize:8/unsigned,
                 TrkId:TrkIdSize/binary,
                 Offs:64/unsigned, Rem/binary>>,
               #?MODULE{offsets = Offsets} = State) ->
    parse_snapshot(Rem, State#?MODULE{offsets = Offsets#{TrkId => Offs}}).

parse_trailer(<<>>, _Ts, State) ->
    State;
parse_trailer(<<?TRK_TYPE_SEQUENCE:8/unsigned,
                TrkIdSize:8/unsigned,
                TrkId:TrkIdSize/binary,
                Seq:64/unsigned, Rem/binary>>,
              Ts, #?MODULE{sequences = Seqs} = State) ->
    parse_trailer(Rem, Ts, State#?MODULE{sequences = Seqs#{TrkId => {Ts, Seq}}});
parse_trailer(<<?TRK_TYPE_OFFSET:8/unsigned,
                TrkIdSize:8/unsigned,
                TrkId:TrkIdSize/binary,
                Offs:64/unsigned, Rem/binary>>,
              Ts, #?MODULE{offsets = Offsets} = State) ->
    parse_trailer(Rem, Ts, State#?MODULE{offsets = Offsets#{TrkId => Offs}}).

trim_writers(Max, Writers) when map_size(Writers) =< Max ->
    Writers;
trim_writers(Max, Writers) ->
    Sorted = lists:sort(fun ({_, {C0, _}}, {_, {C1, _}}) ->
                                C0 < C1
                        end, maps:to_list(Writers)),
    maps:from_list(lists:nthtail(map_size(Writers) - Max, Sorted)).
