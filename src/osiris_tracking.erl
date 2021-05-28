-module(osiris_tracking).

-export([
         init/1,
         add/5,
         flush/1,
         snapshot/2,
         query/3,
         append_trailer/3,
         needs_flush/1
         ]).


-define(TRK_TYPE_SEQUENCE, 0).
-define(TRK_TYPE_OFFSET, 1).
%% holds static or rarely changing fields
-record(cfg, {}).

-type tracking_id() :: binary().
-type tracking_type() :: sequence | offset.
-type tracking() :: osiris:offset() | osiris:milliseconds() | non_neg_integer().

-record(?MODULE, {cfg = #cfg{} :: #cfg{},
                  pending = #{} :: #{sequences | offsets =>
                                     #{tracking_id() =>
                                       {tracking_type(), tracking()}}},
                  sequences = #{} :: #{osiris:writer_id() => {osiris:milliseconds(),
                                                              non_neg_integer()}},
                  offsets = #{} :: #{osiris:tracking_id() => osiris:offset()}
                 }).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0,
              tracking_type/0
              ]).

-spec init(undefined | binary()) -> state().
init(undefined) ->
    #?MODULE{};
init(Bin) when is_binary(Bin) ->
    parse_snapshot(Bin, #?MODULE{}).

-spec add(tracking_id(), tracking_type(), tracking(), osiris:milliseconds(),
          state()) -> state().
add(TrkId, TrkType, Tracking, Timestamp,
    #?MODULE{pending = Pend0} = State)
  when is_integer(Tracking) andalso
       byte_size(TrkId) =< 256 ->
    Pend = Pend0#{TrkId => {TrkType, Tracking}},
    update_tracking(TrkId, TrkType, Tracking,
                    Timestamp, State#?MODULE{pending = Pend}).


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
snapshot(FirstOffset, #?MODULE{sequences = Seqs,
                               offsets = Offsets0} = State) ->
    %% discard any tracking info with offsets lower than the first offset
    %% in the stream
    Offsets = maps:filter(fun(_, O) -> O >= FirstOffset end, Offsets0),

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
                         offsets = Offsets}}.

-spec query(tracking_id(), tracking_type(), state()) ->
    {ok, tracking()} | {error, not_found}.
query(TrkId, sequence, #?MODULE{sequences = Seqs}) ->
    case Seqs of
        #{TrkId := {_Ts, Seq}} ->
            {ok, Seq};
        _ ->
            {error, not_found}
    end;
query(TrkId, offset, #?MODULE{offsets = Offs}) ->
    case Offs of
        #{TrkId := O} ->
            {ok, O};
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


%% INTERNAL
update_tracking(TrkId, sequence, Tracking, Ts,
                #?MODULE{sequences = Seqs0} = State) ->
    State#?MODULE{sequences = Seqs0#{TrkId => {Ts, Tracking}}};
update_tracking(TrkId, offset, Tracking, _Ts,
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
