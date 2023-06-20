-module(osiris_bloom).

-export([
         init/1,
         insert/2,
         to_binary/1,

         is_in_set/2,

         init_matcher/1,
         is_match/2,

         bit_set/2,
         make_hash/2
         ]).

-define(NIL, []).
-define(DEFAULT_FILTER_SIZE, 16).


-record(?MODULE, {size_b = ?DEFAULT_FILTER_SIZE,
                  filter_values = #{} :: #{binary() => ?NIL}
                 }).

-type hash() :: {1..2040, 1..2040} | {0, 0}.
-type filter_size() :: 16..255.

-record(matcher,
        {current_bit_size = ?DEFAULT_FILTER_SIZE * 8,
         values :: [binary()],
         hashes :: [hash()]
        }).

-opaque state() :: #?MODULE{}.
-opaque mstate() :: #matcher{}.

-type filter_spec() :: #{filters := [binary()],
                         match_unfiltered => boolean()}.

-export_type([
              state/0,
              mstate/0,
              filter_size/0,
              filter_spec/0
             ]).

-spec init_matcher(filter_spec()) -> mstate().
init_matcher(Spec) ->
    init_matcher(Spec, ?DEFAULT_FILTER_SIZE).

init_matcher(#{filters := [_|_] = Values} = Spec, SizeB) ->
    MatchUnfiltered = maps:get(match_unfiltered, Spec, false),
    BitSize = SizeB * 8,
    %% pre-calculate matchine hashes
    Hashes0 = [make_hash(Value, BitSize) || Value <- Values],
    Hashes = case MatchUnfiltered of
                 true ->
                     %% the 0th bit is reserved to indicate the presence of entries
                     %% that do not include a filter
                     [{0, 0} | Hashes0];
                 false ->
                     Hashes0
             end,

    #matcher{current_bit_size = BitSize,
             values = Values,
             hashes = Hashes}.

-spec is_match(binary(), mstate()) ->
    {retry_with, mstate()} | boolean().
is_match(Filter, #matcher{hashes = [FirstHash | _],
                          values = Values,
                          current_bit_size = BitSize})
      when byte_size(Filter) > 0 andalso
           bit_size(Filter) =/= BitSize ->
    MatchUnfiltered = FirstHash == {0, 0},
    NewMatcher = init_matcher(#{filters => Values,
                                match_unfiltered => MatchUnfiltered},
                              byte_size(Filter)),
    {retry_with, NewMatcher};
is_match(Filter, #matcher{hashes = Hashes})
  when is_binary(Filter) andalso
       byte_size(Filter) > 0 ->
    lists:any(fun (Hash) ->
                      is_in_set(Hash, Filter)
              end, Hashes);
is_match(<<>>, #matcher{hashes = [Hash1 | _]}) ->
    Hash1 == {0, 0};
is_match(_Filter, undefined) ->
    %% if no reader filter is set
    true.


-spec init(filter_size()) -> state().
init(SizeB) when is_integer(SizeB) andalso
                 SizeB >= 16 andalso
                 SizeB =< 255 ->
    #?MODULE{size_b = SizeB};
init(SizeB) ->
    exit({invalid_filter_size, SizeB}).

-spec insert(binary(), state()) -> state().
insert(Value, #?MODULE{filter_values = Values} = State)
  when is_binary(Value) andalso
       not is_map_key(Value, Values) ->
    State#?MODULE{filter_values = Values#{Value => ?NIL}};
insert(_Value, State) ->
    State.

-spec to_binary(state()) -> binary().
to_binary(#?MODULE{filter_values = Values})
  when map_size(Values) == 0 ->
    <<>>;
to_binary(#?MODULE{filter_values = Values})
  when map_size(Values) == 1 andalso
       is_map_key(<<>>, Values) ->
    <<>>;
to_binary(#?MODULE{size_b = SizeB,
                   filter_values = Values}) ->
    BitSize = SizeB * 8,
    B0 = <<0:BitSize>>,
    B = maps:fold(fun(K, _, Acc) ->
                          {H1, H2} = make_hash(K, BitSize),
                          set_bits(H1, H2, Acc)
                  end, B0, Values),
    B.

is_in_set({H1, H2}, Bin) when is_binary(Bin) ->
    bit_set(H1, Bin) andalso bit_set(H2, Bin);
is_in_set(Value, Bin) when is_binary(Value) andalso
                           is_binary(Bin) ->
    {H1, H2} = make_hash(Value, bit_size(Bin)),
    bit_set(H1, Bin) andalso bit_set(H2, Bin).

% INTERNAL

%% returns two has values in the range 1..BitSize-1
%% bit 0 is reserved to indicate the presence of an unfiltered entry
%% in a chunk
make_hash(<<>>, _BitSize) ->
    {0, 0};
make_hash(Value, BitSize) when is_binary(Value) ->
    H1 = erlang:phash2(Value, BitSize-1) + 1,
    H2 = erlang:phash2([Value], BitSize-1) + 1,
    {H1, H2}.

set_bits(I, I, Bin) ->
    set_bit(I, Bin);
set_bits(BeforeI, AfterI, Bin) when BeforeI < AfterI ->
    %% ordered case
    BetweenN = AfterI - BeforeI - 1, %% in between
    case Bin of
        <<_:BeforeI/bitstring, 1:1,
          _:BetweenN/bitstring, 1:1, _/bitstring>> ->
            %% already set
            Bin;
        <<Before:BeforeI/bitstring, _:1,
          Between:BetweenN/bitstring, _:1, After/bitstring>> ->
            <<Before/bitstring, 1:1, Between/bitstring, 1:1, After/bitstring>>
    end;
set_bits(I1, I2, Bin) ->
    set_bits(I2, I1, Bin).

set_bit(N, Bin) ->
    case Bin of
        <<_:N/bitstring, 1:1, _/bitstring>> ->
            Bin;
        <<Before:N/bitstring, 0:1, After/bitstring>> ->
            <<Before/bitstring, 1:1, After/bitstring>>
    end.


bit_set(N, Bin) ->
    case Bin of
        <<_:N, 1:1, _/bitstring>> ->
            true;
        _ ->
            false
    end.
