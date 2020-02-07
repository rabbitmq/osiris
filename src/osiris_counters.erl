-module(osiris_counters).

-export([
         init/0,
         new/2,
         fetch/1,
         overview/0,
         delete/1
         ]).

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-spec init() -> ok.
init() ->
    _ = ets:new(?MODULE, [set, named_table, public]),
    ok.

-spec new(atom(),  [atom()]) -> counters:counter_ref().
new(Name, Fields)
  when is_list(Fields) ->
    Size = length(Fields),
    CRef = counters:new(Size, []),
    ok = register_counter(Name, CRef, Fields),
    CRef.

-spec fetch(atom()) ->
    undefined | counters:counter_ref().
fetch(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{Name, Ref, _}] ->
            Ref;
        _ ->
            undefined
    end.

-spec delete(atom()) -> ok.
delete(Name) ->
    true = ets:delete(?MODULE, Name),
    ok.

-spec overview() ->
    #{atom() => #{atom() => non_neg_integer()}}.
overview() ->
    ets:foldl(
      fun({Name, Ref, Fields}, Acc) ->
              Size = length(Fields),
              Values = [counters:get(Ref, I) || I <- lists:seq(1, Size)],
              Counters = maps:from_list(lists:zip(Fields, Values)),
              Acc#{Name => Counters}
      end, #{}, ?MODULE).

%% internal

register_counter(Name, Ref, Size) ->
    true = ets:insert(?MODULE, {Name, Ref, Size}),
    ok.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
