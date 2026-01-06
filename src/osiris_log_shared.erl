-module(osiris_log_shared).

-define(COMMITTED_CHUNK_ID, 1).
-define(FIRST_IDX, 2).
-define(LAST_IDX, 3).
-define(COMMITTED_OFFSET, 4).

-export([
         new/0,
         committed_chunk_id/1,
         first_chunk_id/1,
         last_chunk_id/1,
         committed_offset/1,
         set_committed_chunk_id/2,
         set_first_chunk_id/2,
         set_last_chunk_id/2,
         set_committed_offset/2
        ]).

-type chunk_id() :: -1 | non_neg_integer().
-type offset() :: -1 | non_neg_integer().

-spec new() -> atomics:atomics_ref().
new() ->
    %% Oh why, oh why did we think the first chunk id in
    %% a stream should have offset 0?
    Ref = atomics:new(4, [{signed, true}]),
    atomics:put(Ref, ?COMMITTED_CHUNK_ID, -1),
    atomics:put(Ref, ?FIRST_IDX, -1),
    atomics:put(Ref, ?LAST_IDX, -1),
    atomics:put(Ref, ?COMMITTED_OFFSET, -1),
    Ref.

-spec committed_chunk_id(atomics:atomics_ref()) -> chunk_id().
committed_chunk_id(Ref) ->
    atomics:get(Ref, ?COMMITTED_CHUNK_ID).

-spec first_chunk_id(atomics:atomics_ref()) -> chunk_id().
first_chunk_id(Ref) ->
    atomics:get(Ref, ?FIRST_IDX).

-spec last_chunk_id(atomics:atomics_ref()) -> chunk_id().
last_chunk_id(Ref) ->
    atomics:get(Ref, ?LAST_IDX).

-spec committed_offset(atomics:atomics_ref()) -> chunk_id().
committed_offset(Ref) ->
    atomics:get(Ref, ?COMMITTED_OFFSET).

-spec set_committed_chunk_id(atomics:atomics_ref(), chunk_id()) -> ok.
set_committed_chunk_id(Ref, Value) when is_integer(Value) ->
    atomics:put(Ref, ?COMMITTED_CHUNK_ID, Value).

-spec set_first_chunk_id(atomics:atomics_ref(), chunk_id()) -> ok.
set_first_chunk_id(Ref, Value) when is_integer(Value) ->
    atomics:put(Ref, ?FIRST_IDX, Value).

-spec set_last_chunk_id(atomics:atomics_ref(), chunk_id()) -> ok.
set_last_chunk_id(Ref, Value) when is_integer(Value) ->
    atomics:put(Ref, ?LAST_IDX, Value).

-spec set_committed_offset(atomics:atomics_ref(), offset()) -> ok.
set_committed_offset(Ref, Value) when is_integer(Value) ->
    atomics:put(Ref, ?COMMITTED_OFFSET, Value).


-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
basics_test() ->
    R = new(),
    ?assertEqual(-1, committed_chunk_id(R)),
    ?assertEqual(-1, first_chunk_id(R)),
    ?assertEqual(-1, last_chunk_id(R)),
    ?assertEqual(-1, committed_offset(R)),
    ok = set_committed_chunk_id(R, 2),
    ok = set_committed_offset(R, 3),
    ok = set_first_chunk_id(R, 1),
    ok = set_last_chunk_id(R, 4),
    ?assertEqual(2, committed_chunk_id(R)),
    ?assertEqual(3, committed_offset(R)),
    ?assertEqual(1, first_chunk_id(R)),
    ?assertEqual(4, last_chunk_id(R)),

    ok.

-endif.
