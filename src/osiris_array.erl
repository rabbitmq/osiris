%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_array).

-ifdef(TEST).
-include_lib("stdlib/include/assert.hrl").
-define(debugAssert(Expr), ?assert(Expr)).
-else.
-define(debugAssert(Expr), ok).
-endif.

%% An array type specialized for immutable sequences with fast routines for
%% sorted sequences.
%%
%% This array type supports zero-based indexing in constant time and provides
%% functions for searching in logarithmic time in sorted arrays.
%%
%% For comparison, the `array' module from `stdlib' is suitable for mutable
%% sequences since its tree-like structure amortizes expensive updates to the
%% entire array allocation. In contrast this type is suitable for immutable
%% sequences: initialized once and never updated.
%%
%% Internally this means converting the input list into a tuple. This is a
%% surprisingly fast and somewhat memory-efficient operation provided that
%% the output tuple is never modified later. Tuples are Erlang's true allocated
%% array type, providing constant-time indexing - perfect for binary search
%% data structures.

-compile({no_auto_import, [size/1]}).

-opaque array() :: tuple().
-type index() :: non_neg_integer().

-export_type([array/0,
              index/0]).

-export([from_list/1,
         size/1,
         at/2,
         get/2,
         binary_search/2,
         binary_search_by/2,
         partition_point/2]).

-define(IS_ARRAY(B), is_tuple(B)).

-spec from_list(list()) -> array().
from_list(List) when is_list(List) ->
    list_to_tuple(List).

%% @doc Returns the size of the array.
%%
%% This function computes the size in constant time.
-spec size(array()) -> non_neg_integer().
size(Array) when ?IS_ARRAY(Array) ->
    tuple_size(Array).

%% @doc Gets the element at the given zero-based index without bounds checking.
%%
%% If the array does not exist then this function exits with `badarg'.
-spec at(index(), array()) -> term() | no_return().
at(Index, Array)
  when is_integer(Index) andalso Index >= 0 andalso ?IS_ARRAY(Array) ->
    element(Index + 1, Array).

%% @doc Gets the element at the given zero-based index with bounds checking.
%%
%% If the index is with bounds this function returns `{ok, Element}' with the
%% element at that index, otherwise `error'.
-spec get(index(), array()) -> {ok, term()} | error.
get(Index, Array)
  when is_integer(Index) andalso Index >= 0 andalso ?IS_ARRAY(Array) ->
    Size = size(Array),
    case Index < Size of
        true ->
            {ok, at(Index, Array)};
        false ->
            error
    end.

%% @doc Searches for the index where an element exists or should be inserted
%% in logarithmic time.
%%
%% The array must be ordered in a way where all elements comparing `lt'
%% according to the comparison function must come before some index, an
%% optional element comparing `eq' comes after the `lt' elements, and any
%% number of elements comparing `gt' come after. Otherwise the output of this
%% function is meaningless.
%%
%% When this function returns `{error, index()}', the index points to the
%% position in the array where an element comparing `eq' would be inserted to
%% preserve the array's ordering.
-spec binary_search_by(CmpFn, array()) -> {ok | error, index()} when
    CmpFn :: fun((term()) -> lt | gt | eq).
binary_search_by(CmpFn, Array)
  when is_function(CmpFn, 1) andalso ?IS_ARRAY(Array) ->
    case size(Array) of
        0 -> {error, 0};
        Size -> binary_search_by(0, Size, CmpFn, Array)
    end.

binary_search_by(Base, _Size = 1, CmpFn, Array) ->
    case CmpFn(at(Base, Array)) of
        eq -> {ok, Base};
        gt -> {error, Base};
        lt -> {error, Base + 1}
    end;
binary_search_by(Base, Size, CmpFn, Array) ->
    %% Size is non-negative and non-zero: it must be at least 1.
    ?debugAssert(Size > 1),
    Half = Size div 2,
    Mid = Base + Half,
    case CmpFn(at(Mid, Array)) of
        %% NOTE: the implementation of this function is a translation of Rust's
        %% `slice::binary_search_by'. That implementation prioritizes informing
        %% the CPU branch predictor of the loop count by ensuring the number of
        %% iterations is proportional to the array length. Instead we add the
        %% early exit here for `eq' assuming that the cost of running `CmpFn'
        %% redundantly is much more expensive than any micro-optimization can
        %% make up. (This is true at time of writing for how this function is
        %% used in practice.)
        %%
        %% Ref: <https://github.com/rust-lang/rust/blob/ccf3198de316b488ee17441935182e9d5292b4d3/library/core/src/slice/mod.rs#L2887-L2939>.
        eq -> {ok, Mid};
        gt -> binary_search_by(Base, Size - Half, CmpFn, Array);
        lt -> binary_search_by(Mid, Size - Half, CmpFn, Array)
    end.

%% @doc Performs a binary search for the given element within the array.
%%
%% If the element exists in the array then this function returns `{ok, Index}'
%% with the index of that element. Otherwise this function returns `{error,
%% Index}' with the index into the array where the element would be inserted
%% to preserve ordering.
%%
%% If the array is not ordered then the result is meaningless.
-spec binary_search(Elem :: term(), array()) -> {ok | error, index()}.
binary_search(Elem, Array) when ?IS_ARRAY(Array) ->
    CmpFn = fun (E) when E > Elem -> gt;
                (E) when E < Elem -> lt;
                (_) -> eq
            end,
    binary_search_by(CmpFn, Array).

%% @doc Finds the index in the array where inserting an element would make the
%% predicate true.
%%
%% The array must be ordered in a way that the predicate always returns `true'
%% before the partition point and always returns `false' after. Otherwise the
%% index returned by this function is meaningless.
%%
%% If the predicate is always `false' then the index returned will be the size
%% of the array. If the array is empty or if the predicate always returns
%% `true' then this function returns zero.
-spec partition_point(Predicate, array()) -> index() when
    Predicate :: fun((term()) -> boolean()).
partition_point(Predicate, Array)
  when is_function(Predicate, 1) andalso ?IS_ARRAY(Array) ->
    CmpFn = fun(E) ->
                    case Predicate(E) of
                        true -> lt;
                        false -> gt
                    end
            end,
    {_ok_or_err, Idx} = binary_search_by(CmpFn, Array),
    Idx.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

get_test() ->
    Array = from_list(lists:seq(0, 10)),
    Size = size(Array),

    {ok, 3} = get(3, Array),
    error = get(Size, Array),
    error = get(0, from_list([])),

    ok.

binary_search_test() ->
    List = lists:seq(0, 10),
    Array = from_list(List),
    Size = size(Array),

    [begin
         {ok, Idx} = binary_search(Elem, Array),
         Elem = at(Idx, Array)
     end || Elem <- List],

    {error, Size} = binary_search(20, Array),
    {error, 0} = binary_search(-1, Array),

    ok.

partition_point_test() ->
    3 = partition_point(fun(N) -> N < 3 end, from_list([0, 1, 2, 3, 4, 5])),

    IsEven = fun(N) -> N rem 2 =:= 0 end,
    %% The index is the index where the element would be inserted to satisfy
    %% the predicate. Here the list turns from evens to odds at the fourth
    %% element. Inserting another even element at index 4 would keep the
    %% predicate satisfied.
    4 = partition_point(IsEven, from_list([2, 4, 6, 8, 1, 3, 5])),
    %% Same test as above but the midpoint in the first try will be `eq'
    %% because there are an equal number of events and odds. This is an edge
    %% case check.
    3 = partition_point(IsEven, from_list([2, 4, 6, 1, 3, 5])),
    %% Empty list always returns zero.
    0 = partition_point(IsEven, from_list([])),
    %% When the predicate always returns true the index is the size of the
    %% list.
    5 = partition_point(IsEven, from_list([2, 4, 6, 8, 10])),
    %% When it never returns true the index is zero.
    0 = partition_point(IsEven, from_list([1, 3, 5, 7, 9])),

    ok.

-endif.
