%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%

-module(osiris_bloom_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [{group, tests}].

all_tests() -> [
                basics,
                multi_matcher,
                match_unfiltered,
                eval_hash
               ].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

basics(_Config) ->
    S0 = osiris_bloom:init(16),
    S1 = osiris_bloom:insert(<<"banana">>, S0),
    S2 = osiris_bloom:insert(<<"apple">>, S1),
    B = osiris_bloom:to_binary(S2),

    ?assert(osiris_bloom:is_match(B, matcher(<<"banana">>))),
    ?assert(osiris_bloom:is_match(B, matcher(<<"apple">>))),
    ?assertNot(osiris_bloom:is_match(B, matcher(<<"pineapple">>))),
    ok.

multi_matcher(_Config) ->
    B1 = make_filter_bin([<<"banana">>], 16),
    B2 = make_filter_bin([<<"apple">>], 16),
    B3 = make_filter_bin([<<"pineapple">>], 16),

    Spec = #{filters =>[<<"banana">>, <<"apple">>]},
    M = osiris_bloom:init_matcher(Spec),
    ?assert(osiris_bloom:is_match(B1, M)),
    ?assert(osiris_bloom:is_match(B2, M)),
    ?assertNot(osiris_bloom:is_match(B3, M)),
    ok.

match_unfiltered(_Config) ->

    <<>> = make_filter_bin([<<>>], 16),
    Spec = #{filters => [<<"banana">>, <<"apple">>],
             match_unfiltered => true},
    M = osiris_bloom:init_matcher(Spec),
    %% first check the "empty" filter (i.e. none of the messages in the chunk
    %% had a filter)
    ?assert(osiris_bloom:is_match(<<>>, M)),
    %% then check if the match unfiltered is false or missing it does not match
    M2 = osiris_bloom:init_matcher(Spec#{match_unfiltered => false}),
    ?assertNot(osiris_bloom:is_match(<<>>, M2)),

    %% the empty filter indicates unfiltered
    B1 = make_filter_bin([<<>>, <<"banana">>], 16),
    %% the first bit should be set to indicate stuff
    ?assertMatch(<<1:1, _/bitstring>>, B1),
    ?assert(osiris_bloom:is_match(B1, M)),

    B32 = make_filter_bin([<<"pineapple">>], 17),
    {retry_with, M32} = osiris_bloom:is_match(B32, M),
    ?assertNot(osiris_bloom:is_match(B32, M32)),

    %% an empty filter combined with a non match should match
    B3 = make_filter_bin([<<>>, <<"pineapple">>], 16),
    ?assert(osiris_bloom:is_match(B3, M)),
    ok.

eval_hash(_Config) ->
    N = lists:seq(1, 1000),
    Size = 16,

    C = counters:new(Size * 8, []),

    [begin
         {H1, H2} = osiris_bloom:make_hash(
                      crypto:strong_rand_bytes(16), Size * 8),
         counters:add(C, H1+1, 1),
         counters:add(C, H2+1, 1)
     end || _ <- N],


    CList = [counters:get(C, I) || I <- lists:seq(1, Size * 8)],
    ct:pal("counters ~p", [CList]),

    ok.


%% helpers

matcher(Value) ->
    Spec = #{filters => [Value]},
    osiris_bloom:init_matcher(Spec).

make_filter_bin(Values, SizeB) ->
    S0 = osiris_bloom:init(SizeB),
    <<>> = osiris_bloom:to_binary(S0),
    S1 = lists:foldl(fun osiris_bloom:insert/2,
                     S0, Values),
    B = osiris_bloom:to_binary(S1),
    ?assert(byte_size(B) == SizeB orelse byte_size(B) == 0),
    B.

