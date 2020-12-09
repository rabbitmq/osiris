%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_util).

-export([validate_base64uri/1,
         to_base64uri/1,
         id/1,
         lists_find/2]).

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01"
        "23456789_-=").

-spec validate_base64uri(string()) -> boolean().
validate_base64uri(Str) when is_list(Str) ->
    catch begin
              [begin
                   case lists:member(C, ?BASE64_URI_CHARS) of
                       true ->
                           ok;
                       false ->
                           throw(false)
                   end
               end
               || C <- string:to_graphemes(Str)],
              string:is_empty(Str) == false
          end.

-spec to_base64uri(string()) -> string().
to_base64uri(Str) when is_list(Str) ->
    lists:foldr(fun(G, Acc) ->
                   case lists:member(G, ?BASE64_URI_CHARS) of
                       true -> [G | Acc];
                       false -> [$_ | Acc]
                   end
                end,
                [], string:to_graphemes(Str)).

-spec id(term()) -> term().
id(X) ->
    X.

-spec lists_find(fun((term()) -> boolean()), list()) ->
                    {ok, term()} | not_found.
lists_find(_Pred, []) ->
    not_found;
lists_find(Pred, [Item | Rem]) ->
    case Pred(Item) of
        true ->
            {ok, Item};
        false ->
            lists_find(Pred, Rem)
    end.
