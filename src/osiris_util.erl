-module(osiris_util).

-export([validate_base64uri/1,
         to_base64uri/1]).

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-=").

-spec validate_base64uri(string()) -> boolean().
validate_base64uri(Str) when is_list(Str) ->
    catch
    begin
        [begin
             case lists:member(C, ?BASE64_URI_CHARS) of
                 true -> ok;
                 false -> throw(false)
             end
         end || C <- string:to_graphemes(Str)],
        string:is_empty(Str) == false
    end.

-spec to_base64uri(string()) -> string().
to_base64uri(Str) when is_list(Str) ->
    lists:foldr(fun(G, Acc) ->
                        case lists:member(G, ?BASE64_URI_CHARS) of
                            true -> [G | Acc];
                            false -> [$_ | Acc]
                        end
                end, [], string:to_graphemes(Str)).
