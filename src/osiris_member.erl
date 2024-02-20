-module(osiris_member).


-include("osiris.hrl").
-export([start/3,
         stop/2,
         delete/2]).

%% Callbacks

-callback start(node(), osiris:config()) ->
    supervisor:startchild_ret().

%% API

-spec start(module(), node(), osiris:config()) ->
    supervisor:startchild_ret().
start(Mod, Node, Config) ->
    Mod:start(Node, Config).

-spec stop(node(), osiris:config()) ->
    ok | {error, not_found}.
stop(Node, Config) ->
    ?SUP:stop_child(Node, Config).

-spec delete(node(), osiris:config()) ->
    ok | {error, term()}.
delete(Node, Config) ->
    ?SUP:delete_child(Node, Config).
