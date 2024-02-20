-module(osiris_member).

-export([start/3,
         stop/3,
         delete/3]).

%% Callbacks

-callback start(node(), osiris:config()) ->
    supervisor:startchild_ret().

-callback stop(node(), osiris:config()) ->
    ok | {error, not_found}.

-callback delete(node(), osiris:config()) ->
    ok | {error, term()}.

%% API

-spec start(module(), node(), osiris:config()) ->
    supervisor:startchild_ret().
start(Mod, Node, Config) ->
    Mod:start(Node, Config).

-spec stop(module(), node(), osiris:config()) ->
    ok | {error, not_found}.
stop(Mod, Node, Config) ->
    Mod:stop(Node, Config).

-spec delete(module(), node(), osiris:config()) ->
    ok | {error, term()}.
delete(Mod, Node, Config) ->
    Mod:delete(Node, Config).
