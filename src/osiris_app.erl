-module(osiris_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    osiris:configure_logger(logger),
	osiris_sup:start_link().

stop(_State) ->
	ok.
