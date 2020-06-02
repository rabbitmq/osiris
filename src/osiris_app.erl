-module(osiris_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    Logger = application:get_env(osiris, logger_module, logger),
    osiris:configure_logger(Logger),
	osiris_sup:start_link().

stop(_State) ->
	ok.
