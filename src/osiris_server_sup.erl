%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(osiris_server_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).
-export([start/1, stop/1]).
-export([is_children/1,
         get_writer/1,
         get_writer/2,
         get_crc/1,
         get_crc/2]).

-define(SUP, osiris_server_sup_sup).

start_link(Config) ->
	supervisor:start_link(?MODULE, [Config]).

init([#{name := Name} = Config]) ->
    Writer = #{id => writer_name(Name),
               start => {osiris_writer, start_link, [Config]},
               restart => temporary,
               shutdown => 5000,
               type => worker},
    Crc = #{id => crc_name(Name),
            start => {osiris_competing_read_coordinator, start_link, [Config, self()]},
            restart => temporary,
            shutdown => 5000,
            type => worker},
    {ok, {{rest_for_one, 1, 5}, [Writer, Crc]}}.

start(Config = #{name := Name,
                 leader_node := Leader}) ->
    supervisor:start_child({?SUP, Leader},
                           #{id => supervisor_name(Name),
                             type => supervisor,
                             start => {?MODULE, start_link, [Config]}}).

stop(#{name := Name,
       leader_node := Leader}) ->
    CName = supervisor_name(Name),
    try
        _ = supervisor:terminate_child({?SUP, Leader}, CName),
        _ = supervisor:delete_child({?SUP, Leader}, CName),
        ok
    catch
        _:{noproc, _} ->
            %% Whole supervisor or app is already down - i.e. stop_app
            ok
    end.

is_children(#{name := Name,
              leader_node := Leader}) ->
    case supervisor:get_childspec({?SUP, Leader}, supervisor_name(Name)) of
        {ok, _} -> true;
        _ -> false
    end.

get_writer(SupPid, #{name := Name}) ->
    get_child(SupPid, writer_name(Name)).

get_writer(#{name := Name}) ->
    get_child(supervisor_name(Name), writer_name(Name)).

get_crc(SupPid, Name) ->
    get_child(SupPid, crc_name(Name)).

get_crc(Name) ->
    get_child(supervisor_name(Name), crc_name(Name)).

get_child(SupPid, Name) when is_pid(SupPid) ->
    case [Pid || {Id, Pid, _, _} <- supervisor:which_children(SupPid),
                 Id == Name] of
        [] ->
            {error, not_found};
        [Pid] ->
            {ok, Pid}
    end;
get_child(SupName, Name) ->
    case [Pid || {Id, Pid, _, _} <- supervisor:which_children(osiris_server_sup_sup),
                 Id == SupName] of
        [] ->
            {error, not_found};
        [Pid] ->
            get_child(Pid, Name)
    end.

supervisor_name(Name) ->
    lists:flatten(io_lib:format("~s_supervisor", [Name])).

writer_name(Name) ->
    lists:flatten(io_lib:format("~s_writer", [Name])).

crc_name(Name) ->
    lists:flatten(io_lib:format("~s_crc", [Name])).
