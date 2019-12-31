-module(osiris).

-export([
         start_cluster/2,
         write/3
         ]).

-define(BASE64_URI_CHARS,
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789_-=").
%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

start_cluster(List, Replicas)
  when is_list(List) ->
    %% Why does the name have to be a list? We need an atom as process name for the gen_batch_server
    true = validate_base64uri(List),
    Name = list_to_atom(List),
    {ok, Pid} = osiris_writer:start(Name, #{}),
    ReplicaPids  = [element(2, osiris_replica:start(N, Name, Pid))
                    || N <- Replicas],
    {ok, Pid, ReplicaPids}.

write(Pid, Corr, Data) ->
    osiris_writer:write(Pid, self(), Corr, Data).


-spec validate_base64uri(string()) -> boolean().
validate_base64uri(Str) ->
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
