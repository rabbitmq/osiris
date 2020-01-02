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

-spec start_cluster(string(), [node()]) ->
    {ok, pid(), [pid()]}.
start_cluster(Name0, Replicas)
  when is_list(Name0) ->
    %% Why does the name have to be a list? We need an atom as process name for the gen_batch_server
    true = validate_base64uri(Name0),
    Name = list_to_atom(Name0),
    {ok, Pid} = osiris_writer:start(Name, #{replica_nodes => Replicas}),
    ReplicaPids = [element(2, osiris_replica:start(N, Name, Pid))
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
