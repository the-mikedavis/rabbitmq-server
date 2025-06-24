-module(rabbit_snapshot_ticker).

-include_lib("kernel/include/logger.hrl").
-include("amqqueue.hrl").

-record(?MODULE, { members, interval }).

-export([start_link/2, init/1, handle_info/2, snapshot_index/1]).

start_link(Q, Interval) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, {Q, Interval}, []).

init({Q, Interval}) ->
    {ok, Members, _Leader} = ra:members(Q),
    erlang:send_after(Interval, self(), ping),
    {ok, #?MODULE{members = Members, interval = Interval}}.

handle_info(ping, #?MODULE{members = Members, interval = Interval} = State) ->
    print(Members),
    erlang:send_after(Interval, self(), ping),
    {noreply, State}.

print(Members) ->
    Overview = #{Member => snapshot_index(Member) || Member <- Members},
    ?LOG_ERROR("Overview: ~p", [Overview]),
    ok.

snapshot_index({_Q, Node} = Member) when Node =:= node() ->
    #{snapshot_index := Idx} = ra_counters:counters(Member, [snapshot_index]),
    Idx;
snapshot_index({_Q, Node} = Member) ->
    erpc:call(Node, ?MODULE, ?FUNCTION_NAME, [Member]).
