-module(rabbit_basic_get_chaos).

-export([start_link/0, run/1]).
-export([
    init/1,
    handle_continue/2,
    handle_info/2,
    handle_call/3,
    handle_cast/2
]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/logger.hrl").

-define(Q, <<"qq">>).
-define(NIL, []).
-define(INTERVAL, 5_000).
-define(BASIC_GET_BATCH_SIZE, 10).
-define(N_WORKERS, 3).

-record(?MODULE, {workers, mon_ref}).

-behaviour(gen_server).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
    {ok, #?MODULE{}, {continue, start}}.

handle_continue(start, State) ->
    ?LOG_WARNING("Spawning ~b workers", [?N_WORKERS]),
    Workers = #{spawn_worker() => ?NIL || _ <- lists:seq(1, ?N_WORKERS)},
    erlang:send_after(?INTERVAL, self(), chaos),
    {noreply, State#?MODULE{workers = Workers}}.

handle_info(chaos, #?MODULE{workers = Workers} = State0) ->
    {Pid, ?NIL, _} = maps:next(maps:iterator(Workers)),
    unlink(Pid),
    Ref = erlang:monitor(process, Pid),
    ?LOG_WARNING("Worker ~p will die", [Pid]),
    Pid ! die,
    State = State0#?MODULE{
        workers = maps:remove(Pid, Workers),
        mon_ref = Ref
    },
    {noreply, State};
handle_info(
    {'DOWN', Ref, process, _Pid, _Reason},
    #?MODULE{mon_ref = Ref, workers = Workers} = State0
) ->
    State = State0#?MODULE{workers = Workers#{spawn_worker() => ?NIL}},
    erlang:send_after(?INTERVAL, self(), chaos),
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Msg, _From, State) ->
    {reply, ok, State}.

spawn_worker() ->
    spawn_link(?MODULE, run, [?BASIC_GET_BATCH_SIZE]).

run(N) ->
    {ok, Conn} = amqp_connection:start(
        #amqp_params_direct{
            virtual_host = <<"/">>,
            username = <<"guest">>,
            password = <<"guest">>
        }
    ),
    link(Conn),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    link(Ch),

    run(N, Ch, false).

run(N, Ch, Die) ->
    Result = basic_get(Ch, N),

    case Result of
        {some, DeliveryTag} when not Die ->
            ok = amqp_channel:cast(
                Ch,
                #'basic.ack'{delivery_tag = DeliveryTag, multiple = true}
            );
        _ ->
            ok
    end,

    case Die of
        true ->
            ?LOG_WARNING("Worker ~p dying", [self()]),
            ok;
        false ->
            receive
                die ->
                    run(N, Ch, true)
            after 10 ->
                run(N, Ch, false)
            end
    end.

basic_get(Ch, N) ->
    basic_get(Ch, N, none).
    
basic_get(_Ch, 0, Tag) ->
    Tag;
basic_get(Ch, N, Tag0) ->
    Tag = case amqp_channel:call(Ch, #'basic.get'{queue = ?Q}) of
        {#'basic.get_ok'{delivery_tag = DeliveryTag}, _} ->
            {some, DeliveryTag};
        #'basic.get_empty'{} ->
            Tag0
    end,
    basic_get(Ch, N - 1, Tag).
