-module(rabbit_repro).

-export([run/0, purge/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(Q, <<"qq">>).
-define(N_MESSAGES, 10).

purge() ->
    Self = self(),
    spawn(fun() ->
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
        Self ! amqp_channel:call(Ch, #'queue.purge'{queue = ?Q}),
        ok
    end),
    ok.

run() ->
    ?assert(?N_MESSAGES > 1),
    ?LOG_INFO("Starting connection"),
    Runner = self(),

    %% Check out some messages and then crash the channel process so they're
    %% all returned.
    {_, BasicGetRunnerRef} = spawn_monitor(fun() ->
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

        basic_get(Ch, ?N_MESSAGES),
        ?LOG_INFO("basic.get worker exiting"),
        Runner ! done,
        ok
    end),

    receive
        done ->
            ok;
        {'DOWN', BasicGetRunnerRef, process, _, _} ->
            exit(basic_get_runner_fail)
    end,

    %% Probably not necessary but let's let everything settle:
    timer:sleep(500),

    {_, BasicRejectRunnerRef} = spawn_monitor(fun() ->
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

        {some, AckDeliveryTag} = basic_get(Ch, ?N_MESSAGES - 1),
        amqp_channel:cast(
            Ch,
            #'basic.ack'{delivery_tag = AckDeliveryTag, multiple = true}
        ),

        {some, RejectDeliveryTag} = basic_get(Ch, 1),
        amqp_channel:cast(
            Ch,
            #'basic.reject'{delivery_tag = RejectDeliveryTag, requeue = true}
        ),
        Runner ! done,
        ok
    end),

    receive
        done ->
            ok;
        {'DOWN', BasicRejectRunnerRef, process, _, _} ->
            exit(basic_reject_runner_fail)
    end,

    ok.

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
