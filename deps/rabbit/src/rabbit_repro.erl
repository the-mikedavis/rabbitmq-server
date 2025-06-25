-module(rabbit_repro).

-export([run/0]).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(Q, <<"qq">>).
-define(N_MESSAGES, 10).

run() ->
    {_, BasicGetRunnerRef} = spawn_monitor(fun() ->
        {ok, Conn} = connection(),
        link(Conn),
        {ok, Ch} = amqp_connection:open_channel(Conn),
        link(Ch),

        %% Get 10 messages and then exit without checking them out so that
        %% they are returned.
        basic_get(Ch, ?N_MESSAGES),
        ok
    end),

    receive
        {'DOWN', BasicGetRunnerRef, process, _, normal} ->
            ok;
        {'DOWN', BasicGetRunnerRef, process, _, _} ->
            exit(basic_get_runner_fail)
    end,

    %% Probably not necessary but let's let everything settle. Wait for 1/2
    %% second.
    timer:sleep(500),

    {_, BasicRejectRunnerRef} = spawn_monitor(fun() ->
        {ok, Conn} = connection(),
        link(Conn),
        {ok, Ch} = amqp_connection:open_channel(Conn),
        link(Ch),

        %% Get and ack 9 messages.
        {some, AckDeliveryTag} = basic_get(Ch, ?N_MESSAGES - 1),
        amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = AckDeliveryTag,
                                           multiple = true}),

        %% Then get and reject the last returned message.
        {some, RejectDeliveryTag} = basic_get(Ch, 1),
        amqp_channel:cast(Ch, #'basic.reject'{delivery_tag = RejectDeliveryTag,
                                              requeue = true}),
        ok
    end),

    receive
        {'DOWN', BasicRejectRunnerRef, process, _, normal} ->
            ok;
        {'DOWN', BasicRejectRunnerRef, process, _, _} ->
            exit(basic_reject_runner_fail)
    end,

    ok.

connection() ->
    amqp_connection:start(#amqp_params_direct{virtual_host = <<"/">>,
                                              username = <<"guest">>,
                                              password = <<"guest">>}).

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
