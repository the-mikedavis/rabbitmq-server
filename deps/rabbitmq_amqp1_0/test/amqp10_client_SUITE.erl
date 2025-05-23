%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

all() ->
    [
      {group, tests},
      {group, metrics}
    ].

groups() ->
    [
     {tests, [], [
                  reliable_send_receive_with_outcomes,
                  publishing_to_non_existing_queue_should_settle_with_released,
                  open_link_to_non_existing_destination_should_end_session,
                  roundtrip_classic_queue_with_drain,
                  roundtrip_quorum_queue_with_drain,
                  roundtrip_stream_queue_with_drain,
                  amqp_stream_amqpl,
                  message_headers_conversion,
                  resource_alarm
                 ]},
     {metrics, [], [
                    auth_attempt_metrics
                   ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    application:ensure_all_started(amqp10_client),
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

init_per_group(Group, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config, [
                         {rmq_nodename_suffix, Suffix},
                         {amqp10_client_library, Group}
                        ]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%%% TESTS
%%%

reliable_send_receive_with_outcomes(Config) ->
    Outcomes = [accepted,
                modified,
                {modified, true, false, #{<<"fruit">> => <<"banana">>}},
                {modified, false, true, #{}},
                rejected,
                released],
    [begin
         reliable_send_receive(Config, Outcome)
     end || Outcome <- Outcomes],
    ok.

reliable_send_receive(Config, Outcome) ->
    Container = atom_to_binary(?FUNCTION_NAME, utf8),
    OutcomeBin = case is_atom(Outcome) of
                     true ->
                         atom_to_binary(Outcome, utf8);
                     false ->
                         O1 = atom_to_binary(element(1, Outcome), utf8),
                         O2 = atom_to_binary(element(2, Outcome), utf8),
                         <<O1/binary, "_", O2/binary>>
                 end,

    ct:pal("~s testing ~s", [?FUNCTION_NAME, OutcomeBin]),
    QName = <<Container/binary, OutcomeBin/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = [{<<"x-queue-type">>,
                                                         longstr, <<"quorum">>}]}),
    rabbit_ct_client_helpers:close_channel(Ch),
    %% reliable send and consume
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Address = <<"/amq/queue/", QName/binary>>,

    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Msg1 = amqp10_msg:new(DTag1, <<"body-1">>, false),
    %% Use the 2 byte AMQP boolean encoding, see AMQP §1.6.2
    True = {boolean, true},
    Msg2 = amqp10_msg:set_headers(#{durable => True}, Msg1),
    ok = amqp10_client:send_msg(Sender, Msg2),
    ok = wait_for_settlement(DTag1),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    flush("post sender close"),

    {ok, Connection2} = amqp10_client:open_connection(OpnConf),
    {ok, Session2} = amqp10_client:begin_session(Connection2),
    ReceiverLinkName = <<"test-receiver">>,
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session2,
                                                        ReceiverLinkName,
                                                        Address,
                                                        unsettled),
    {ok, Msg} = amqp10_client:get_msg(Receiver),
    ct:pal("got ~p", [amqp10_msg:body(Msg)]),
    ?assertEqual(true, amqp10_msg:header(durable, Msg)),

    ok = amqp10_client:settle_msg(Receiver, Msg, Outcome),

    flush("post accept"),

    ok = amqp10_client:detach_link(Receiver),
    ok = amqp10_client:close_connection(Connection2),

    ok.

publishing_to_non_existing_queue_should_settle_with_released(Config) ->
    Container = atom_to_binary(?FUNCTION_NAME, utf8),
    Suffix = <<"foo">>,
    %% does not exist
    QName = <<Container/binary, Suffix/binary>>,
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Address = <<"/exchange/amq.direct/", QName/binary>>,

    OpnConf = #{address => Host,
                port => Port,
                container_id => Container,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    ok = wait_for_credit(Sender),
    DTag1 = <<"dtag-1">>,
    %% create an unsettled message,
    %% link will be in "mixed" mode by default
    Msg1 = amqp10_msg:new(DTag1, <<"body-1">>, false),
    ok = amqp10_client:send_msg(Sender, Msg1),
    ok = wait_for_settlement(DTag1, released),

    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),
    flush("post sender close"),
    ok.

open_link_to_non_existing_destination_should_end_session(Config) ->
    Container = atom_to_list(?FUNCTION_NAME),
    Name = Container ++ "foo",
    Addresses = [
                 "/exchange/" ++ Name ++ "/bar",
                 "/amq/queue/" ++ Name
                ],
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    OpnConf = #{address => Host,
                port => Port,
                container_id => list_to_binary(Container),
                sasl => {plain, <<"guest">>, <<"guest">>}},

    [begin
         {ok, Connection} = amqp10_client:open_connection(OpnConf),
         {ok, Session} = amqp10_client:begin_session(Connection),
         SenderLinkName = <<"test-sender">>,
         ct:pal("Address ~p", [Address]),
         {ok, _} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    list_to_binary(Address)),

         wait_for_session_end(Session),
         ok = amqp10_client:close_connection(Connection),
         flush("post sender close")

     end || Address <- Addresses],
    ok.

roundtrip_classic_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    roundtrip_queue_with_drain(Config, <<"classic">>, QName).

roundtrip_quorum_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    roundtrip_queue_with_drain(Config, <<"quorum">>, QName).

roundtrip_stream_queue_with_drain(Config) ->
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    roundtrip_queue_with_drain(Config, <<"stream">>, QName).

roundtrip_queue_with_drain(Config, QueueType, QName) when is_binary(QueueType) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    Args = [{<<"x-queue-type">>, longstr, QueueType}],
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                           durable = true,
                                           arguments = Args}),
    % create a configuration map
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},

    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),

    wait_for_credit(Sender),

    Dtag = <<"my-tag">>,
    % create a new message using a delivery-tag, body and indicate
    % it's settlement status (true meaning no disposition confirmation
    % will be sent by the receiver).
    OutMsg = amqp10_msg:new(Dtag, <<"my-body">>, false),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    ok = wait_for_settlement(Dtag),

    flush("pre-receive"),
    % create a receiver link

    TerminusDurability = none,
    Filter = case QueueType of
                 <<"stream">> ->
                     #{<<"rabbitmq:stream-offset-spec">> => <<"first">>};
                 _ ->
                     #{}
             end,
    Properties = #{},
    {ok, Receiver} = amqp10_client:attach_receiver_link(Session, <<"test-receiver">>,
                                                        Address, unsettled,
                                                        TerminusDurability,
                                                        Filter, Properties),

    % grant credit and drain
    ok = amqp10_client:flow_link_credit(Receiver, 1, never, true),

    % wait for a delivery
    receive
        {amqp10_msg, Receiver, InMsg} ->
            ok = amqp10_client:accept_msg(Receiver, InMsg),
            wait_for_accepts(1),
            ok
    after 2000 ->
              flush("delivery_timeout"),
              exit(delivery_timeout)
    end,
    Dtag = <<"my-tag">>,
    OutMsg2 = amqp10_msg:new(Dtag, <<"my-body2">>, false),
    ok = amqp10_client:send_msg(Sender, OutMsg2),
    ok = wait_for_settlement(Dtag),

    %% no delivery should be made at this point
    receive
        {amqp10_msg, _, _} ->
            flush("unexpected_delivery"),
            exit(unexpected_delivery)
    after 500 ->
              ok
    end,

    flush("final"),
    ok = amqp10_client:detach_link(Sender),

    ok = amqp10_client:close_connection(Connection),
    ok.

%% Send a message with a body containing a single AMQP 1.0 value section
%% to a stream and consume via AMQP 0.9.1.
amqp_stream_amqpl(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    ContainerId = QName = atom_to_binary(?FUNCTION_NAME),

    amqp_channel:call(Ch, #'queue.declare'{
                             queue = QName,
                             durable = true,
                             arguments = [{<<"x-queue-type">>, longstr, <<"stream">>}]}),

    Address = <<"/amq/queue/", QName/binary>>,
    OpnConf = #{address => Host,
                port => Port,
                container_id => ContainerId,
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    wait_for_credit(Sender),
    OutMsg = amqp10_msg:new(<<"my-tag">>, {'v1_0.amqp_value', {binary, <<0, 255>>}}, true),
    ok = amqp10_client:send_msg(Sender, OutMsg),
    flush("final"),
    ok = amqp10_client:detach_link(Sender),
    ok = amqp10_client:close_connection(Connection),

    #'basic.qos_ok'{} =  amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                            prefetch_count = 1}),
    CTag = <<"my-tag">>,
    #'basic.consume_ok'{} = amqp_channel:subscribe(
                              Ch,
                              #'basic.consume'{
                                 queue = QName,
                                 consumer_tag = CTag,
                                 arguments = [{<<"x-stream-offset">>, longstr, <<"first">>}]},
                              self()),
    receive
        {#'basic.deliver'{consumer_tag = CTag,
                          redelivered  = false},
         #amqp_msg{props = #'P_basic'{type = <<"amqp-1.0">>}}} ->
            ok
    after 5000 ->
              exit(basic_deliver_timeout)
    end,
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    ok = rabbit_ct_client_helpers:close_channel(Ch).

message_headers_conversion(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    %% declare a quorum queue
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                            durable = true,
                                            arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]}),

    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,[rabbitmq_amqp1_0, convert_amqp091_headers_to_app_props, true]),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,[rabbitmq_amqp1_0, convert_app_props_to_amqp091_headers, true]),

    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},

    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),

    amqp10_to_amqp091_header_conversion(Session, Ch, QName, Address),

    amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address),
    delete_queue(Config, QName),
    ok = amqp10_client:close_connection(Connection),
    ok.

resource_alarm(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    QName  = atom_to_binary(?FUNCTION_NAME, utf8),
    Address = <<"/amq/queue/", QName/binary>>,
    Ch = rabbit_ct_client_helpers:open_channel(Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName}),

    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, Session} = amqp10_client:begin_session(Connection),
    {ok, Sender} = create_amqp10_sender(Session, Address),

    M1 = amqp10_msg:new(<<"t1">>, <<"m1">>, false),
    M2 = amqp10_msg:new(<<"t2">>, <<"m2">>, false),
    M3 = amqp10_msg:new(<<"t3">>, <<"m3">>, false),
    M4 = amqp10_msg:new(<<"t4">>, <<"m4">>, false),

    ok = amqp10_client:send_msg(Sender, M1),
    ok = wait_for_settlement(<<"t1">>),

    ok = rabbit_ct_broker_helpers:rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0]),
    %% Let connection block.
    timer:sleep(100),

    ok = amqp10_client:send_msg(Sender, M2),
    ok = amqp10_client:send_msg(Sender, M3),
    ok = amqp10_client:send_msg(Sender, M4),

    %% M2 still goes through, but M3 should get blocked. (Server is off by one message.)
    receive {amqp10_disposition, {accepted, <<"t2">>}} -> ok
    after 300 -> ct:fail({accepted_timeout, ?LINE})
    end,
    receive {amqp10_disposition, {accepted, <<"t3">>}} -> ct:fail("expected connection to be blocked")
    after 300 -> ok
    end,

    %% Unblock connection.
    ok = rabbit_ct_broker_helpers:rpc(Config, vm_memory_monitor, set_vm_memory_high_watermark, [0.4]),

    %% Previously sent M3 and M4 should now be processed on the server.
    receive {amqp10_disposition, {accepted, <<"t3">>}} -> ok
    after 5000 -> ct:fail({accepted_timeout, ?LINE})
    end,
    ok = wait_for_settlement(<<"t4">>),

    delete_queue(Config, QName),
    ok = amqp10_client:close_connection(Connection).

amqp10_to_amqp091_header_conversion(Session,Ch, QName, Address) -> 
    {ok, Sender} = create_amqp10_sender(Session, Address),

    OutMsg = amqp10_msg:new(<<"my-tag">>, <<"my-body">>, true),
    OutMsg2 = amqp10_msg:set_application_properties(#{
        "x-string" => "string-value",
        "x-int" => 3,
        "x-bool" => true
    }, OutMsg),
    ok = amqp10_client:send_msg(Sender, OutMsg2),
    wait_for_accepts(1),

    {ok, Headers} = amqp091_get_msg_headers(Ch, QName),

    ?assertEqual({bool, true}, rabbit_misc:table_lookup(Headers, <<"x-bool">>)),
    ?assertEqual({unsignedint, 3}, rabbit_misc:table_lookup(Headers, <<"x-int">>)),
    ?assertEqual({longstr, <<"string-value">>}, rabbit_misc:table_lookup(Headers, <<"x-string">>)).    


amqp091_to_amqp10_header_conversion(Session, Ch, QName, Address) -> 
    Amqp091Headers = [{<<"x-forwarding">>, array, 
                        [{table, [{<<"uri">>, longstr,
                                   <<"amqp://localhost/%2F/upstream">>}]}]},
                      {<<"x-string">>, longstr, "my-string"},
                      {<<"x-int">>, long, 92},
                      {<<"x-bool">>, bool, true}],

    amqp_channel:cast(Ch, 
        #'basic.publish'{exchange = <<"">>, routing_key = QName},
        #amqp_msg{props = #'P_basic'{
            headers = Amqp091Headers}, 
            payload = <<"foobar">> }
        ),

    {ok, [Msg]} = drain_queue(Session, Address, 1),
    Amqp10Props = amqp10_msg:application_properties(Msg),
    ?assertEqual(true, maps:get(<<"x-bool">>, Amqp10Props, undefined)),
    ?assertEqual(92, maps:get(<<"x-int">>, Amqp10Props, undefined)),    
    ?assertEqual(<<"my-string">>, maps:get(<<"x-string">>, Amqp10Props, undefined)).

auth_attempt_metrics(Config) ->
    Host = ?config(rmq_hostname, Config),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    % create a configuration map
    OpnConf = #{address => Host,
                port => Port,
                container_id => atom_to_binary(?FUNCTION_NAME, utf8),
                sasl => {plain, <<"guest">>, <<"guest">>}},
    open_and_close_connection(OpnConf),
    [Attempt] =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts, []),
    ?assertEqual(false, proplists:is_defined(remote_address, Attempt)),
    ?assertEqual(false, proplists:is_defined(username, Attempt)),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"amqp10">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt), 1),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, reset_auth_attempt_metrics, []),
    rabbit_ct_broker_helpers:rpc(Config, 0, application, set_env,
                                 [rabbit, track_auth_attempt_source, true]),
    open_and_close_connection(OpnConf),
    Attempts =
        rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_core_metrics, get_auth_attempts_by_source, []),
    [Attempt1] = lists:filter(fun(Props) ->
                                      proplists:is_defined(remote_address, Props)
                              end, Attempts),
    ?assertEqual(proplists:get_value(remote_address, Attempt1), <<>>),
    ?assertEqual(proplists:get_value(username, Attempt1), <<"guest">>),
    ?assertEqual(proplists:get_value(protocol, Attempt), <<"amqp10">>),
    ?assertEqual(proplists:get_value(auth_attempts, Attempt1), 1),
    ?assertEqual(proplists:get_value(auth_attempts_failed, Attempt1), 0),
    ?assertEqual(proplists:get_value(auth_attempts_succeeded, Attempt1), 1),
    ok.

%% internal
%%

flush(Prefix) ->
    receive
        Msg ->
            ct:pal("~ts flushed: ~w~n", [Prefix, Msg]),
            flush(Prefix)
    after 1 ->
              ok
    end.

open_and_close_connection(OpnConf) ->
    {ok, Connection} = amqp10_client:open_connection(OpnConf),
    {ok, _} = amqp10_client:begin_session(Connection),
    ok = amqp10_client:close_connection(Connection).

% before we can send messages we have to wait for credit from the server
wait_for_credit(Sender) ->
    receive
        {amqp10_event, {link, Sender, credited}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_credit timed out"),
              ct:fail(credited_timeout)
    end.

wait_for_session_end(Session) ->
    receive
        {amqp10_event, {session, Session, {ended, _}}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_session_end timed out"),
              ct:fail(settled_timeout)
    end.

wait_for_settlement(Tag) ->
    wait_for_settlement(Tag, accepted).

wait_for_settlement(Tag, State) ->
    receive
        {amqp10_disposition, {State, Tag}} ->
            flush(?FUNCTION_NAME),
            ok
    after 5000 ->
              flush("wait_for_settlement timed out"),
              ct:fail({settled_timeout, Tag})
    end.

wait_for_accepts(0) -> ok;
wait_for_accepts(N) ->
    receive
        {amqp10_disposition,{accepted,_}} ->
            wait_for_accepts(N -1)
    after 250 ->
              ok
    end.

delete_queue(Config, QName) -> 
    Ch = rabbit_ct_client_helpers:open_channel(Config, 0),
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch).


amqp091_get_msg_headers(Channel, QName) -> 
    {#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{ headers= Headers}}}
        = amqp_channel:call(Channel, #'basic.get'{queue = QName, no_ack = true}),
    {ok, Headers}.

create_amqp10_sender(Session, Address) -> 
    SenderLinkName = <<"test-sender">>,
    {ok, Sender} = amqp10_client:attach_sender_link(Session,
                                                    SenderLinkName,
                                                    Address),
    wait_for_credit(Sender),
    {ok, Sender}.

    drain_queue(Session, Address, N) -> 
        flush("Before drain_queue"),
        {ok, Receiver} = amqp10_client:attach_receiver_link(Session,
                        <<"test-receiver">>,
                        Address, 
                        settled,
                        configuration),
    
        ok = amqp10_client:flow_link_credit(Receiver, 1000, never, true),
        Msgs = receive_message(Receiver, N, []),
        flush("after drain"),
        ok = amqp10_client:detach_link(Receiver),
        {ok, Msgs}.
    
receive_message(_Receiver, 0, Acc) -> lists:reverse(Acc);
receive_message(Receiver, N, Acc) ->
    receive
        {amqp10_msg, Receiver, Msg} -> 
            receive_message(Receiver, N-1, [Msg | Acc])
    after 5000  ->
            exit(receive_timed_out)
    end.
