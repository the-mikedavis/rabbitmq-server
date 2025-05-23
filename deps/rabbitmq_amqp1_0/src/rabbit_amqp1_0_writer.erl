%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp1_0_writer).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("rabbit_amqp1_0.hrl").

-export([start/5, start_link/5, start/6, start_link/6]).
-export([send_command/2, send_command/3,
         send_command_sync/2, send_command_sync/3,
         send_command_and_notify/4, send_command_and_notify/5]).
-export([internal_send_command/4]).

%% internal
-export([mainloop/1, mainloop1/1]).

-record(wstate, {sock, channel, frame_max, protocol, reader,
                 stats_timer, pending}).

-define(HIBERNATE_AFTER, 6_000).
-define(AMQP_SASL_FRAME_TYPE, 1).

%%---------------------------------------------------------------------------

-spec start
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid())
        -> rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid())
        -> rabbit_types:ok(pid()).
-spec start
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(), boolean())
        -> rabbit_types:ok(pid()).
-spec start_link
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         non_neg_integer(), rabbit_types:protocol(), pid(), boolean())
        -> rabbit_types:ok(pid()).
-spec send_command
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok'.
-spec send_command_sync
        (pid(), rabbit_framing:amqp_method_record()) -> 'ok'.
-spec send_command_sync
        (pid(), rabbit_framing:amqp_method_record(), rabbit_types:content())
        -> 'ok'.
-spec send_command_and_notify
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record())
        -> 'ok'.
-spec send_command_and_notify
        (pid(), pid(), pid(), rabbit_framing:amqp_method_record(),
         rabbit_types:content())
        -> 'ok'.
-spec internal_send_command
        (rabbit_net:socket(), rabbit_channel:channel_number(),
         rabbit_framing:amqp_method_record(), 'amqp10_framing' | 'rabbit_amqp1_0_sasl')
        -> 'ok'.

%%---------------------------------------------------------------------------

start(Sock, Channel, FrameMax, Protocol, ReaderPid) ->
    start(Sock, Channel, FrameMax, Protocol, ReaderPid, false).

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid) ->
    start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, false).

start(Sock, Channel, FrameMax, Protocol, ReaderPid, ReaderWantsStats) ->
    State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
                          ReaderWantsStats),
    {ok, proc_lib:spawn(?MODULE, mainloop, [State])}.

start_link(Sock, Channel, FrameMax, Protocol, ReaderPid, ReaderWantsStats) ->
    State = initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid,
                          ReaderWantsStats),
    {ok, proc_lib:spawn_link(?MODULE, mainloop, [State])}.

initial_state(Sock, Channel, FrameMax, Protocol, ReaderPid, ReaderWantsStats) ->
    (case ReaderWantsStats of
         true  -> fun rabbit_event:init_stats_timer/2;
         false -> fun rabbit_event:init_disabled_stats_timer/2
     end)(#wstate{sock      = Sock,
                  channel   = Channel,
                  frame_max = FrameMax,
                  protocol  = Protocol,
                  reader    = ReaderPid,
                  pending   = []},
          #wstate.stats_timer).

mainloop(State) ->
    try
        mainloop1(State)
    catch
        exit:Error -> #wstate{reader = ReaderPid, channel = Channel} = State,
                      ReaderPid ! {channel_exit, Channel, Error}
    end,
    done.

mainloop1(State = #wstate{pending = []}) ->
    receive
        Message -> ?MODULE:mainloop1(handle_message(Message, State))
    after ?HIBERNATE_AFTER ->
            erlang:hibernate(?MODULE, mainloop, [State])
    end;
mainloop1(State) ->
    receive
        Message -> ?MODULE:mainloop1(handle_message(Message, State))
    after 0 ->
            ?MODULE:mainloop1(flush(State))
    end.

handle_message({send_command, MethodRecord}, State) ->
    internal_send_command_async(MethodRecord, State);
handle_message({send_command, MethodRecord, Content}, State) ->
    internal_send_command_async(MethodRecord, Content, State);
handle_message({'$gen_call', From, {send_command_sync, MethodRecord}}, State) ->
    State1 = flush(internal_send_command_async(MethodRecord, State)),
    gen_server:reply(From, ok),
    State1;
handle_message({'$gen_call', From, {send_command_sync, MethodRecord, Content}},
               State) ->
    State1 = flush(internal_send_command_async(MethodRecord, Content, State)),
    gen_server:reply(From, ok),
    State1;
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord}, State) ->
    State1 = internal_send_command_async(MethodRecord, State),
    rabbit_amqqueue:notify_sent(QPid, ChPid),
    State1;
handle_message({send_command_and_notify, QPid, ChPid, MethodRecord, Content},
               State) ->
    State1 = internal_send_command_async(MethodRecord, Content, State),
    rabbit_amqqueue:notify_sent(QPid, ChPid),
    State1;
handle_message({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue:notify_sent_queue_down(QPid),
    State;
handle_message(emit_stats, State = #wstate{reader = ReaderPid}) ->
    ReaderPid ! ensure_stats,
    rabbit_event:reset_stats_timer(State, #wstate.stats_timer);
handle_message(Message, _State) ->
    exit({writer, message_not_understood, Message}).

%%---------------------------------------------------------------------------

send_command(W, MethodRecord) ->
    W ! {send_command, MethodRecord},
    ok.

send_command(W, MethodRecord, Content) ->
    W ! {send_command, MethodRecord, Content},
    ok.

send_command_sync(W, MethodRecord) ->
    call(W, {send_command_sync, MethodRecord}).

send_command_sync(W, MethodRecord, Content) ->
    call(W, {send_command_sync, MethodRecord, Content}).

send_command_and_notify(W, Q, ChPid, MethodRecord) ->
    W ! {send_command_and_notify, Q, ChPid, MethodRecord},
    ok.

send_command_and_notify(W, Q, ChPid, MethodRecord, Content) ->
    W ! {send_command_and_notify, Q, ChPid, MethodRecord, Content},
    ok.

%%---------------------------------------------------------------------------

call(Pid, Msg) ->
    {ok, Res} = gen:call(Pid, '$gen_call', Msg, infinity),
    Res.

%%---------------------------------------------------------------------------

%% Begin 1-0

assemble_frame(Channel, Performative, amqp10_framing) ->
    ?DEBUG("Channel ~tp <-~n~tp",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, PerfBin);

assemble_frame(Channel, Performative, rabbit_amqp1_0_sasl) ->
    ?DEBUG("Channel ~tp <-~n~tp",
           [Channel, amqp10_framing:pprint(Performative)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel,
                                                ?AMQP_SASL_FRAME_TYPE, PerfBin).

%% Note: a transfer record can be followed by a number of other
%% records to make a complete frame but unlike 0-9-1 we may have many
%% content records. However, that's already been handled for us, we're
%% just sending a chunk, so from this perspective it's just a binary.

assemble_frames(Channel, Performative, Content, _FrameMax,
                amqp10_framing) ->
    ?DEBUG("Channel ~tp <-~n~tp~n  followed by ~tp bytes of content",
           [Channel, amqp10_framing:pprint(Performative),
            iolist_size(Content)]),
    PerfBin = amqp10_framing:encode_bin(Performative),
    amqp10_binary_generator:build_frame(Channel, [PerfBin, Content]).

%% End 1-0

tcp_send(Sock, Data) ->
    rabbit_misc:throw_on_error(inet_error,
                               fun () -> rabbit_net:send(Sock, Data) end).

internal_send_command(Sock, Channel, MethodRecord, Protocol) ->
    ok = tcp_send(Sock, assemble_frame(Channel, MethodRecord, Protocol)).

internal_send_command_async(MethodRecord,
                            State = #wstate{channel   = Channel,
                                            protocol  = Protocol,
                                            pending   = Pending}) ->
    Frame = assemble_frame(Channel, MethodRecord, Protocol),
    maybe_flush(State#wstate{pending = [Frame | Pending]}).

internal_send_command_async(MethodRecord, Content,
                            State = #wstate{channel   = Channel,
                                            frame_max = FrameMax,
                                            protocol  = Protocol,
                                            pending   = Pending}) ->
    Frames = assemble_frames(Channel, MethodRecord, Content, FrameMax,
                             Protocol),
    maybe_flush(State#wstate{pending = [Frames | Pending]}).

%% This magic number is the tcp-over-ethernet MSS (1460) minus the
%% minimum size of a AMQP basic.deliver method frame (24) plus basic
%% content header (22). The idea is that we want to flush just before
%% exceeding the MSS.
-define(FLUSH_THRESHOLD, 1414).

maybe_flush(State = #wstate{pending = Pending}) ->
    case iolist_size(Pending) >= ?FLUSH_THRESHOLD of
        true  -> flush(State);
        false -> State
    end.

flush(State = #wstate{pending = []}) ->
    State;
flush(State0 = #wstate{sock = Sock, pending = Pending}) ->
    case rabbit_net:send(Sock, lists:reverse(Pending)) of
        ok ->
            ok;
        {error, Reason} ->
            exit({writer, send_failed, Reason})
    end,
    State = State0#wstate{pending = []},
    rabbit_event:ensure_stats_timer(State, #wstate.stats_timer, emit_stats).
