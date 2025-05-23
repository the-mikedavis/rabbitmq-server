%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(amqp10_client_app).

-behaviour(application).

%% Application callbacks
-export([start/2,
         stop/1]).

-type start_type() :: (
        normal |
        {takeover, Node :: node()} |
        {failover, Node :: node()}
       ).
-type state() :: term().

%%====================================================================
%% API
%%====================================================================

-spec start(StartType :: start_type(), StartArgs :: term()) ->
    {ok, Pid :: pid()} | {ok, Pid :: pid(), State :: state()} | {error, Reason :: term()}.
start(_Type, _Args) ->
    amqp10_client_sup:start_link().

-spec stop(State :: state()) -> ok.
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
