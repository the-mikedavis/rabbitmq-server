%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_ff).

-include("rabbit_mqtt.hrl").

-export([track_client_id_in_ra/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Feature flags introduced in 3.12.0 %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-rabbit_feature_flag(
   {?QUEUE_TYPE_QOS_0,
    #{desc          => "Support pseudo queue type for MQTT QoS 0 subscribers omitting a queue process",
      stability     => stable
     }}).

-rabbit_feature_flag(
   {delete_ra_cluster_mqtt_node,
    #{desc          => "Delete Ra cluster 'mqtt_node' since MQTT client IDs are tracked locally",
      stability     => stable,
      callbacks     => #{enable => {mqtt_node, delete}}
     }}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Feature flags introduced in 3.13.0 %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% This feature flag is needed:
%% 1. to prevent clients from downgrading an MQTT v5 session to v3 or v4 on a lower version node.
%%    Such a session downgrade (or upgrade) requires changing binding arguments.
%% 2. to ensure function rabbit_mqtt_processor:remove_duplicate_client_id_connections/3 is
%%    available on all nodes to support Will Delay Interval.
-rabbit_feature_flag(
   {mqtt_v5,
    #{desc       => "Support MQTT 5.0",
      stability  => stable,
      depends_on => [
                     %% MQTT 5.0 feature Will Delay Interval depends on client ID tracking in pg local.
                     delete_ra_cluster_mqtt_node,
                     message_containers
                    ]
     }}).

-spec track_client_id_in_ra() -> boolean().
track_client_id_in_ra() ->
    rabbit_feature_flags:is_disabled(delete_ra_cluster_mqtt_node).
