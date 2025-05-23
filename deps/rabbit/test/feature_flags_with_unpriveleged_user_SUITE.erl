%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(feature_flags_with_unpriveleged_user_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         enable_feature_flag_when_ff_file_is_unwritable/1
        ]).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [
     {group, enabling_on_single_node},
     {group, enabling_in_cluster}
    ].

groups() ->
    [
     {enabling_on_single_node, [],
      [
       enable_feature_flag_when_ff_file_is_unwritable
      ]},
     {enabling_in_cluster, [],
      [
       enable_feature_flag_when_ff_file_is_unwritable
      ]}
    ].

%% This suite exists to allow running a portion of the feature_flags_SUITE
%% under separate conditions in ci

init_per_suite(Config) ->
    feature_flags_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    feature_flags_SUITE:end_per_suite(Config).

init_per_group(Group, Config) ->
    feature_flags_SUITE:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    feature_flags_SUITE:end_per_group(Group, Config).

init_per_testcase(Testcase, Config) ->
    feature_flags_SUITE:init_per_testcase(Testcase, Config).

end_per_testcase(Testcase, Config) ->
    feature_flags_SUITE:end_per_testcase(Testcase, Config).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

enable_feature_flag_when_ff_file_is_unwritable(Config) ->
    feature_flags_SUITE:enable_feature_flag_when_ff_file_is_unwritable(Config).
