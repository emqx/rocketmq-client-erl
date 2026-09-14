%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(rocketmq_protocol_frame_tests).

-include_lib("eunit/include/eunit.hrl").
-include("rocketmq.hrl").

%% Namespace handling of the auto-create default topic must match the
%% Java client: user topics and producer groups are prefixed, the
%% default topic is not.

route_request_prefixes_user_topic_test() ->
    Frame = rocketmq_protocol_frame:get_routeinfo_by_topic(1, <<"ns1">>, <<"t1">>, #{}),
    ?assertEqual(<<"ns1%t1">>, ext_field(<<"topic">>, Frame)).

route_request_keeps_default_topic_bare_test() ->
    Frame = rocketmq_protocol_frame:get_routeinfo_by_topic(1, <<"ns1">>, ?DEFAULT_TOPIC, #{}),
    ?assertEqual(?DEFAULT_TOPIC, ext_field(<<"topic">>, Frame)).

route_request_without_namespace_test() ->
    Frame = rocketmq_protocol_frame:get_routeinfo_by_topic(1, <<>>, <<"t1">>, #{}),
    ?assertEqual(<<"t1">>, ext_field(<<"topic">>, Frame)).

send_message_default_topic_header_bare_test() ->
    Frame = rocketmq_protocol_frame:send_message_v2(
              1, <<"g1">>, <<"ns1">>, <<"t1">>, 0, {<<"payload">>, <<>>}, #{}),
    ?assertEqual(<<"ns1%g1">>, ext_field(<<"a">>, Frame)),
    ?assertEqual(<<"ns1%t1">>, ext_field(<<"b">>, Frame)),
    ?assertEqual(?DEFAULT_TOPIC, ext_field(<<"c">>, Frame)).

send_batch_message_default_topic_header_bare_test() ->
    Frame = rocketmq_protocol_frame:send_batch_message_v2(
              1, <<"g1">>, <<"ns1">>, <<"t1">>, 0, [{<<"payload">>, <<>>}], #{}),
    ?assertEqual(<<"ns1%t1">>, ext_field(<<"b">>, Frame)),
    ?assertEqual(?DEFAULT_TOPIC, ext_field(<<"c">>, Frame)).

%% Decode only the header: message payloads are opaque, not JSON.
ext_field(Key, <<_Len:32, HeaderLen:32, HeaderData:HeaderLen/binary, _/binary>>) ->
    Header = jsone:decode(HeaderData),
    maps:get(Key, maps:get(<<"extFields">>, Header)).
