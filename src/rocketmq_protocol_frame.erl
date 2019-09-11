%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------

-module(rocketmq_protocol_frame).

-define(HEART_BEAT, 34).
-define(GET_ROUTEINTO_BY_TOPIC, 105).
-define(SEND_MESSAGE_V2, 310).

-export([ get_routeinfo_by_topic/2
        , send_message_v2/5
        , send_batch_message_v2/5
        , heart_beat/3
        ]).

-export([parse/1]).

% 6 Read/Write
% 4 not write
% 2 not read

get_routeinfo_by_topic(Opaque, Topic) ->
    serialized(?GET_ROUTEINTO_BY_TOPIC, Opaque, [{<<"extFields">>, [{<<"topic">>, Topic}]}], <<"">>).

% Header field is
% ProducerGroup
% Topic
% QueueId
% BornTimestamp
% Properties
send_message_v2(Opaque, ProducerGroup, Topic, QueueId, {Payload, Properties}) ->
    Header = [{<<"a">>, ProducerGroup},
              {<<"b">>, Topic},
              {<<"e">>, integer_to_binary(QueueId)},
              {<<"i">>, Properties},
              {<<"g">>, integer_to_binary(erlang:system_time(millisecond))}],
    serialized(?SEND_MESSAGE_V2, Opaque, [{<<"extFields">>, Header ++ message_base()}], Payload).

send_batch_message_v2(Opaque, ProducerGroup, Topic, QueueId, Payloads) ->
    send_message_v2(Opaque, ProducerGroup, Topic, QueueId, {batch_message(Payloads), <<>>}).

heart_beat(Opaque, ClientID, GroupName) ->
    Payload = [{<<"clientID">>, ClientID},
               {<<"consumerDataSet">>, []},
               {<<"producerDataSet">>, [[{<<"groupName">>, GroupName}],
                                        [{<<"groupName">>, <<"CLIENT_INNER_PRODUCER">>}]]}],
    serialized(?HEART_BEAT, Opaque, jsonr:encode(Payload)).

parse(<<Len:32, HeaderLen:32, HeaderData:HeaderLen/binary, Bin/binary>>) ->
    case Bin == <<>> of
        true ->
            {jsonr:decode(HeaderData), undefined, Bin};
        false ->
            case (Len - 4) - HeaderLen of
                0 ->
                    {jsonr:decode(HeaderData), undefined, Bin};
                PayloadLen ->
                    <<Payload:PayloadLen/binary, Bin1/binary>> = Bin,
                    {jsonr:decode(HeaderData), jsonr:decode(Payload), Bin1}
            end
    end.

batch_message(Payloads) ->
    batch_message(Payloads, <<>>).
batch_message([], Acc) ->
    Acc;
batch_message([{Payload, Properties} | Payloads], Acc) ->
    MagicCode = 0,
    Crc = 0,
    PayloadLen = size(Payload),
    Properties = <<>>,
    PropertiesLen = size(Properties),
    Len = 10 + PayloadLen + size(Properties),
    NewAcc = <<Acc/binary, Len:32, MagicCode:32, Crc:32, PayloadLen:32, Payload/binary, PropertiesLen:16>>,
    batch_message(Payloads, NewAcc).

serialized(Code, Opaque, Payload) ->
    serialized(Code, Opaque, [], Payload).

serialized(Code, Opaque, Header0, Payload) ->
    Header = [{<<"code">>, Code},
              {<<"opaque">>, Opaque}] ++ Header0 ++ header_base(),
    HeaderData = jsonr:encode(Header),
    HeaderLen = size(HeaderData),
    Len = 4 + HeaderLen + size(Payload),
    <<Len:32, HeaderLen:32, HeaderData/binary, Payload/binary>>.

header_base() ->
    [{<<"flag">>, 0},
     {<<"language">>, <<"JAVA">>},
     {<<"serializeTypeCurrentRPC">>, <<"JSON">>},
     {<<"version">>, 315}].

% String a;// producerGroup;
% String b;// topic;
% String c;// defaultTopic;
% Integer d;// defaultTopicQueueNums;
% Integer e;// queueId;
% Integer f;// sysFlag;
% Long g;// bornTimestamp;
% Integer h;// flag;
% String i;// properties;
% Integer j;// reconsumeTimes;
% boolean k;// unitMode = false;
% boolean m;// ??;
message_base() ->
    [{<<"c">>, <<"TBW102">>},
     {<<"d">>, 8},
     {<<"f">>, 0},
     {<<"h">>, 0},
     {<<"j">>, 0},
     {<<"k">>, <<"false">>},
     {<<"m">>, <<"false">>}].