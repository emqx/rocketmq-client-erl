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
-define(SEND_BATCH_MESSAGE, 320).

-export([ get_routeinfo_by_topic/3
        , send_message_v2/6
        , send_batch_message_v2/6
        , heart_beat/4
        ]).

-export([parse/1]).

% 6 Read/Write
% 4 not write
% 2 not read

get_routeinfo_by_topic(Opaque, Topic, ACLInfo) ->
    serialized(?GET_ROUTEINTO_BY_TOPIC, Opaque, [{<<"topic">>, Topic}], <<"">>, ACLInfo).

% Header field is
% ProducerGroup
% Topic
% QueueId
% BornTimestamp
% Properties
send_message_v2(Opaque, ProducerGroup, Topic, QueueId, {Payload, Properties}, ACLInfo) ->
    ExtFields = [{<<"a">>, ProducerGroup},
                 {<<"b">>, Topic},
                 {<<"e">>, integer_to_binary(QueueId)},
                 {<<"i">>, Properties},
                 {<<"g">>, integer_to_binary(erlang:system_time(millisecond))}
                 | single_message_fixed_headers()],
    serialized(?SEND_MESSAGE_V2, Opaque, ExtFields, Payload, ACLInfo).

send_batch_message_v2(Opaque, ProducerGroup, Topic, QueueId, Payloads, ACLInfo) ->
    ExtFields = [{<<"a">>, ProducerGroup},
                 {<<"b">>, Topic},
                 {<<"e">>, integer_to_binary(QueueId)},
                 {<<"i">>, <<>>},
                 {<<"g">>, integer_to_binary(erlang:system_time(millisecond))}
                 | batch_message_fixed_headers()],
    I = batch_message(Payloads),
    serialized(?SEND_BATCH_MESSAGE, Opaque, ExtFields, I, ACLInfo).

heart_beat(Opaque, ClientID, GroupName, ACLInfo) ->
    Payload = [{<<"clientID">>, ClientID},
               {<<"consumerDataSet">>, []},
               {<<"producerDataSet">>, [[{<<"groupName">>, GroupName}],
                                        [{<<"groupName">>, <<"CLIENT_INNER_PRODUCER">>}]]}],
    serialized(?HEART_BEAT, Opaque, jsonr:encode(Payload), ACLInfo).


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
    end;
parse(Bin) ->
    {undefined, undefined, Bin}.

batch_message(Payloads) ->
    batch_message(Payloads, <<>>).
batch_message([], Acc) ->
    Acc;
batch_message([{Payload, Properties} | Payloads], Acc) ->
    MagicCode = 0,
    Crc = 0,
    PayloadLen = size(Payload),
    Flag = 0,
    PropertiesLen = size(Properties),
    Len = 18 + PayloadLen + size(Properties),
    NewAcc = <<Acc/binary, Len:32, MagicCode:32, Crc:32, Flag:32,
      PayloadLen:32, Payload/binary, PropertiesLen:16>>,
    batch_message(Payloads, NewAcc).

serialized(Code, Opaque, Payload, ACLInfo) ->
    serialized(Code, Opaque, [], Payload, ACLInfo).

serialized(Code, Opaque, ExtFields0, Payload, ACLInfo = #{access_key := AK, secret_key := SK}) ->
    ExtFields1 =
        case maps:get(security_token, ACLInfo, undefined) of
            Token when is_binary(Token) ->
                [{<<"AccessKey">>, AK}, {<<"SecurityToken">>, Token} | ExtFields0];
            undefined ->
                [{<<"AccessKey">>, AK} | ExtFields0]
        end,
    %% must sort before sign.
    ExtFields2 = lists:sort(ExtFields1),
    Signature = sign(SK, ExtFields2, Payload),
    ExtFields = lists:sort([{<<"Signature">>, Signature} | ExtFields2]),
    Headers = [{<<"extFields">>, ExtFields}],
    serialized_(Code, Opaque, Headers, Payload);

serialized(Code, Opaque, ExtFields, Payload, _ACLInfo) ->
    Headers = [{<<"extFields">>, ExtFields}],
    serialized_(Code, Opaque, Headers, Payload).

serialized_(Code, Opaque, Header0, Payload) ->
    Header = [{<<"code">>, Code},
              {<<"opaque">>, Opaque}] ++ Header0 ++ header_base(),
    HeaderData = jsonr:encode(Header),
    HeaderLen = size(HeaderData),
    Len = 4 + HeaderLen + size(Payload),
    <<Len:32, HeaderLen:32, HeaderData/binary, Payload/binary>>.

sign(SK, ExtFields, Payload) ->
    EFBin = plist_value_binary(ExtFields),
    SignData = <<EFBin/binary, Payload/binary>>,
    base64:encode(crypto:mac(hmac, sha, SK, SignData)).

plist_value_binary(List) ->
    plist_value_binary(lists:reverse(List), <<>>).

plist_value_binary([], Res) -> Res;
plist_value_binary([{_K, V} | List], Res) ->
    VBin = bin(V),
    plist_value_binary(List, <<VBin/binary, Res/binary>>).


bin(Bin) when is_binary(Bin) -> Bin;
bin(Num) when is_number(Num) -> number_to_binary(Num);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
bin(List) when is_list(List) -> list_to_binary(List).

number_to_binary(Int) when is_integer(Int) ->
    integer_to_binary(Int);
number_to_binary(Float) when is_float(Float) ->
    float_to_binary(Float, [{decimals, 17}, compact]).

header_base() ->
    [{<<"flag">>, 0},
     {<<"language">>, <<"ERLANG">>},
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
% boolean m;// batch message;

single_message_fixed_headers() -> [{<<"m">>, <<"false">>} | message_fixed_headers()].

batch_message_fixed_headers() -> [{<<"m">>, <<"true">>} | message_fixed_headers()].

message_fixed_headers() ->
    [{<<"c">>, <<"TBW102">>},
     {<<"d">>, 8},
     {<<"f">>, 0},
     {<<"h">>, 0},
     {<<"j">>, 0},
     {<<"k">>, <<"false">>}
    ].
