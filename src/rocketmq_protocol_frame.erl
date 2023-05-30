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

-include("rocketmq.hrl").

-define(HEART_BEAT, 34).
-define(GET_ROUTEINTO_BY_TOPIC, 105).
-define(SEND_MESSAGE_V2, 310).
-define(SEND_BATCH_MESSAGE, 320).

%% will be encoded as "\u0001" or "\u0002" by jsonr:encode/1 before sending
-define(NAME_VALUE_SEPARATOR, 1).
-define(PROPERTY_SEPARATOR, 2).

-export([ get_routeinfo_by_topic/4
        , send_message_v2/7
        , send_batch_message_v2/7
        , heart_beat/4
        , make_produce_props/1
        ]).

-export([parse/1]).

% 6 Read/Write
% 4 not write
% 2 not read

get_routeinfo_by_topic(Opaque, Namespace, Topic, ACLInfo) ->
    ExHeaders = [{<<"topic">>, maybe_with_namespace(Namespace, Topic)}],
    serialized(?GET_ROUTEINTO_BY_TOPIC, Opaque, ExHeaders, <<"">>, ACLInfo).

% Header field is
% ProducerGroup
% Topic
% QueueId
% BornTimestamp
% Properties
send_message_v2(Opaque, ProducerGroup, Namespace, Topic, QueueId, {Payload, Properties}, ACLInfo) ->
    ExtFields = basic_ext_headers(ProducerGroup, Namespace, Topic, QueueId, Properties)
        ++ single_message_fixed_headers(Namespace),
    serialized(?SEND_MESSAGE_V2, Opaque, ExtFields, Payload, ACLInfo).

send_batch_message_v2(Opaque, ProducerGroup, Namespace, Topic, QueueId, [{_, Properties} | _] = PayloadPropsList, ACLInfo) ->
    ExtFields = basic_ext_headers(ProducerGroup, Namespace, Topic, QueueId, Properties)
        ++ batch_message_fixed_headers(Namespace),
    serialized(?SEND_BATCH_MESSAGE, Opaque, ExtFields, batch_message(PayloadPropsList), ACLInfo).

heart_beat(Opaque, ClientID, GroupName, ACLInfo) ->
    Payload = [{<<"clientID">>, ClientID},
               {<<"consumerDataSet">>, []},
               {<<"producerDataSet">>, [[{<<"groupName">>, GroupName}],
                                        [{<<"groupName">>, <<"CLIENT_INNER_PRODUCER">>}]]}],
    serialized(?HEART_BEAT, Opaque, json_encode(Payload), ACLInfo).


parse(<<Len:32, HeaderLen:32, HeaderData:HeaderLen/binary, Bin/binary>>) ->
    case Bin == <<>> of
        true ->
            {json_decode(HeaderData), undefined, Bin};
        false ->
            case (Len - 4) - HeaderLen of
                0 ->
                    {json_decode(HeaderData), undefined, Bin};
                PayloadLen ->
                    <<Payload:PayloadLen/binary, Bin1/binary>> = Bin,
                    {json_decode(HeaderData), json_decode(Payload), Bin1}
            end
    end;
parse(Bin) ->
    {undefined, undefined, Bin}.

batch_message(PayloadPropsList) ->
    batch_message(PayloadPropsList, <<>>).
batch_message([], Acc) ->
    Acc;
batch_message([{Payload, Properties} | Payloads], Acc) ->
    %% https://github.com/apache/rocketmq/blob/06f2208a34907211591114f6b0d327168c250fb3/common/src/main/java/org/apache/rocketmq/common/message/MessageDecoder.java#L517
    MagicCode = 0,
    CRC = 0,
    Flag = 0,
    BodyLen = size(Payload),
    PropertiesLen = size(Properties),
    %% MagicCode(4) + CRC(4) + Flag(4) + BodyLen(4) + PropertiesLen(2) = 18,
    Len = 18 + BodyLen + size(Properties),
    NewAcc = <<Acc/binary, Len:32, MagicCode:32, CRC:32, Flag:32,
      BodyLen:32, Payload/binary, PropertiesLen:16, Properties/binary>>,
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
    Headers =
        case length(ExtFields) of
            0 -> [];
            _ ->
                [{<<"extFields">>, ExtFields}]
        end,
    serialized_(Code, Opaque, Headers, Payload);

serialized(Code, Opaque, ExtFields0, Payload, ACLInfo) when is_function(ACLInfo, 0) ->
    serialized(Code, Opaque, ExtFields0, Payload, unwrap(ACLInfo));

serialized(Code, Opaque, ExtFields, Payload, _ACLInfo) ->
    Headers =
        case length(ExtFields) of
            0 -> [];
            _ ->
                [{<<"extFields">>, ExtFields}]
        end,
    serialized_(Code, Opaque, Headers, Payload).

serialized_(Code, Opaque, Header0, Payload) ->
    Header = [{<<"code">>, Code},
              {<<"opaque">>, Opaque}] ++ Header0 ++ header_base(),
    HeaderData = json_encode(Header),
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

maybe_with_namespace(<<>>, ResourceName) -> ResourceName;
maybe_with_namespace(Namespace, ResourceName) ->
    <<Namespace/binary, "%", ResourceName/binary>>.

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
     {<<"version">>, 401}].

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

basic_ext_headers(ProducerGroup, Namespace, Topic, QueueId, Properties) ->
    [{<<"a">>, maybe_with_namespace(Namespace, ProducerGroup)},
     {<<"b">>, maybe_with_namespace(Namespace, Topic)},
     {<<"e">>, integer_to_binary(QueueId)},
     {<<"i">>, Properties},
     {<<"g">>, integer_to_binary(erlang:system_time(millisecond))}
    ].

single_message_fixed_headers(Namespace) ->
    [{<<"m">>, <<"false">>} | message_fixed_headers(Namespace)].

batch_message_fixed_headers(Namespace) ->
    [{<<"m">>, <<"true">>} | message_fixed_headers(Namespace)].

message_fixed_headers(Namespace) ->
    [{<<"c">>, maybe_with_namespace(Namespace, ?DEFAULT_TOPIC)},
     {<<"d">>, 8},
     {<<"f">>, 0},
     {<<"h">>, 0},
     {<<"j">>, 0},
     {<<"k">>, <<"false">>}
    ].

json_encode(Input) ->
    jsone:encode(Input).

json_decode(Input) ->
    jsone:decode(Input, [{allow_int_key, true}]).

make_produce_props(Context) when is_map(Context) ->
    do_make_produce_props(maps:to_list(Context), <<>>).

do_make_produce_props([], ProdContext) ->
    ProdContext;
do_make_produce_props([{key, Key} | Context], ProdContext) ->
    do_make_produce_props(Context, append_property(<<"KEYS">>, Key, ProdContext));
do_make_produce_props([{tag, Tag} | Context], ProdContext) ->
    do_make_produce_props(Context, append_property(<<"TAGS">>, Tag, ProdContext)).

append_property(Name, Val, <<>>) ->
    <<Name/binary, ?NAME_VALUE_SEPARATOR, Val/binary>>;
append_property(Name, Val, ProdContext) ->
    <<ProdContext/binary, ?PROPERTY_SEPARATOR, Name/binary, ?NAME_VALUE_SEPARATOR, Val/binary>>.

unwrap(Term) when is_function(Term, 0) ->
    unwrap(Term());
unwrap(Term) ->
    Term.
