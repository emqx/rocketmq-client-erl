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

-module(rocketmq).

-export([start/0]).

%% Supervised client management APIs
-export([ ensure_supervised_client/2
        , ensure_supervised_client/3
        , stop_and_delete_supervised_client/1
        ]).

%% Primitive producer worker management APIs
-export([ ensure_supervised_producers/4
        , stop_and_delete_supervised_producers/1
        ]).

%% Messaging APIs
-export([ send/2
        , send_sync/3
        , batch_send_sync/3
        ]).

start() ->
    application:start(rocketmq).

ensure_supervised_client(ClientId, Opts) ->
    rocketmq_client_sup:ensure_present(ClientId, [{"127.0.0.1", 9876}], Opts).

ensure_supervised_client(ClientId, Hosts, Opts) ->
    rocketmq_client_sup:ensure_present(ClientId, Hosts, Opts).

stop_and_delete_supervised_client(ClientId) ->
    rocketmq_client_sup:ensure_absence(ClientId).

ensure_supervised_producers(ClientId, ProducerGroup, Topic, Opts) ->
    rocketmq_producers:start_supervised(ClientId, ProducerGroup, Topic, Opts).

stop_and_delete_supervised_producers(Producers) ->
    rocketmq_producers:stop_supervised(Producers).

-spec send(rocketmq_producers:producers(),
           binary() | {binary(), rocketmq_producers:produce_context()}) -> ok.
send(Producers, Message) when is_binary(Message) ->
    send(Producers, {Message, _Context = #{}});
send(Producers, {Message, Context}) when is_map(Context) ->
    {_Partition, ProducerPid} = rocketmq_producers:pick_producer(Producers, Context),
    Props = rocketmq_protocol_frame:make_produce_props(Context),
    rocketmq_producer:send(ProducerPid, {Message, Props}).

-spec send_sync(rocketmq_producers:producers(),
                binary() | {binary(), rocketmq_producers:produce_context()},
                timer:time()) -> ok | {error, term()}.
send_sync(Producers, Message, Timeout) when is_binary(Message) ->
    send_sync(Producers, {Message, _Context = #{}}, Timeout);
send_sync(Producers, {Message, Context}, Timeout) when is_map(Context) ->
    {_Partition, ProducerPid} = rocketmq_producers:pick_producer(Producers, Context),
    Props = rocketmq_protocol_frame:make_produce_props(Context),
    rocketmq_producer:send_sync(ProducerPid, {Message, Props}, Timeout).

-spec batch_send_sync(rocketmq_producers:producers(),
                list(binary() | {binary(), rocketmq_producers:produce_context()}),
                timer:time()) -> ok | {error, term()}.
batch_send_sync(Producers, Messages, Timeout) ->
    Context = fast_get_context(Messages),
    Messages2 = normalize_batch_messages(Messages),
    {_Partition, ProducerPid} = rocketmq_producers:pick_producer(Producers, Context),
    rocketmq_producer:batch_send_sync(ProducerPid, Messages2, Timeout).

fast_get_context([{_Bin, Context} | _]) ->
    Context;
fast_get_context(_) ->
    #{}.

normalize_batch_messages(Messages) ->
    lists:map(fun({Bin, _Context}) ->
                      {Bin, <<>>};
                 (Bin) ->
                      {Bin, <<>>}
              end,
              Messages).
