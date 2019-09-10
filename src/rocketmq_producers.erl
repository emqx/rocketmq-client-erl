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

-module(rocketmq_producers).

%% APIs
-export([start_link/4]).
%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).

-export([ start_supervised/4
        , stop_supervised/1
        ]).

-export([pick_producer/1]).

-record(state, {topic,
                client_id,
                workers,
                queue_nums,
                producer_opts,
                producers = #{},
                producer_group}).

start_supervised(ClientId, ProducerGroup, Topic, ProducerOpts) ->
  {ok, Pid} = rocketmq_producers_sup:ensure_present(ClientId, ProducerGroup, Topic, ProducerOpts),
  {QueueNums, Workers} = gen_server:call(Pid, get_workers, infinity),
  {ok, #{client => ClientId,
         topic => Topic,
         workers => Workers,
         queue_nums => QueueNums
        }}.

stop_supervised(#{client := ClientId, topic := Topic}) ->
  rocketmq_producers_sup:ensure_absence(ClientId, Topic).

pick_producer(#{workers := Workers, queue_nums := QueueNums}) ->
    QueueNum = pick_queue_num(QueueNums),
    do_pick_producer(QueueNum, QueueNums, Workers).

do_pick_producer(QueueNum, QueueNums, Workers) ->
    Pid = lookup_producer(Workers, QueueNum),
    case is_pid(Pid) andalso is_process_alive(Pid) of
        true ->
            {QueueNum, Pid};
        false ->
            R = {QueueNum, Pid} = pick_next_alive(Workers, QueueNum, QueueNums),
            _ = put(rocketmq_roundrobin, (QueueNum + 1) rem QueueNums),
            R
    end.

pick_next_alive(Workers, QueueNum, QueueNums) ->
    pick_next_alive(Workers, (QueueNum + 1) rem QueueNums, QueueNums, _Tried = 1).

pick_next_alive(_Workers, _QueueNum, QueueNums, QueueNums) ->
    erlang:error(all_producers_down);
pick_next_alive(Workers, QueueNum, QueueNums, Tried) ->
    Pid = lookup_producer(Workers, QueueNum),
    case is_alive(Pid) of
        true -> {QueueNum, Pid};
        false -> pick_next_alive(Workers, (QueueNum + 1) rem QueueNums, QueueNums, Tried + 1)
    end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

lookup_producer(#{workers := Workers}, QueueNum) ->
    lookup_producer(Workers, QueueNum);
lookup_producer(Workers, QueueNum) when is_map(Workers) ->
    maps:get(QueueNum, Workers);
lookup_producer(Workers, QueueNum) ->
    [{QueueNum, Pid}] = ets:lookup(Workers, QueueNum),
    Pid.

pick_queue_num(QueueNums) ->
    QueueNum = case get(rocketmq_roundrobin) of
        undefined -> 0;
        Number    -> Number
    end,
    _ = put(rocketmq_roundrobin, (QueueNum + 1) rem QueueNums),
    QueueNum.

start_link(ClientId, ProducerGroup, Topic, ProducerOpts) ->
    gen_server:start_link({local, get_name(Topic)}, ?MODULE, [ClientId, ProducerGroup, Topic, ProducerOpts], []).

init([ClientId, ProducerGroup, Topic, ProducerOpts]) ->
    erlang:process_flag(trap_exit, true),
    {ok, #state{topic = Topic,
                client_id = ClientId,
                producer_opts = ProducerOpts,
                producer_group = ProducerGroup,
                workers = ets:new(get_name(Topic), [protected, named_table])}, 0}.

handle_call(get_workers, _From, State = #state{workers = Workers, queue_nums = QueueNum}) ->
    {reply, {QueueNum, Workers}, State};

handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(timeout, State = #state{client_id = ClientId, topic = Topic}) ->
    case rocketmq_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            Result = rocketmq_client:get_routeinfo_by_topic(Pid, Topic),
            {QueueNums, NewProducers} = maybe_start_producer(Pid, Result, State),
            {noreply, State#state{queue_nums = QueueNums, producers = NewProducers}};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info({'EXIT', Pid, _Error}, State = #state{workers = Workers, producers = Producers}) ->
    case maps:get(Pid, Producers, undefined) of
        undefined ->
            log_error("Not find Pid:~p producer", [Pid]),
            {noreply, State};
        {BrokerAddrs, QueueNum} ->
            ets:delete(Workers, QueueNum),
            self() ! {start_producer, BrokerAddrs, QueueNum},
            {noreply, State#state{producers = maps:remove(Pid, Producers)}}
    end;

handle_info({start_producer, BrokerAddrs, QueueSeq}, State = #state{producers = Producers}) ->
    NewProducers = start_producer(BrokerAddrs, QueueSeq, Producers, State),
    {noreply, State#state{producers = NewProducers}};

handle_info(_Info, State) ->
    log_error("Receive unknown message:~p~n", [_Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, _St) -> ok.

get_name(Topic) ->
    list_to_atom(lists:concat(["rocketmq_producers_", binary_to_list(Topic)])).

log_error(Fmt, Args) ->
    error_logger:error_msg(Fmt, Args).

maybe_start_producer(Pid, {Header, undefined}, State = #state{topic = Topic}) ->
    log_error("Start topic:~p producer fail:~p", [Topic, maps:get(<<"remark">>, Header, undefined)]),
    Result = rocketmq_client:get_routeinfo_by_topic(Pid, <<"TBW102">>),
    maybe_start_producer(Pid, Result, State);

maybe_start_producer(_, {_, Payload}, State = #state{topic = Topic,producers = Producers}) ->
    BrokerDatas = maps:get(<<"brokerDatas">>, Payload, []),
    QueueDatas = maps:get(<<"queueDatas">>, Payload, []),
    lists:foldl(fun(BrokerData, {QueueNumAcc, ProducersAcc}) ->
        BrokerAddrs = maps:get(<<"brokerAddrs">>, BrokerData),
        BrokerName = maps:get(<<"brokerName">>, BrokerData),
        QueueData = find_queue_data(BrokerName, QueueDatas),
        case maps:get(<<"perm">>, QueueData) =:= 4 of
            true ->
                log_error("Start producer fail tioic:~p permission denied:~p", [Topic]),
                {QueueNumAcc, ProducersAcc};
            false ->
                QueueNum = maps:get(<<"writeQueueNums">>, QueueData),
                QueueNumAcc1 = QueueNumAcc + QueueNum,
                ProducersAcc1 = lists:foldl(fun(QueueSeq, Acc) ->
                    start_producer(BrokerAddrs, QueueSeq, Acc, State)
                end, ProducersAcc, lists:seq(QueueNumAcc, QueueNumAcc1)),
                {QueueNumAcc1, ProducersAcc1}
        end
    end, {0, Producers}, BrokerDatas).

find_queue_data(_Key, []) ->
    [];
find_queue_data(Key, [QueueData | QueueDatas]) ->
    BrokerName = maps:get(<<"brokerName">>, QueueData),
    case BrokerName =:= Key of
        true -> QueueData;
        false -> find_queue_data(Key, QueueDatas)
    end.

start_producer(BrokerAddrs, QueueSeq, Producers, #state{workers = Workers,
                                                        topic = Topic,
                                                        producer_group = ProducerGroup,
                                                        producer_opts = ProducerOpts}) ->
    Server = maps:get(<<"0">>, BrokerAddrs),
    {ok, Producer} = rocketmq_producer:start_link(QueueSeq, Topic, Server, ProducerGroup, ProducerOpts),
    ets:insert(Workers, {QueueSeq, Producer}),
    maps:put(Producer, {BrokerAddrs, QueueSeq}, Producers).