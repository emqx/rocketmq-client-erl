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

-include("rocketmq.hrl").

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

-export([ pick_producer/1
        , pick_producer/2
        ]).

-ifdef(TEST).
-export([ get_delta_broker_datas/2
        ]).
-endif.

-record(state, {topic,
                client_id,
                workers,
                queue_nums,
                producer_opts,
                producers = #{},
                producer_group,
                broker_datas,
                ref_topic_route_interval = 5000}).

-define(RESTART_INTERVAL, 3000).

-type index() :: non_neg_integer().
-type queue_count() :: pos_integer().
-type clientid() :: atom().
-type topic() :: binary().
-type partitioner() :: roundrobin | key_dispatch.
-type producer_group() :: binary().
-type producers() :: #{ client := clientid()
                      , topic := topic()
                      , workers := ets:table()
                      , partitioner := partitioner()
                      }.
-type producer_opts() :: map().
-type produce_context() :: #{ key => term()
                            , any() => term()
                            }.

-export_type([ producers/0
             , produce_context/0
             ]).

-define(PRODUCER_INFO(INDEX, BORKER_NAME, BROKER_ADDR, QUEUE_SEQ_NUM), #{tab_index => INDEX, broker_name => BORKER_NAME, broker_addr => BROKER_ADDR, queue_seq_num => QUEUE_SEQ_NUM}).
-define(M_PRODUCER_INFO(INDEX, BORKER_NAME, BROKER_ADDR, QUEUE_SEQ_NUM), #{tab_index := INDEX, broker_name := BORKER_NAME, broker_addr := BROKER_ADDR, queue_seq_num := QUEUE_SEQ_NUM}).

-spec start_supervised(clientid(), producer_group(), topic(), producer_opts()) -> {ok, producers()}.
start_supervised(ClientId, ProducerGroup, Topic, ProducerOpts) ->
  {ok, Pid} = rocketmq_producers_sup:ensure_present(ClientId, ProducerGroup, Topic, ProducerOpts),
  WorkersTab = gen_server:call(Pid, get_workers, infinity),
  {ok, #{client => ClientId,
         topic => Topic,
         workers => WorkersTab,
         partitioner => maps:get(partitioner, ProducerOpts, roundrobin)
        }}.

stop_supervised(#{client := ClientId, workers := WorkersTab}) ->
  rocketmq_producers_sup:ensure_absence(ClientId, WorkersTab).

-spec pick_producer(producers()) -> {index(), pid()}.
pick_producer(Producers) ->
    Context = #{},
    pick_producer(Producers, Context).

-spec pick_producer(producers(), produce_context()) -> {index(), pid()}.
pick_producer(Producers = #{workers := WorkersTab, topic := Topic},
              Context = #{}) ->
    Partitioner = maps:get(partitioner, Producers, roundrobin),
    QueueCount = producer_queue_count(WorkersTab),
    Index = pick_index(Partitioner, QueueCount, Topic, Context),
    do_pick_producer(Index, QueueCount, WorkersTab).

do_pick_producer(Index, QueueCount, WorkersTab) ->
    Pid0 = lookup_producer(WorkersTab, Index),
    case is_pid(Pid0) andalso is_process_alive(Pid0) of
        true -> {Index, Pid0};
        false ->
            R = {Index1, _Pid1} = pick_next_alive(WorkersTab, Index, QueueCount),
            _ = put(rocketmq_roundrobin, (Index1 + 1) rem QueueCount),
            R
    end.

pick_next_alive(WorkersTab, Index, QueueCount) ->
    pick_next_alive(WorkersTab, (Index + 1) rem QueueCount, QueueCount, _Tried = 1).

pick_next_alive(_Workers, _Index, QueueCount, QueueCount) ->
    erlang:error(all_producers_down);
pick_next_alive(WorkersTab, Index, QueueCount, Tried) ->
    case lookup_producer(WorkersTab, Index) of
        {error, _} ->
            pick_next_alive(WorkersTab, (Index + 1) rem QueueCount, QueueCount, Tried + 1);
        Pid ->
            case is_alive(Pid) of
                true -> {Index, Pid};
                false -> pick_next_alive(WorkersTab, (Index + 1) rem QueueCount, QueueCount, Tried + 1)
            end
    end.

is_alive(Pid) -> is_pid(Pid) andalso is_process_alive(Pid).

producer_queue_count(WorkersTab) ->
    case ets:info(WorkersTab, size) of
        undefined -> throw({ets_table_not_found, WorkersTab});
        Count -> Count
    end.

lookup_producer(WorkersTab, Index) ->
    case ets:lookup(WorkersTab, Index) of
        [] -> {error, get_worker_fail};
        [{_TabIndex, _BrokerName, _QueueSeqNum, Pid}] -> Pid
    end.

-spec pick_index(partitioner(), queue_count(), topic(), produce_context()) -> index().
pick_index(roundrobin, QueueCount, _Topic, _Context) ->
    Index = case get(rocketmq_roundrobin) of
        undefined -> 0;
        Number -> Number
    end,
    _ = put(rocketmq_roundrobin, (Index + 1) rem QueueCount),
    Index;
pick_index(key_dispatch, QueueCount, _Topic, _Context = #{key := Key}) ->
    erlang:phash2(Key, QueueCount).

start_link(ClientId, ProducerGroup, Topic, ProducerOpts) ->
    gen_server:start_link({local, get_name(ProducerOpts)}, ?MODULE, [ClientId, ProducerGroup, Topic, ProducerOpts], []).

init([ClientId, ProducerGroup, Topic, ProducerOpts]) ->
    logger:debug("start producer manager for topic: ~p, clientid: ~p, producer_group: ~p",
        [Topic, ClientId, ProducerGroup]),
    erlang:process_flag(trap_exit, true),
    RefTopicRouteInterval = maps:get(ref_topic_route_interval, ProducerOpts, 5000),
    erlang:send_after(RefTopicRouteInterval, self(), ref_topic_route),
    State = #state{
        topic = Topic,
        client_id = ClientId,
        producer_opts = ProducerOpts,
        producer_group = ProducerGroup,
        ref_topic_route_interval = RefTopicRouteInterval,
        workers = ensure_ets_created(get_name(ProducerOpts))
    },
    case init_producers(ClientId, State) of
        {ok, State1} -> {ok, State1};
        {error, Reason} -> {stop, {shutdown, Reason}}
    end.

init_producers(ClientId, State) ->
    case rocketmq_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            case maybe_start_producer(Pid, State) of
                {ok, {QueueNums, NewProducers, BrokerDatas}} ->
                    {ok, State#state{
                            queue_nums = QueueNums,
                            producers = NewProducers,
                            broker_datas = BrokerDatas}};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

handle_call(get_workers, _From, State = #state{workers = WorkersTab}) ->
    {reply, WorkersTab, State};

handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Error}, State = #state{workers = WorkersTab, producers = Producers,
                                                 broker_datas = BrokerDatas, producer_opts = ProducerOpts}) ->
    case maps:get(Pid, Producers, undefined) of
        undefined ->
            logger:error("Got EXIT from unknown pid: ~p, error: ~p", [Pid, Error]),
            {noreply, State};
        ?M_PRODUCER_INFO(Index, _, BrokerAddrs, QueueSeqNum) = ProducerInfo->
            case first_broker_addr_exists(maps:get(<<"0">>, BrokerAddrs, not_found), BrokerDatas) of
                true ->
                    ets:delete(WorkersTab, Index),
                    RestartAfter = maps:get(producer_restart_interval, ProducerOpts, ?RESTART_INTERVAL),
                    erlang:send_after(RestartAfter, self(), {start_producer, ProducerInfo}),
                    logger:warning("Producer closed with error: ~p, try to restart a new producer. broker_addrs: ~p, queue_num: ~p",
                        [Error, BrokerAddrs, QueueSeqNum]),
                    {noreply, State#state{producers = maps:remove(Pid, Producers)}};
                false ->
                    logger:warning("Producer closed with error: ~p, don't restart it as it has been removed from the broker_datas. broker_addrs: ~p",
                        [Error, BrokerAddrs]),
                    {noreply, State}
            end
    end;

handle_info({start_producer, ?M_PRODUCER_INFO(TabIndex, BrokerName, BrokerAddrs, QueueSeqNum)}, State = #state{producers = Producers}) ->
    NewProducers = do_start_producer(TabIndex, BrokerName, BrokerAddrs, QueueSeqNum, Producers, State),
    {noreply, State#state{producers = NewProducers}};

handle_info(ref_topic_route, State = #state{client_id = ClientId,
                                            topic = Topic,
                                            queue_nums = QueueNums,
                                            broker_datas = BrokerDatas,
                                            producers = Producers,
                                            ref_topic_route_interval = RefTopicRouteInterval}) ->
    case rocketmq_client_sup:find_client(ClientId) of
        {ok, Pid} ->
            erlang:send_after(RefTopicRouteInterval, self(), ref_topic_route),
            case rocketmq_client:get_routeinfo_by_topic(Pid, Topic) of
                {ok, {_, undefined}} ->
                    {noreply, State};
                {ok, {_, RouteInfo}} ->
                    BrokerDatas1 = maps:get(<<"brokerDatas">>, RouteInfo, []),
                    case get_delta_broker_datas(BrokerDatas, BrokerDatas1) of
                        [] -> {noreply, State};
                        DeltaBrokerData ->
                            QueueDatas = maps:get(<<"queueDatas">>, RouteInfo, []),
                            logger:warning("Start new producers for new topic route, delta broker_data: ~p, queue_nums: ~p",
                                [DeltaBrokerData, QueueNums]),
                            {NewQueueNums, NewProducers} = start_producer(QueueNums, DeltaBrokerData, QueueDatas, Producers, State),
                            {noreply, State#state{queue_nums = NewQueueNums,
                                                producers = NewProducers,
                                                broker_datas = BrokerDatas1}}
                    end;
                {error, Reason} ->
                    logger:error("Get routeinfo by topic failed: ~p", [Reason]),
                    {noreply, State}
                end;
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info(_Info, State) ->
    logger:error("Receive unknown message:~p~n", [_Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, _St) -> ok.

get_name(ProducerOpts) -> maps:get(name, ProducerOpts, ?MODULE).

maybe_start_producer(Pid, State = #state{topic = Topic, producers = Producers}) ->
    case rocketmq_client:get_routeinfo_by_topic(Pid, Topic) of
        {ok, {_Header, undefined}} ->
            %% Try again using the default topic, as the 'Topic' does not exists for now.
            %% Note that the topic will be created by the rocketmq server automatically
            %% at first time we send message to it, if the user has configured
            %% `autoCreateTopicEnable = true` in the rocketmq server side.
            maybe_start_producer_using_default_topic(Pid, State);
        {ok, {_, RouteInfo}} ->
            start_producer_with_route_info(RouteInfo, Producers, State);
        {error, Reason} ->
            logger:error("Get routeinfo by topic failed: ~p, topic: ~p", [Reason, Topic]),
            {error, {get_routeinfo_by_topic_failed, Reason}}
    end.

maybe_start_producer_using_default_topic(Pid, State = #state{producers = Producers}) ->
    case rocketmq_client:get_routeinfo_by_topic(Pid, ?DEFAULT_TOPIC) of
        {ok, {Header, undefined}} ->
            logger:error("Start producer failed, remark: ~p",
                [maps:get(<<"remark">>, Header, undefined)]),
            {error, {start_producer_failed, Header}};
        {ok, {_, RouteInfo}} ->
            start_producer_with_route_info(RouteInfo, Producers, State);
        {error, Reason} ->
            logger:error("Get routeinfo by topic failed: ~p, topic: ~p", [Reason, ?DEFAULT_TOPIC]),
            {error, {get_routeinfo_by_topic_failed, Reason}}
    end.

find_queue_data(_Key, []) ->
    [];
find_queue_data(Key, [QueueData | QueueDatas]) ->
    BrokerName = maps:get(<<"brokerName">>, QueueData),
    case BrokerName =:= Key of
        true -> QueueData;
        false -> find_queue_data(Key, QueueDatas)
    end.

start_producer_with_route_info(RouteInfo, Producers, State) ->
    BrokerDatas = maps:get(<<"brokerDatas">>, RouteInfo, []),
    QueueDatas = maps:get(<<"queueDatas">>, RouteInfo, []),
    {QueueNums, NewProducers} = start_producer(0, BrokerDatas, QueueDatas, Producers, State),
    {ok, {QueueNums, NewProducers, BrokerDatas}}.

start_producer(Start, BrokerDatas,  QueueDatas, Producers, State = #state{topic = Topic}) ->
    lists:foldl(fun(BrokerData, {QueueNumAcc, ProducersAcc}) ->
        BrokerAddrs = maps:get(<<"brokerAddrs">>, BrokerData),
        BrokerName = maps:get(<<"brokerName">>, BrokerData),
        QueueData = find_queue_data(BrokerName, QueueDatas),
        case maps:get(<<"perm">>, QueueData) =:= 4 of
            true ->
                logger:error("Start producer fail; topic: ~p; permission denied", [Topic]),
                {QueueNumAcc, ProducersAcc};
            false ->
                QueueCount = maps:get(<<"writeQueueNums">>, QueueData),
                ProducersAcc1 =
                    lists:foldl(fun(QueueSeqNum, Acc) ->
                            do_start_producer(QueueNumAcc + QueueSeqNum, BrokerName, BrokerAddrs, QueueSeqNum, Acc, State)
                        end, ProducersAcc, lists:seq(0, QueueCount - 1)),
                {QueueNumAcc + QueueCount, ProducersAcc1}
        end
    end, {Start, Producers}, BrokerDatas).

do_start_producer(TabIndex, BrokerName, BrokerAddrs, QueueSeqNum, Producers,
        #state{workers = WorkersTab, topic = Topic,
               producer_group = ProducerGroup,
               producer_opts = ProducerOpts}) ->
    Server = maps:get(<<"0">>, BrokerAddrs),
    logger:notice("Start producer for topic: ~p, broker_addr: ~p", [Topic, Server]),
    {ok, Pid} = rocketmq_producer:start_link(QueueSeqNum, Topic, Server, ProducerGroup, ProducerOpts),
    ets:insert(WorkersTab, {TabIndex, BrokerName, QueueSeqNum, Pid}),
    maps:put(Pid, ?PRODUCER_INFO(TabIndex, BrokerName, BrokerAddrs, QueueSeqNum), Producers).

ensure_ets_created(TabName) ->
    try ets:new(TabName, [protected, named_table, {read_concurrency, true},
                          {decentralized_counters, false}])
    catch
        error:badarg -> TabName %% already exists
    end.

get_delta_broker_datas(OldDatas, NewDatas) ->
    lists:foldl(fun
            (#{<<"brokerAddrs">> := #{<<"0">> := FirstAddr}} = NewD, Acc) ->
                Acc ++ [NewD || not first_broker_addr_exists(FirstAddr, OldDatas)];
            (_, Acc) ->
                Acc
        end, [], NewDatas).

first_broker_addr_exists(Addr, Datas) ->
    Res = lists:search(fun
            (#{<<"brokerAddrs">> := #{<<"0">> := FirstAddr}}) ->
                FirstAddr =:= Addr;
            (_) ->
                false
        end, Datas),
    Res =/= false.
