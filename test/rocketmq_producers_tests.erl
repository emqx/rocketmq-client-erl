%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(rocketmq_producers_tests).

-include_lib("eunit/include/eunit.hrl").

producers() ->
    QueueCount = 5,
    Partitioner = roundrobin,
    Workers = ets:new(test_producers, [public]),
    #{ client => clientid
     , topic => <<"topic">>
     , workers => Workers
     , queue_nums => QueueCount
     , partitioner => Partitioner
     }.

setup_topic_table() ->
    catch ets:new(rocketmq_topic, [public, named_table]).

spawn_producers(#{workers := Workers, queue_nums := QueueCount}) ->
    lists:map(
     fun(N) ->
       Pid = spawn_link(fun() -> receive die -> ok end end),
       true = ets:insert(Workers, {N, <<"broker-name-1">>, N, Pid}),
       Pid
     end,
     lists:seq(0, QueueCount - 1)).

add_topic(#{queue_nums := QueueCount, topic := Topic}) ->
    true = ets:insert(rocketmq_topic, {Topic, QueueCount}).

kill_producer(Pid) ->
    Ref = monitor(process, Pid),
    Pid ! die,
    receive
        {'DOWN', Ref, process, Pid, _Reason} ->
            ok
    end.

pick_producer_roundrobin_dead_producer_test() ->
    setup_topic_table(),
    Producers = producers(),
    add_topic(Producers),
    [Pid0, Pid1, Pid2, Pid3, Pid4] = spawn_producers(Producers),
    kill_producer(Pid0),
    %% it should be 0 and Pid0, if it was alive; queue number is
    %% incremented twice after picking it.
    ?assertEqual({1, Pid1}, rocketmq_producers:pick_producer(Producers)),
    %% next is queue number 2 and Pid2, the original assigned worker
    ?assertEqual({2, Pid2}, rocketmq_producers:pick_producer(Producers)),
    %% next would queue number 3 and Pid3, but it's dead; and the
    %% queue number gets wrapped while looking for an alive worker.
    kill_producer(Pid3),
    kill_producer(Pid4),
    %% back to the first worker
    ?assertEqual({1, Pid1}, rocketmq_producers:pick_producer(Producers)),

    kill_producer(Pid1),
    kill_producer(Pid2),
    ?assertError(all_producers_down, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),

    ok.

pick_producer_key_dispatch_dead_producer_test() ->
    setup_topic_table(),
    Producers0 = producers(),
    Producers = Producers0#{partitioner := key_dispatch},
    add_topic(Producers),
    [Pid0, Pid1, Pid2, Pid3, Pid4] = spawn_producers(Producers),

    ?assertEqual({4, Pid4}, rocketmq_producers:pick_producer(Producers, #{key => <<"k0">>})),
    ?assertEqual({4, Pid4}, rocketmq_producers:pick_producer(Producers, #{key => <<"k0">>})),

    ?assertEqual({0, Pid0}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),
    ?assertEqual({0, Pid0}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),

    kill_producer(Pid0),
    ?assertEqual({1, Pid1}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),
    ?assertEqual({1, Pid1}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),

    kill_producer(Pid1),
    ?assertEqual({2, Pid2}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),
    ?assertEqual({2, Pid2}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),

    kill_producer(Pid2),
    ?assertEqual({3, Pid3}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),
    ?assertEqual({3, Pid3}, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),
    ?assertEqual({4, Pid4}, rocketmq_producers:pick_producer(Producers, #{key => <<"k4">>})),

    kill_producer(Pid3),
    kill_producer(Pid4),
    ?assertError(all_producers_down, rocketmq_producers:pick_producer(Producers, #{key => <<"k2">>})),

    ok.

diff_broker_datas_1_test() ->
    DataA = [#{<<"brokerAddrs">> =>
                    #{<<"0">> => <<"10.188.96.8:30911">>,
                    <<"2">> => <<"10.188.96.7:30911">>,
                    <<"3">> => <<"10.188.96.9:30911">>},
                <<"brokerName">> => <<"nc4srv-car-rocketmq-raft0">>,
                <<"cluster">> => <<"nc4srv-car-rocketmq">>,
                <<"random">> =>
                    #{<<"haveNextNextGaussian">> => false,
                    <<"nextNextGaussian">> => 0.0,
                    <<"seed">> => 206625674873441}},
            #{<<"brokerAddrs">> =>
                    #{<<"0">> => <<"10.188.96.7:30921">>,
                    <<"1">> => <<"10.188.96.8:30921">>,
                    <<"3">> => <<"10.188.96.9:30921">>},
                <<"brokerName">> => <<"nc4srv-car-rocketmq-raft1">>,
                <<"cluster">> => <<"nc4srv-car-rocketmq">>,
                <<"random">> =>
                    #{<<"haveNextNextGaussian">> => false,
                    <<"nextNextGaussian">> => 0.0,
                    <<"seed">> => 62307961321294}}],
    DataB = [#{<<"brokerAddrs">> =>
                    #{<<"0">> => <<"10.188.96.8:30911">>,
                    <<"2">> => <<"10.188.96.7:30911">>,
                    <<"3">> => <<"10.188.96.9:30911">>},
                <<"brokerName">> => <<"nc4srv-car-rocketmq-raft0">>,
                <<"cluster">> => <<"nc4srv-car-rocketmq">>,
                <<"random">> =>
                    #{<<"haveNextNextGaussian">> => false,
                    <<"nextNextGaussian">> => 0.0,<<"seed">> => 36556624759058}},
            #{<<"brokerAddrs">> =>
                    #{<<"0">> => <<"10.188.96.7:30921">>,
                    <<"1">> => <<"10.188.96.8:30921">>,
                    <<"3">> => <<"10.188.96.9:30921">>},
                <<"brokerName">> => <<"nc4srv-car-rocketmq-raft1">>,
                <<"cluster">> => <<"nc4srv-car-rocketmq">>,
                <<"random">> =>
                    #{<<"haveNextNextGaussian">> => false,
                    <<"nextNextGaussian">> => 0.0,
                    <<"seed">> => 143571006215535}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA, DataB)).

diff_broker_datas_2_test() ->
    DataA = [#{<<"brokerAddrs">> => #{<<"0">> => a0}},
             #{<<"brokerAddrs">> => #{<<"0">> => b0}}],
    DataB = [#{<<"brokerAddrs">> => #{<<"0">> => a0}},
             #{<<"brokerAddrs">> => #{<<"0">> => b0}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA, DataB)),

    DataA0 = [#{<<"brokerAddrs">> => #{<<"0">> => b0}},
              #{<<"brokerAddrs">> => #{<<"0">> => a0}}],
    DataB0 = [#{<<"brokerAddrs">> => #{<<"0">> => a0}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA0, DataB0)),

    DataA1 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB1 = [#{<<"brokerAddrs">> => #{<<"0">> => a0}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA1, DataB1)),

    DataA2 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB2 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => aa1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => bb1}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA2, DataB2)),

    DataA3 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB3 = [#{<<"brokerAddrs">> => #{}},
              #{<<"brokerAddrs">> => #{}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA3, DataB3)),

    DataA4 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB4 = [#{<<"brokerAddrs">> => #{<<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"1">> => b1}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA4, DataB4)),

    DataA5 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB5 = [#{<<"brokerAddrs">> => #{<<"0">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b1}}],
    ?assertEqual(
        lists:sort([#{<<"brokerAddrs">> => #{<<"0">> => a1}},
                    #{<<"brokerAddrs">> => #{<<"0">> => b1}}])
        , lists:sort(rocketmq_producers:get_delta_broker_datas(DataA5, DataB5))),

    DataA6 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB6 = [#{<<"brokerAddrs">> => #{<<"0">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => bb1}}],
    ?assertEqual([#{<<"brokerAddrs">> => #{<<"0">> => a1}}],
        rocketmq_producers:get_delta_broker_datas(DataA6, DataB6)),

    DataA7 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    DataB7 = [#{<<"foo">> => #{<<"0">> => a1}},
              #{<<"foo">> => #{<<"0">> => b0, <<"1">> => bb1}}],
    ?assertEqual([], rocketmq_producers:get_delta_broker_datas(DataA7, DataB7)),

    DataA8 = [#{<<"foo">> => #{<<"0">> => a1}},
              #{<<"foo">> => #{<<"0">> => b0, <<"1">> => bb1}}],
    DataB8 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    ?assertEqual(DataB8, rocketmq_producers:get_delta_broker_datas(DataA8, DataB8)),

    DataA9 = [],
    DataB9 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
              #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    ?assertEqual(DataB9, rocketmq_producers:get_delta_broker_datas(DataA9, DataB9)),

    DataA10 = [#{<<"foo">> => #{<<"0">> => a1}},
               #{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}}],
    DataB10 = [#{<<"brokerAddrs">> => #{<<"0">> => a0, <<"1">> => a1}},
               #{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
    ?assertEqual([#{<<"brokerAddrs">> => #{<<"0">> => b0, <<"1">> => b1}}],
        rocketmq_producers:get_delta_broker_datas(DataA10, DataB10)),
    ok.