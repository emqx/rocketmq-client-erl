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
       true = ets:insert(Workers, {N, Pid}),
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
