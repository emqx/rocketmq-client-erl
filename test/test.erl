-module(test).

-export([start/0, callback/3]).

start() ->
    %% test:start().
    application:ensure_all_started(rocketmq),
    Clientid = 'test-client',
    Servers = [{"127.0.0.1", 9876}],
    AclInfo = #{access_key => <<"RocketMQ">>, secret_key => <<"12345678">>},
    ClientCfg = #{acl_info => AclInfo},
    {ok, Pid} = rocketmq:ensure_supervised_client(Clientid, Servers, ClientCfg),
    io:format("start client ~p~n", [Pid]),
    Topic = <<"topicB">>,
    ProducerGroup = list_to_binary(lists:concat([Clientid, "_", binary_to_list(Topic)])),

    ProducerOpts =
        #{batch_size => 100,
          callback => {?MODULE, callback, []},
          name => producer_t1,
          ref_topic_route_interval => 3000,
          tcp_opts => [{sndbuf, 1048576}],
          acl_info => AclInfo},
    {ok, Producers} =
        rocketmq:ensure_supervised_producers(Clientid, ProducerGroup, Topic, ProducerOpts),
    io:format("start producer ~p~n", [Producers]),
    % SSendRes = rocketmq:send_sync(Producers, <<"hello, form sdk">>, 1000),
    SSendRes = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send async ~p~n", [SSendRes]),
    SSendRes2 = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send2 async ~p~n", [SSendRes2]),
    ok.

callback(Code, Topic, Size) ->
    io:format("callback Code: ~p Topic: ~p Size: ~p~n", [Code, Topic, Size]).
