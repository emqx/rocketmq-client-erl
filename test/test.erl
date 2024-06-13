-module(test).

-export([start/0, start_tls/0, callback/3]).

-include_lib("eunit/include/eunit.hrl").

run_test() ->
    ok = start().

start() ->
    application:ensure_all_started(rocketmq),
    Clientid = 'test-client',
    Servers = [{"127.0.0.1", 9876}],
    AclInfo = #{access_key => <<"RocketMQ">>, secret_key => <<"12345678">>},
    ClientCfg = #{acl_info => AclInfo},
    {ok, Pid} = rocketmq:ensure_supervised_client(Clientid, Servers, ClientCfg),
    io:format("start client ~p~n", [Pid]),
    Topic = <<"test-topic">>,
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
    SSendRes = ok = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send async ~p~n", [SSendRes]),
    SSendRes2 = ok = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send2 async ~p~n", [SSendRes2]),

    ok = rocketmq:batch_send_sync(Producers, [<<"hello">>, <<"form">>, <<"sdk">>], 10000),
    ok.

%% RocketMQ listen to both TLS traffic and unencrypted traffic on the same port
%% by default (https://github.com/apache/rocketmq/wiki/How-to-Configure-TLS) so
%% we can use the same docker-compose file for testing both unencrypted and TLS
%% connections. Please notice that we need to configure ssl both for the client
%% and the producer if both the name sever and the brokers use SSL.
start_tls() ->
    application:ensure_all_started(rocketmq),
    Clientid = 'test-client',
    Servers = [{"127.0.0.1", 9876}],
    AclInfo = #{access_key => <<"RocketMQ">>, secret_key => <<"12345678">>},
    ClientCfg = #{acl_info => AclInfo, ssl => [{verify, verify_none}]},
    {ok, Pid} = rocketmq:ensure_supervised_client(Clientid, Servers, ClientCfg),
    io:format("start client ~p~n", [Pid]),
    Topic = <<"test-topic">>,
    ProducerGroup = list_to_binary(lists:concat([Clientid, "_", binary_to_list(Topic)])),

    ProducerOpts =
        #{batch_size => 100,
          callback => {?MODULE, callback, []},
          name => producer_t1,
          ref_topic_route_interval => 3000,
          tcp_opts => [{sndbuf, 1048576}],
          ssl => [{verify, verify_none}],
          acl_info => AclInfo},
    {ok, Producers} =
        rocketmq:ensure_supervised_producers(Clientid, ProducerGroup, Topic, ProducerOpts),
    io:format("start producer ~p~n", [Producers]),
    % SSendRes = rocketmq:send_sync(Producers, <<"hello, form sdk">>, 1000),
    SSendRes = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send async ~p~n", [SSendRes]),
    SSendRes2 = rocketmq:send(Producers, <<"hello, form sdk">>),
    io:format("send2 async ~p~n", [SSendRes2]),

    ok = rocketmq:batch_send_sync(Producers, [<<"hello">>, <<"form">>, <<"sdk">>], 10000),
    ok.

callback(Code, Topic, Size) ->
    ok = Code,
    <<"test-topic">> = Topic,
    1 = Size,
    io:format("callback Code: ~p Topic: ~p Size: ~p~n", [Code, Topic, Size]).
