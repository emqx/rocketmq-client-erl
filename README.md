# rocketmq-client-erl
A Erlang client library for Apache RocketMQ

## Example Code

### Async Produce

```
application:ensure_all_started(rocketmq),
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"TopicTest">>, #{}),
ok = rocketmq:send(Producers, <<"hello">>),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

### Async Batch Produce

```
application:ensure_all_started(rocketmq),
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"TopicTest">>, #{batch_size => 100}),
[begin rocketmq:send(Producers, <<"turtle:", (integer_to_binary(Seq))/binary >>) end||Seq<- lists:seq(1,100)],
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```
### Sync Produce

```
application:ensure_all_started(rocketmq),
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"TopicTest">>, #{}),
ok = rocketmq:send_sync(Producers, <<"hello">>, 5000),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

### Sync Batch Produce

```
application:ensure_all_started(rocketmq),
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"TopicTest">>, #{}),
ok = rocketmq:batch_send_sync(Producers, [<<"hello">>, <<"world">>], 5000),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

### Supervised Producers

```
application:ensure_all_started(rocketmq).
Client = 'client1',
Opts = #{},
{ok, _ClientPid} = rocketmq:ensure_supervised_client(Client, [{"127.0.0.1", 9876}], Opts),
Callback = fun(Code, Topic) ->
            io:format("message produced  receipt:~p~n",[{Code, Topic}]),
            ok
         end,
ProducerOpts = #{callback => Callback, tcp_opts => [], batch_size => 20},
{ok, Producers} = rocketmq:ensure_supervised_producers(Client, <<"client1_turtle">>, <<"TopicTest">>, ProducerOpts),
[begin rocketmq:send(Producers, <<"turtle:", (integer_to_binary(Seq))/binary >>) end||Seq<- lists:seq(1,100)].
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

### SSL/TLS Connection

See `test:start_tls()` example in `test/test.erl`.

## License

Apache License Version 2.0

## Author

EMQX Team.
