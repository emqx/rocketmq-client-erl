# rocketmq-client-erl
A Erlang client library for Apache RocketMQ

## Example Code

### Async Produce

```
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"turtle">>, #{}),
ok = rocketmq:send(Producers, <<"hello">>),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```
### Sync Produce

```
{ok, Pid} = rocketmq:ensure_supervised_client('client1', [{"127.0.0.1", 9876}], #{}),
{ok, Producers} = rocketmq:ensure_supervised_producers('client1', <<"client1_turtle">>, <<"turtle">>, #{}),
ok = rocketmq:send_sync(Producers, <<"hello">>, 5000),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

### Supervised Producers

```
application:ensure_all_started(rocketmq).
Client = 'client1',
Opts = #{},
{ok, _ClientPid} = rocketmq:ensure_supervised_client(Client, [{"127.0.0.1", 9876}], Opts),
Callback = fun(SendReceipt) ->
            io:format("message produced  receipt:~p~n",[SendReceipt]),
            ok
         end,
ProducerOpts = #{callback => Callback, tcp_opts => []},
{ok, Producers} = rocketmq:ensure_supervised_producers(Client, <<"client1_turtle">>, <<"turtle">>, ProducerOpts),
ok = rocketmq:stop_and_delete_supervised_producers(Producers),
ok = rocketmq:stop_and_delete_supervised_client('client1').
```

## License

Apache License Version 2.0

## Author

EMQ X Team.