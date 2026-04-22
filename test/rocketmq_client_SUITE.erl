%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Unit-level tests for rocketmq_client state machine. Uses a tiny
%% gen_tcp listener helper instead of a real RocketMQ broker so the
%% tests can deterministically exercise connect / drop / reconnect
%% transitions.
%%--------------------------------------------------------------------

-module(rocketmq_client_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([t_connect_refused_reports_disconnected_with_reason/1,
         t_connect_success_reports_connected/1,
         t_socket_drop_flips_to_connecting_with_reason/1,
         t_reconnect_driven_by_get_connection_state_poll/1,
         t_reconnecting_flag_dedups_polls/1,
         t_unreachable_then_reachable_recovers/1,
         t_get_status_unchanged_for_backwards_compat/1]).

all() ->
    [t_connect_refused_reports_disconnected_with_reason,
     t_connect_success_reports_connected,
     t_socket_drop_flips_to_connecting_with_reason,
     t_reconnect_driven_by_get_connection_state_poll,
     t_reconnecting_flag_dedups_polls,
     t_unreachable_then_reachable_recovers,
     t_get_status_unchanged_for_backwards_compat].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) -> ok.

init_per_testcase(TC, Config) ->
    %% Each case gets a unique registered ClientId so parallel
    %% start_link calls don't collide via the local name registry.
    [{client_id, list_to_atom("rocketmq_client_test_" ++ atom_to_list(TC))} | Config].

end_per_testcase(_TC, Config) ->
    ClientId = ?config(client_id, Config),
    case whereis(ClientId) of
        undefined -> ok;
        Pid -> ok = stop(Pid)
    end,
    ok.

%%--------------------------------------------------------------------
%% Cases

t_connect_refused_reports_disconnected_with_reason(Config) ->
    %% Pick a free port and never bind to it -> immediate econnrefused.
    Port = free_port(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    %% handle_continue fires the first attempt synchronously before the
    %% gen_server starts accepting external calls; it fails immediately
    %% and bumps reconnect_attempts.
    ?assertMatch({disconnected, {tcp_connect_error, {_, _, econnrefused}}},
                 rocketmq_client:get_connection_state(Pid)),
    ok.

t_connect_success_reports_connected(Config) ->
    {ok, Listener, Port} = start_listener(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    ok = wait_for(fun() -> rocketmq_client:get_connection_state(Pid) =:= connected end, 2000),
    ?assertEqual(connected, rocketmq_client:get_connection_state(Pid)),
    stop_listener(Listener),
    ok.

t_socket_drop_flips_to_connecting_with_reason(Config) ->
    {ok, Listener, Port} = start_listener(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    ok = wait_for(fun() -> rocketmq_client:get_connection_state(Pid) =:= connected end, 2000),
    %% Close the listener and all its accepted sockets to simulate a drop.
    stop_listener(Listener),
    ok = wait_for(
           fun() ->
                   case rocketmq_client:get_connection_state(Pid) of
                       connecting -> true;
                       {disconnected, _} -> true;
                       _ -> false
                   end
           end, 2000),
    %% After the drop but before the next failed retry, the status is
    %% `connecting'. After the next retry fails (listener is gone) it
    %% flips to {disconnected, _}.
    ok = wait_for(
           fun() ->
                   case rocketmq_client:get_connection_state(Pid) of
                       {disconnected, _} -> true;
                       _ -> false
                   end
           end, 5000),
    {disconnected, Reason} = rocketmq_client:get_connection_state(Pid),
    %% Reason is either the original drop (tcp_closed) or the follow-up
    %% failed reconnect. Either way, a concrete tagged reason rather
    %% than `undefined'.
    ?assertMatch(R when R =/= undefined, Reason),
    ok.

t_reconnect_driven_by_get_connection_state_poll(Config) ->
    %% Start with no server, client stays disconnected.
    Port = free_port(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    ?assertMatch({disconnected, _}, rocketmq_client:get_connection_state(Pid)),
    %% Bring up a listener on that port, then poll a few times to
    %% confirm get_connection_state drives the reconnect itself (no
    %% producer traffic required).
    {ok, Listener} = listen_on(Port),
    ok = wait_for(fun() ->
                          _ = rocketmq_client:get_connection_state(Pid),
                          rocketmq_client:get_connection_state(Pid) =:= connected
                  end, 3000),
    ?assertEqual(connected, rocketmq_client:get_connection_state(Pid)),
    stop_listener(Listener),
    ok.

t_reconnecting_flag_dedups_polls(Config) ->
    %% With a dead port, reconnect attempts are slow (TCP connect
    %% timeout). Fire a burst of get_connection_state calls and assert
    %% only one `try_reconnect' message is in flight at any moment --
    %% the `reconnecting' flag must prevent mailbox buildup.
    Port = free_port(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    _ = rocketmq_client:get_connection_state(Pid),
    %% Fire 50 polls in quick succession. If dedup wasn't working we'd
    %% accumulate 50 try_reconnect messages in the mailbox.
    [rocketmq_client:get_connection_state(Pid) || _ <- lists:seq(1, 50)],
    {message_queue_len, QLen} = erlang:process_info(Pid, message_queue_len),
    %% Allow for at most one queued try_reconnect (the in-flight one is
    %% not counted in message_queue_len since it's mid-handler).
    ?assert(QLen =< 1, {too_many_pending, QLen}),
    ok.

t_unreachable_then_reachable_recovers(Config) ->
    %% End-to-end recovery: unreachable -> reachable -> connected.
    Port = free_port(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    ok = wait_for(
           fun() ->
                   case rocketmq_client:get_connection_state(Pid) of
                       {disconnected, _} -> true;
                       _ -> false
                   end
           end, 2000),
    {ok, Listener} = listen_on(Port),
    %% Polling drives recovery; within ~3s of bringing the listener up,
    %% the client should flip to connected.
    ok = wait_for(
           fun() ->
                   _ = rocketmq_client:get_connection_state(Pid),
                   rocketmq_client:get_connection_state(Pid) =:= connected
           end, 3000),
    stop_listener(Listener),
    ok.

t_get_status_unchanged_for_backwards_compat(Config) ->
    {ok, Listener, Port} = start_listener(),
    {ok, Pid} = start_client(?config(client_id, Config),
                             [{"127.0.0.1", Port}], #{}),
    ok = wait_for(fun() -> rocketmq_client:get_status(Pid) =:= true end, 2000),
    ?assertEqual(true, rocketmq_client:get_status(Pid)),
    stop_listener(Listener),
    %% After the server goes away and sock drops, get_status should
    %% attempt an inline reconnect and return false (preserving the
    %% pre-v0.7.0 return shape).
    ok = wait_for(fun() -> rocketmq_client:get_status(Pid) =:= false end, 5000),
    ok.

%%--------------------------------------------------------------------
%% helpers

start_client(ClientId, Servers, ExtraOpts) ->
    Opts = maps:merge(#{connect_timeout => 1000}, ExtraOpts),
    rocketmq_client:start_link(ClientId, Servers, Opts).

stop(Pid) when is_pid(Pid) ->
    %% Unlink first so the test process isn't taken down by the
    %% start_link relationship when we terminate the client.
    unlink(Pid),
    try gen_server:stop(Pid, shutdown, 2000) of
        ok -> ok
    catch
        exit:noproc -> ok;
        exit:{noproc, _} -> ok
    end.

free_port() ->
    {ok, L} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}]),
    {ok, Port} = inet:port(L),
    ok = gen_tcp:close(L),
    Port.

start_listener() ->
    Port = free_port(),
    {ok, L} = listen_on(Port),
    {ok, L, Port}.

%% Returns an opaque listener handle that tracks both the listen socket
%% and every accepted socket, so stop_listener/1 can close them all
%% and deterministically drop whatever the client connected through.
-record(listener, {acceptor, ctrl}).

listen_on(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, false}, {reuseaddr, true}]),
    Parent = self(),
    Ctrl = spawn_link(fun() -> listener_ctrl_loop(LSock, Parent, []) end),
    {ok, #listener{acceptor = LSock, ctrl = Ctrl}}.

listener_ctrl_loop(LSock, Parent, Accepted) ->
    receive
        stop ->
            catch gen_tcp:close(LSock),
            [catch gen_tcp:close(S) || S <- Accepted],
            ok
    after 0 ->
        case gen_tcp:accept(LSock, 100) of
            {ok, Sock} ->
                gen_tcp:controlling_process(Sock, self()),
                listener_ctrl_loop(LSock, Parent, [Sock | Accepted]);
            {error, timeout} ->
                listener_ctrl_loop(LSock, Parent, Accepted);
            {error, closed} ->
                ok;
            {error, _} ->
                ok
        end
    end.

stop_listener(#listener{ctrl = Ctrl}) ->
    Ref = erlang:monitor(process, Ctrl),
    Ctrl ! stop,
    receive
        {'DOWN', Ref, process, Ctrl, _} -> ok
    after 2000 ->
        erlang:demonitor(Ref, [flush]),
        ok
    end.

wait_for(Pred, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_loop(Pred, Deadline).

wait_loop(Pred, Deadline) ->
    case Pred() of
        true -> ok;
        false ->
            case erlang:monotonic_time(millisecond) of
                T when T > Deadline -> {error, timeout};
                _ ->
                    timer:sleep(50),
                    wait_loop(Pred, Deadline)
            end
    end.
