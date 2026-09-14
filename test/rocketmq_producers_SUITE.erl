%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Producer start-up against a fake name server that has no route for
%% any topic (a broker with auto-create disabled, or a namespace where
%% the topic was never created).
%%--------------------------------------------------------------------

-module(rocketmq_producers_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("rocketmq.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([t_topic_not_found_returns_error/1,
         t_topic_not_found_is_retried_fresh/1,
         t_default_topic_lookup_is_not_namespaced/1,
         t_check_topic_reports_topic_not_found/1,
         t_other_error_codes_are_not_topic_not_found/1]).

-define(TOPIC_NOT_EXIST, 17).
-define(SYSTEM_BUSY, 2).
-define(NO_PERMISSION, 16).

all() ->
    [t_topic_not_found_returns_error,
     t_topic_not_found_is_retried_fresh,
     t_default_topic_lookup_is_not_namespaced,
     t_check_topic_reports_topic_not_found,
     t_other_error_codes_are_not_topic_not_found].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(rocketmq),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(rocketmq).

init_per_testcase(TC, Config) ->
    {ok, Server, Port} = start_fake_namesrv(),
    ClientId = list_to_atom("rocketmq_producers_test_" ++ atom_to_list(TC)),
    {ok, _} = rocketmq:ensure_supervised_client(
                ClientId, [{"127.0.0.1", Port}], #{namespace => <<"ns1">>}),
    [{client_id, ClientId}, {fake_namesrv, Server} | Config].

end_per_testcase(_TC, Config) ->
    ok = rocketmq:stop_and_delete_supervised_client(?config(client_id, Config)),
    stop_fake_namesrv(?config(fake_namesrv, Config)),
    ok.

%%--------------------------------------------------------------------
%% Cases

%% No route for the topic and none for the default topic: the caller
%% gets a tagged error instead of a crash from the supervisor reply.
t_topic_not_found_returns_error(Config) ->
    ClientId = ?config(client_id, Config),
    Res = rocketmq:ensure_supervised_producers(
            ClientId, <<"group1">>, <<"t1">>, producer_opts(?FUNCTION_NAME)),
    ?assertMatch({error, {topic_not_found, #{topic := <<"t1">>, remark := <<_/binary>>}}}, Res),
    ok.

%% A failed start leaves nothing behind in the supervisor, so a later
%% call makes a fresh attempt instead of reporting `already_present'.
t_topic_not_found_is_retried_fresh(Config) ->
    ClientId = ?config(client_id, Config),
    Opts = producer_opts(?FUNCTION_NAME),
    ?assertMatch({error, {topic_not_found, _}},
                 rocketmq:ensure_supervised_producers(ClientId, <<"group1">>, <<"t1">>, Opts)),
    ?assertMatch({error, {topic_not_found, _}},
                 rocketmq:ensure_supervised_producers(ClientId, <<"group1">>, <<"t1">>, Opts)),
    ?assertNot(is_child(ClientId, ?FUNCTION_NAME)),
    ok.

%% The user topic is looked up with the namespace prefix; the default
%% topic fallback is looked up by its bare system name.
t_default_topic_lookup_is_not_namespaced(Config) ->
    ClientId = ?config(client_id, Config),
    _ = rocketmq:ensure_supervised_producers(
          ClientId, <<"group1">>, <<"t1">>, producer_opts(?FUNCTION_NAME)),
    ?assertEqual([<<"ns1%t1">>, ?DEFAULT_TOPIC],
                 requested_topics(?config(fake_namesrv, Config))),
    ok.

%% check_topic/2 runs the same lookup as a producer start, without
%% starting anything.
t_check_topic_reports_topic_not_found(Config) ->
    ClientId = ?config(client_id, Config),
    ?assertMatch({error, {topic_not_found, #{topic := <<"t1">>, remark := <<_/binary>>}}},
                 rocketmq:check_topic(ClientId, <<"t1">>)),
    ?assertEqual([<<"ns1%t1">>, ?DEFAULT_TOPIC],
                 requested_topics(?config(fake_namesrv, Config))),
    ?assertNot(is_child(ClientId, ?FUNCTION_NAME)),
    ok.

%% A bodyless answer with another code (name server busy, access denied)
%% is a lookup failure that keeps its code, not a missing topic, and it
%% does not fall back to the default topic.
t_other_error_codes_are_not_topic_not_found(Config) ->
    ClientId = ?config(client_id, Config),
    Server = ?config(fake_namesrv, Config),
    lists:foreach(
      fun(Code) ->
              set_response_code(Server, Code),
              ?assertMatch({error, {route_lookup_failed, #{topic := <<"t1">>, code := Code, remark := <<_/binary>>}}},
                           rocketmq:check_topic(ClientId, <<"t1">>)),
              ?assertMatch({error, {route_lookup_failed, #{topic := <<"t1">>, code := Code}}},
                           rocketmq:ensure_supervised_producers(
                             ClientId, <<"group1">>, <<"t1">>, producer_opts(?FUNCTION_NAME)))
      end,
      [?SYSTEM_BUSY, ?NO_PERMISSION]),
    %% Only the user topic was asked about: no default topic fallback.
    ?assertEqual(lists:duplicate(4, <<"ns1%t1">>), requested_topics(Server)),
    ok.

%%--------------------------------------------------------------------
%% helpers

%% Other suites on the same node may leave their own producers under the
%% supervisor, so only look for ours (child id {ClientId, Name}).
is_child(ClientId, Name) ->
    lists:keymember({ClientId, Name}, 1, supervisor:which_children(rocketmq_producers_sup)).

producer_opts(Name) ->
    #{name => Name, namespace => <<"ns1">>, ref_topic_route_interval => 60000}.

%% A name server that answers every route request with a bodyless error
%% (TOPIC_NOT_EXIST unless set otherwise) and records the topics it was
%% asked about.
-record(fake, {lsock, ctrl}).

start_fake_namesrv() ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}, {reuseaddr, true}, {packet, raw}]),
    {ok, Port} = inet:port(LSock),
    Parent = self(),
    Ctrl = spawn_link(fun() -> fake_ctrl(LSock, Parent, [], [], ?TOPIC_NOT_EXIST) end),
    {ok, #fake{lsock = LSock, ctrl = Ctrl}, Port}.

set_response_code(#fake{ctrl = Ctrl}, Code) ->
    Ctrl ! {set_code, Code},
    ok.

stop_fake_namesrv(#fake{ctrl = Ctrl}) ->
    Ref = erlang:monitor(process, Ctrl),
    Ctrl ! stop,
    receive {'DOWN', Ref, process, Ctrl, _} -> ok after 2000 -> ok end.

requested_topics(#fake{ctrl = Ctrl}) ->
    Ctrl ! {topics, self()},
    receive {topics, Topics} -> Topics after 2000 -> error(fake_namesrv_timeout) end.

fake_ctrl(LSock, Parent, Socks, Topics, Code) ->
    receive
        stop ->
            catch gen_tcp:close(LSock),
            [catch gen_tcp:close(S) || S <- Socks],
            ok;
        {set_code, NewCode} ->
            fake_ctrl(LSock, Parent, Socks, Topics, NewCode);
        {topics, From} ->
            From ! {topics, lists:reverse(Topics)},
            fake_ctrl(LSock, Parent, Socks, Topics, Code);
        {tcp, Sock, Bin} ->
            {Header, _Payload, <<>>} = rocketmq_protocol_frame:parse(Bin),
            Opaque = maps:get(<<"opaque">>, Header),
            Topic = maps:get(<<"topic">>, maps:get(<<"extFields">>, Header)),
            ok = gen_tcp:send(Sock, error_response(Code, Opaque, Topic)),
            fake_ctrl(LSock, Parent, Socks, [Topic | Topics], Code);
        {tcp_closed, _Sock} ->
            fake_ctrl(LSock, Parent, Socks, Topics, Code)
    after 0 ->
        case gen_tcp:accept(LSock, 50) of
            {ok, Sock} ->
                ok = inet:setopts(Sock, [{active, true}]),
                fake_ctrl(LSock, Parent, [Sock | Socks], Topics, Code);
            {error, timeout} ->
                fake_ctrl(LSock, Parent, Socks, Topics, Code);
            {error, _} ->
                ok
        end
    end.

error_response(Code, Opaque, Topic) ->
    Header = jsone:encode([{<<"code">>, Code},
                           {<<"opaque">>, Opaque},
                           {<<"flag">>, 1},
                           {<<"language">>, <<"JAVA">>},
                           {<<"version">>, 475},
                           {<<"remark">>, <<"No topic route info in name server for the topic: ",
                                            Topic/binary>>}]),
    HeaderLen = size(Header),
    <<(4 + HeaderLen):32, HeaderLen:32, Header/binary>>.
