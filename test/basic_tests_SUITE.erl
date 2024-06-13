-module(basic_tests_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").


-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2, end_per_testcase/2]).
-export([
         basic_test/1,
         basic_test_tls/1
        ]).

all() ->
    [
     basic_test,
     basic_test_tls
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

basic_test(_Config) ->
    ?assertEqual(ok, test:start()),
    ok.

basic_test_tls(_Config) ->
     ?check_trace(
         %% Run stage:
         begin
           test:start_tls()
         end,
         %% Check stage:
         fun(RunStageResult, Trace) ->
             ?assertMatch(ok, RunStageResult),
             ?assertMatch([_|_], ?of_kind(rocketmq_client_got_tls_sock, Trace)),
             ?assertMatch([_|_], ?of_kind(rocketmq_producer_got_tls_sock, Trace))
         end).
