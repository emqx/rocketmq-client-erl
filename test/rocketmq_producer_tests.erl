%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(rocketmq_producer_tests).

-include_lib("eunit/include/eunit.hrl").

-record(state, {
    producer_group,
    topic,
    server,
    sock,
    client_id = undefined,
    sock_mod = gen_tcp,
    queue_id,
    opaque_id = 1,
    opts = [],
    ssl_opts = undefined,
    callback,
    batch_size = 0,
    requests = #{},
    last_bin = <<>>,
    producer_opts
}).

completed_batch_response_removes_request_test() ->
    Opaque = 42,
    OtherOpaque = 999,
    State0 = #state{
        requests = #{Opaque => {batch_len, 3}, OtherOpaque => pending},
        callback = undefined,
        topic = <<"topic">>
    },
    {keep_state, State} = rocketmq_producer:connected(info, response(Opaque), State0),
    ?assertEqual(#{OtherOpaque => pending}, State#state.requests).

response(Opaque) ->
    HeaderData = jsone:encode([
        {<<"code">>, 0},
        {<<"opaque">>, Opaque},
        {<<"extFields">>, []}
    ]),
    HeaderLen = byte_size(HeaderData),
    Len = 4 + HeaderLen,
    <<Len:32, HeaderLen:32, HeaderData/binary>>.
