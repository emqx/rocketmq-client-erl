%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%--------------------------------------------------------------------

-module(rocketmq_client).

-behaviour(gen_server).

-export([start_link/3]).

-export([get_routeinfo_by_topic/2]).

-export([get_status/1]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {requests, opaque_id, sock, servers, opts, last_bin = <<>>}).

-define(TIMEOUT, 60000).
-define(T_GET_ROUTEINFO, 15000).

-define(TCPOPTIONS, [
    binary,
    {packet,    raw},
    {reuseaddr, true},
    {nodelay,   true},
    {active,    true},
    {reuseaddr, true},
    {send_timeout,  ?TIMEOUT}]).

start_link(ClientId, Servers, Opts) ->
    gen_server:start_link({local, ClientId}, ?MODULE, [Servers, Opts], []).

get_routeinfo_by_topic(Pid, Topic) ->
    try
        gen_server:call(Pid, {get_routeinfo_by_topic, Topic}, ?T_GET_ROUTEINFO)
    catch
        exit:{timeout, _Details} ->
            {error, timeout};
        exit:Reason ->
            {error, {rocketmq_client_down, Reason}}
    end.

get_status(Pid) ->
    gen_server:call(Pid, get_status, 5000).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([Servers, Opts]) ->
    State = #state{servers = Servers, opts = Opts},
    case get_sock(Servers, undefined) of
        error ->
            {stop, fail_to_connect_rocketmq_server};
        Sock ->
            {ok, State#state{sock = Sock, opaque_id = 1, requests = #{}}}
    end.

handle_call({get_routeinfo_by_topic, Topic}, From, State = #state{opaque_id = OpaqueId,
                                                                  sock = Sock,
                                                                  requests = Reqs,
                                                                  servers = Servers,
                                                                  opts = Opts
                                                                  }) ->
    case get_sock(Servers, Sock) of
        error ->
            log_error("Servers: ~p down", [Servers]),
            {noreply, State};
        Sock1 ->
            ACLInfo = maps:get(acl_info, Opts, #{}),
            Namespace = maps:get(namespace, Opts, <<>>),
            Package = rocketmq_protocol_frame:get_routeinfo_by_topic(OpaqueId, Namespace, Topic, ACLInfo),
            gen_tcp:send(Sock1, Package),
            {noreply, next_opaque_id(State#state{requests = maps:put(OpaqueId, From, Reqs), sock = Sock1})}
    end;

handle_call(get_status, _From, State = #state{sock = undefined, servers = Servers}) ->
    case get_sock(Servers, undefined) of
        error -> {reply, false, State};
        Sock -> {reply, true, State#state{sock = Sock}}
    end;
handle_call(get_status, _From, State) ->
    {reply, true, State};

handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({tcp, _, Bin}, State) ->
    handle_response(Bin, State);

handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, State#state{sock = undefined}, hibernate};

handle_info(_Info, State) ->
    log_error("RocketMQ client Receive unknown message:~p~n", [_Info]),
    {noreply, State, hibernate}.

terminate(_Reason, #state{}) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

handle_response(<<>>, State) ->
    {noreply, State, hibernate};

handle_response(Bin, State = #state{requests = Reqs, last_bin = LastBin}) ->
    case rocketmq_protocol_frame:parse(<<LastBin/binary, Bin/binary>>) of
        {undefined, undefined, Bin1} ->
            {noreply, State#state{last_bin = Bin1}, hibernate};
        {Header, Payload, Bin1} ->
            NewReqs = do_response(Header, Payload, Reqs),
            handle_response(Bin1, State#state{requests = NewReqs, last_bin = <<>>})
        end.

do_response(Header, Payload, Reqs) ->
    OpaqueId = maps:get(<<"opaque">>, Header, 1),
    case maps:get(OpaqueId, Reqs, undefined) of
        undefined ->
            Reqs;
        From ->
            gen_server:reply(From, {ok, {Header, Payload}}),
            maps:remove(OpaqueId, Reqs)
    end.

tune_buffer(Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]}
        = inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

get_sock(Servers, undefined) ->
    try_connect(Servers);
get_sock(_Servers, Sock) ->
    Sock.

try_connect([]) ->
    error;
try_connect([{Host, Port} | Servers]) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            Sock;
        _Error ->
            try_connect(Servers)
    end.

log_error(Fmt, Args) ->
    logger:error("[rocketmq_client]: " ++ Fmt, Args).

next_opaque_id(State = #state{opaque_id = 65535}) ->
    State#state{opaque_id = 1};
next_opaque_id(State = #state{opaque_id = OpaqueId}) ->
    State#state{opaque_id = OpaqueId+1}.
