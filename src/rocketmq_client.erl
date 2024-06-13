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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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

-record(state, {requests, opaque_id, sock, sock_mod = gen_tcp, servers, opts, last_bin = <<>>}).


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
    SSLOpts = maps:get(ssl_opts, Opts, undefined),
    SockSendMod = case SSLOpts of
                      undefined ->
                          gen_tcp;
                      _ ->
                          ssl
                  end,
    case get_sock(Servers, undefined, SSLOpts) of
        error ->
            {stop, fail_to_connect_rocketmq_server};
        Sock ->
            {ok, State#state{sock = Sock, opaque_id = 1, requests = #{}, sock_mod = SockSendMod}}
    end.

handle_call({get_routeinfo_by_topic, Topic}, From, State = #state{opaque_id = OpaqueId,
                                                                  sock = Sock,
                                                                  requests = Reqs,
                                                                  servers = Servers,
                                                                  opts = Opts,
                                                                  sock_mod = SockSendMod
                                                                  }) ->
    case get_sock(Servers, Sock, maps:get(ssl_opts, Opts, undefined)) of
        error ->
            log(error, "Servers: ~p down", [Servers]),
            {noreply, State};
        Sock1 ->
            ACLInfo = maps:get(acl_info, Opts, #{}),
            Namespace = maps:get(namespace, Opts, <<>>),
            Package = rocketmq_protocol_frame:get_routeinfo_by_topic(OpaqueId, Namespace, Topic, ACLInfo),
            SockSendMod:send(Sock1, Package),
            {noreply, next_opaque_id(State#state{requests = maps:put(OpaqueId, From, Reqs), sock = Sock1})}
    end;

handle_call(get_status, _From, State = #state{sock = undefined, servers = Servers, opts = Opts}) ->
    case get_sock(Servers, undefined, maps:get(ssl_opts, Opts, undefined)) of
        error -> {reply, false, State};
        Sock -> {reply, true, State#state{sock = Sock}}
    end;
handle_call(get_status, _From, State) ->
    {reply, true, State};

handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({tcp, Sock, Bin}, #state{sock = Sock} = State) ->
    handle_response(Bin, State);

handle_info({ssl, Sock, Bin}, #state{sock = Sock} = State) ->
    handle_response(Bin, State);

handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, State#state{sock = undefined}, hibernate};

handle_info({ssl_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, State#state{sock = undefined}, hibernate};

handle_info({ssl_error, Sock, Reason}, State = #state{sock = Sock}) ->
    _ = ssl:close(Sock),
    log(error, "RocketMQ client Received SSL socket error: ~p~n", [Reason]),
    {noreply, State#state{sock = undefined}, hibernate};

handle_info(_Info, State) ->
    log(error, "RocketMQ client Receive unknown message:~p~n", [_Info]),
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
            log(warning, "Received incomplete message from peer, raw_bin: ~0p", [Bin1]),
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

get_sock(Servers, undefined, SSLOpts) ->
    try_connect(Servers, SSLOpts);
get_sock(_Servers, Sock, _SSLOpts) ->
    Sock.

try_connect([], _SSLOpts) ->
    error;
try_connect([{Host, Port} | Servers], SSLOpts) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            case maybe_upgrade_tls(Sock, SSLOpts) of
                {error, TLSConnectErrorReason} ->
                    log(warning, "Could not establish TLS connection ~p:~p, Reason: ~p",
                        [Host, Port, TLSConnectErrorReason]),
                    try_connect(Servers, SSLOpts);
                TLSSock ->
                    TLSSock
            end;
        {error, TCPConnectErrorReason} ->
            log(warning, "Could not establish TCP connection ~p:~p, Reason: ~p",
                [Host, Port, TCPConnectErrorReason]),
            try_connect(Servers, SSLOpts)
    end.

maybe_upgrade_tls(Sock, undefined) ->
    Sock;
maybe_upgrade_tls(Sock, SSLOpts) ->
    case ssl:connect(Sock, SSLOpts, ?TIMEOUT) of
        {ok, Sock1} ->
            ?tp(rocketmq_client_got_tls_sock, #{}),
            Sock1;
        Error ->
            Error
    end.


log(Level, Fmt, Args) ->
    logger:log(Level, "[rocketmq_client]: " ++ Fmt, Args).

next_opaque_id(State = #state{opaque_id = 65535}) ->
    State#state{opaque_id = 1};
next_opaque_id(State = #state{opaque_id = OpaqueId}) ->
    State#state{opaque_id = OpaqueId+1}.
