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

-export([get_status/1, get_connection_state/1]).

%% gen_server Callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_continue/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
    requests,
    opaque_id,
    sock,
    sock_mod = gen_tcp,
    servers,
    opts,
    last_bin = <<>>,
    %% Number of consecutive failed connect attempts since the last
    %% successful connect. Stays at 0 while the socket is up; resets
    %% to 0 on every successful get_sock. Used to distinguish a
    %% transient disconnect (just dropped, not yet retried) from a
    %% persistent failure.
    reconnect_attempts = 0,
    %% Last connect / socket failure reason since the last successful
    %% connect. `undefined' when the socket is healthy or has never
    %% failed. Cleared on successful connect.
    last_error = undefined,
    %% True while an async try_reconnect is in flight, to avoid
    %% queueing multiple reconnect attempts when health checks arrive
    %% faster than a connect attempt can complete.
    reconnecting = false
}).


-define(TIMEOUT, 60000).
-define(CONNECT_TIMEOUT, 10000).
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

%% Reports connection state without blocking on a connect attempt, and
%% kicks off an async reconnect when the socket is down so every poll
%% from the host application drives recovery.
%%
%%   connected              -- socket is currently up
%%   connecting             -- socket down, no failed retry yet
%%                             (transient drop or first attempt in flight)
%%   {disconnected, Reason} -- one or more failed attempts since last
%%                             success; Reason is the last connect /
%%                             socket close error (e.g. econnrefused,
%%                             tcp_closed, {tls_alert, ...}).
-spec get_connection_state(pid() | atom()) ->
    connected
    | connecting
    | {disconnected, term()}
    | {error, term()}.
get_connection_state(Pid) ->
    try
        gen_server:call(Pid, get_connection_state, 5000)
    catch
        exit:{timeout, _Details} ->
            {error, timeout};
        exit:Reason ->
            {error, {rocketmq_client_down, Reason}}
    end.

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([Servers, Opts]) ->
    SSLOpts = maps:get(ssl_opts, Opts, undefined),
    SockSendMod = case SSLOpts of
                      undefined ->
                          gen_tcp;
                      _ ->
                          ssl
                  end,
    %% Do not perform the initial TCP/TLS connect in init/1.
    %% The supervisor is blocked while init runs, so a slow/unreachable
    %% server would stall every other rocketmq client sharing the
    %% singleton rocketmq_client_sup. Kick off the connect asynchronously
    %% via handle_continue/2 instead; sock stays undefined until ready.
    State = #state{
        servers = Servers,
        opts = Opts,
        sock = undefined,
        opaque_id = 1,
        requests = #{},
        sock_mod = SockSendMod
    },
    {ok, State, {continue, connect}}.

handle_continue(connect, State) ->
    {noreply, do_connect(State)};
handle_continue(_, State) ->
    {noreply, State}.

handle_call({get_routeinfo_by_topic, Topic}, From, State = #state{opaque_id = OpaqueId,
                                                                  sock = Sock,
                                                                  requests = Reqs,
                                                                  servers = Servers,
                                                                  opts = Opts,
                                                                  sock_mod = SockSendMod
                                                                  }) ->
    case get_sock(Servers, Sock, Opts) of
        {error, Reason} ->
            log(error, "Servers: ~p down, reason: ~p", [Servers, Reason]),
            {noreply, record_connect_failure(Reason, State)};
        {ok, Sock1} ->
            ACLInfo = maps:get(acl_info, Opts, #{}),
            Namespace = maps:get(namespace, Opts, <<>>),
            Package = rocketmq_protocol_frame:get_routeinfo_by_topic(OpaqueId, Namespace, Topic, ACLInfo),
            SockSendMod:send(Sock1, Package),
            {noreply, next_opaque_id(
                record_connect_success(
                    State#state{requests = maps:put(OpaqueId, From, Reqs), sock = Sock1}
                ))}
    end;

handle_call(get_status, _From, State = #state{sock = undefined,
                                              servers = Servers,
                                              opts = Opts}) ->
    case get_sock(Servers, undefined, Opts) of
        {error, Reason} ->
            {reply, false, record_connect_failure(Reason, State)};
        {ok, Sock} ->
            {reply, true, record_connect_success(State#state{sock = Sock})}
    end;
handle_call(get_status, _From, State) ->
    {reply, true, State};

handle_call(get_connection_state, _From, State = #state{sock = undefined,
                                                        reconnect_attempts = 0}) ->
    %% Socket is down but no failed retry yet (either a transient drop
    %% that hasn't been retried, or init hasn't run handle_continue yet).
    %% Report `connecting' so a brief blip doesn't flip the status; still
    %% kick off a reconnect so we don't rely on the producer's slower
    %% route-refresh cadence to recover.
    {reply, connecting, kick_async_reconnect(State)};
handle_call(get_connection_state, _From, State = #state{sock = undefined,
                                                        last_error = LastError}) ->
    %% One or more failed connect attempts since the last success --
    %% treat as a terminal failure until the host application drives
    %% another attempt. Callers map this to the `disconnected' resource
    %% status so a misconfigured connector surfaces correctly rather
    %% than appearing to be 'still trying'. Keep attempting in the
    %% background so recovery happens automatically once the broker is
    %% reachable again.
    {reply, {disconnected, LastError}, kick_async_reconnect(State)};
handle_call(get_connection_state, _From, State) ->
    %% sock =/= undefined -- either a gen_tcp port or an ssl socket tuple
    {reply, connected, State};

handle_call(_Req, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Req, State) ->
    {noreply, State, hibernate}.

handle_info({tcp, Sock, Bin}, #state{sock = Sock} = State) ->
    handle_response(Bin, State);

handle_info({ssl, Sock, Bin}, #state{sock = Sock} = State) ->
    handle_response(Bin, State);

handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, record_socket_drop(tcp_closed, State), hibernate};

handle_info({ssl_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, record_socket_drop(ssl_closed, State), hibernate};

handle_info({ssl_error, Sock, Reason}, State = #state{sock = Sock}) ->
    _ = ssl:close(Sock),
    log(error, "RocketMQ client Received SSL socket error: ~p~n", [Reason]),
    {noreply, record_socket_drop({ssl_error, Reason}, State), hibernate};

handle_info(try_reconnect, State = #state{sock = undefined}) ->
    {noreply, do_connect(State#state{reconnecting = false}), hibernate};
handle_info(try_reconnect, State) ->
    {noreply, State#state{reconnecting = false}, hibernate};

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

%% Attempt a connect once, updating state accordingly. The caller
%% decides whether that was driven by init (handle_continue) or a
%% kick_async_reconnect (handle_info).
do_connect(State = #state{sock = undefined,
                          servers = Servers,
                          opts = Opts}) ->
    case get_sock(Servers, undefined, Opts) of
        {error, Reason} ->
            record_connect_failure(Reason, State);
        {ok, Sock} ->
            record_connect_success(State#state{sock = Sock})
    end;
do_connect(State) ->
    State.

record_connect_success(State) ->
    State#state{reconnect_attempts = 0, last_error = undefined}.

record_connect_failure(Reason, State = #state{reconnect_attempts = N}) ->
    State#state{reconnect_attempts = N + 1, last_error = Reason}.

record_socket_drop(Reason, State) ->
    %% A socket drop is not itself a failed connect attempt, so we
    %% don't bump reconnect_attempts here. We do remember the reason
    %% so get_connection_state can surface it if the next reconnect
    %% also fails.
    State#state{sock = undefined, last_error = Reason}.

kick_async_reconnect(State = #state{reconnecting = true}) ->
    State;
kick_async_reconnect(State) ->
    self() ! try_reconnect,
    State#state{reconnecting = true}.

get_sock(Servers, undefined, Opts) ->
    SSLOpts = maps:get(ssl_opts, Opts, undefined),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?CONNECT_TIMEOUT),
    try_connect(Servers, SSLOpts, ConnectTimeout, no_error);
get_sock(_Servers, Sock, _Opts) ->
    {ok, Sock}.

try_connect([], _SSLOpts, _ConnectTimeout, LastError) ->
    {error, LastError};
try_connect([{Host, Port} | Servers], SSLOpts, ConnectTimeout, _PrevError) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ConnectTimeout) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            case maybe_upgrade_tls(Sock, SSLOpts, ConnectTimeout) of
                {error, TLSConnectErrorReason} ->
                    log(warning, "Could not establish TLS connection ~p:~p, Reason: ~p",
                        [Host, Port, TLSConnectErrorReason]),
                    try_connect(Servers, SSLOpts, ConnectTimeout,
                                {tls_connect_error, {Host, Port, TLSConnectErrorReason}});
                {ok, TLSSock} ->
                    {ok, TLSSock}
            end;
        {error, TCPConnectErrorReason} ->
            log(warning, "Could not establish TCP connection ~p:~p, Reason: ~p",
                [Host, Port, TCPConnectErrorReason]),
            try_connect(Servers, SSLOpts, ConnectTimeout,
                        {tcp_connect_error, {Host, Port, TCPConnectErrorReason}})
    end.

maybe_upgrade_tls(Sock, undefined, _ConnectTimeout) ->
    {ok, Sock};
maybe_upgrade_tls(Sock, SSLOpts, ConnectTimeout) ->
    case ssl:connect(Sock, SSLOpts, ConnectTimeout) of
        {ok, Sock1} ->
            ?tp(rocketmq_client_got_tls_sock, #{}),
            {ok, Sock1};
        Error ->
            Error
    end.


log(Level, Fmt, Args) ->
    logger:log(Level, "[rocketmq_client]: " ++ Fmt, Args).

next_opaque_id(State = #state{opaque_id = 65535}) ->
    State#state{opaque_id = 1};
next_opaque_id(State = #state{opaque_id = OpaqueId}) ->
    State#state{opaque_id = OpaqueId+1}.
