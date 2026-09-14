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
    %% True while a connect worker is in flight (see start_connect/1),
    %% so polls and drops do not start a second attempt.
    reconnecting = false,
    %% Opt-in free-form field reserved for hot upgrades so the record
    %% layout can be frozen across versions without appending new
    %% fields.
    extra = #{} :: map()
}).


-define(TIMEOUT, 60000).
-define(CONNECT_TIMEOUT, 10000).
-define(T_GET_ROUTEINFO, 15000).

-define(TCPOPTIONS, [
    binary,
    {packet,    raw},
    {reuseaddr, true},
    {nodelay,   true},
    %% Passive until the socket is owned by the client process
    %% (see activate/2); a connect worker must not receive data.
    {active,    false},
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
%% starts a connect worker when the socket is down so every poll from
%% the host application drives recovery.
%%
%%   connected              -- socket is currently up
%%   connecting             -- socket down, a connect attempt is in
%%                             flight and none has failed since the last
%%                             success (first connect, or the reconnect
%%                             that follows a passive close)
%%   {disconnected, Reason} -- one or more failed attempts since last
%%                             success; Reason is the last connect /
%%                             socket close error (e.g. econnrefused,
%%                             tcp_closed, {tls_alert, ...}).
%%
%% A socket closed by the peer starts a reconnect at once, in a worker
%% process, so the client keeps answering while a slow TCP connect or
%% TLS handshake is under way. In the common case the socket is back
%% before the next poll and the poll sees `connected'.
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
    {noreply, start_connect(State)};
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
            %% Reply instead of leaving the caller to its 15 s timeout.
            log(error, "Servers: ~p down, reason: ~p", [Servers, Reason]),
            {reply, {error, Reason}, record_connect_failure(Reason, State)};
        {ok, Sock1} ->
            ok = activate(Sock1, SockSendMod),
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
                                              opts = Opts,
                                              sock_mod = SockSendMod}) ->
    case get_sock(Servers, undefined, Opts) of
        {error, Reason} ->
            {reply, false, record_connect_failure(Reason, State)};
        {ok, Sock} ->
            ok = activate(Sock, SockSendMod),
            {reply, true, record_connect_success(State#state{sock = Sock})}
    end;
handle_call(get_status, _From, State) ->
    {reply, true, State};

handle_call(get_connection_state, _From, State = #state{sock = undefined,
                                                        reconnect_attempts = 0}) ->
    %% Socket is down and no connect attempt has failed since the last
    %% success: the first connect or the reconnect after a passive close
    %% is in flight. Report `connecting'; start_connect/1 is a no-op
    %% while a worker runs.
    {reply, connecting, start_connect(State)};
handle_call(get_connection_state, _From, State = #state{sock = undefined,
                                                        last_error = LastError}) ->
    %% One or more failed connect attempts since the last success --
    %% treat as a terminal failure until the host application drives
    %% another attempt. Callers map this to the `disconnected' resource
    %% status so a misconfigured connector surfaces correctly rather
    %% than appearing to be 'still trying'. Keep attempting in the
    %% background so recovery happens automatically once the broker is
    %% reachable again.
    {reply, {disconnected, LastError}, start_connect(State)};
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
    {noreply, reconnect_after_drop(tcp_closed, State), hibernate};

handle_info({ssl_closed, Sock}, State = #state{sock = Sock}) ->
    {noreply, reconnect_after_drop(ssl_closed, State), hibernate};

handle_info({ssl_error, Sock, Reason}, State = #state{sock = Sock}) ->
    _ = ssl:close(Sock),
    log(error, "RocketMQ client Received SSL socket error: ~p~n", [Reason]),
    {noreply, reconnect_after_drop({ssl_error, Reason}, State), hibernate};

handle_info({connect_result, Worker, Result}, State = #state{extra = Extra}) ->
    case maps:get(connect_worker, Extra, undefined) of
        {Worker, Ref} ->
            erlang:demonitor(Ref, [flush]),
            State1 = State#state{reconnecting = false, extra = maps:remove(connect_worker, Extra)},
            {noreply, connect_result(Result, State1), hibernate};
        _ ->
            %% A stale worker (its socket, if any, was closed by the worker).
            {noreply, State, hibernate}
    end;

handle_info({'DOWN', Ref, process, Worker, Reason}, State = #state{extra = Extra}) ->
    case maps:get(connect_worker, Extra, undefined) of
        {Worker, Ref} ->
            State1 = State#state{reconnecting = false, extra = maps:remove(connect_worker, Extra)},
            {noreply, record_connect_failure({connect_worker_down, Reason}, State1), hibernate};
        _ ->
            {noreply, State, hibernate}
    end;

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

%% Start a connect attempt in a worker process, unless one is in flight.
%% The worker connects (TCP, then TLS when configured) with the socket in
%% passive mode, hands the socket to the client and reports the result as
%% {connect_result, WorkerPid, {ok, Sock} | {error, Reason}}. The client
%% keeps serving calls meanwhile, so a slow connect or a stalled TLS
%% handshake never blocks a status poll or a route request.
start_connect(State = #state{reconnecting = true}) ->
    State;
start_connect(State = #state{sock = undefined, servers = Servers, opts = Opts,
                             sock_mod = SockMod, extra = Extra}) ->
    Client = self(),
    {Worker, Ref} = spawn_monitor(fun() -> connect_worker(Client, Servers, Opts, SockMod) end),
    State#state{reconnecting = true, extra = Extra#{connect_worker => {Worker, Ref}}};
start_connect(State) ->
    State.

connect_worker(Client, Servers, Opts, SockMod) ->
    Result =
        case get_sock(Servers, undefined, Opts) of
            {ok, Sock} ->
                case SockMod:controlling_process(Sock, Client) of
                    ok -> {ok, Sock};
                    {error, Reason} -> _ = SockMod:close(Sock), {error, {controlling_process, Reason}}
                end;
            {error, _} = Error ->
                Error
        end,
    Client ! {connect_result, self(), Result},
    ok.

connect_result({ok, Sock}, State = #state{sock = undefined, sock_mod = SockMod}) ->
    ok = activate(Sock, SockMod),
    record_connect_success(State#state{sock = Sock});
connect_result({ok, Sock}, State = #state{sock_mod = SockMod}) ->
    %% A synchronous connect (route request) won the race; keep that one.
    _ = SockMod:close(Sock),
    State;
connect_result({error, Reason}, State) ->
    record_connect_failure(Reason, State).

%% The socket is passive while it changes owner; make it active once the
%% client process owns it.
activate(Sock, gen_tcp) -> inet:setopts(Sock, [{active, true}]);
activate(Sock, ssl) -> ssl:setopts(Sock, [{active, true}]).

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

%% The name server closes a connection that carried no request for
%% serverChannelMaxIdleTimeSeconds (120 s by default). This client only
%% sends route requests on behalf of producers, so with no producer
%% running every idle period ends with a passive close. Reconnect right
%% away: a health check that follows then sees `connected' again, or
%% `{disconnected, Reason}' when the server is really gone, instead of a
%% one-poll `connecting' blip that raises and clears an alarm.
reconnect_after_drop(Reason, State) ->
    start_connect(record_socket_drop(Reason, State)).

get_sock(Servers, undefined, Opts) ->
    SSLOpts = maps:get(ssl_opts, Opts, undefined),
    ConnectTimeout = maps:get(connect_timeout, Opts, ?CONNECT_TIMEOUT),
    try_connect(Servers, SSLOpts, ConnectTimeout, no_servers);
get_sock(_Servers, Sock, _Opts) ->
    {ok, Sock}.

try_connect([], _SSLOpts, _ConnectTimeout, LastError) ->
    {error, LastError};
try_connect([{Host, Port} | Servers], SSLOpts, ConnectTimeout, _PrevError) ->
    case gen_tcp:connect(Host, Port, ?TCPOPTIONS, ConnectTimeout) of
        {ok, Sock} ->
            tune_buffer(Sock),
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
