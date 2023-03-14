%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(rocketmq_producer).

-behaviour(gen_statem).

-export([ send/2
        , send_sync/2
        , send_sync/3
        , batch_send_sync/2
        , batch_send_sync/3
        ]).

-export([ start_link/5
        , idle/3
        , connected/3
        ]).

-export([ callback_mode/0
        , init/1
        , terminate/3
        , code_change/4
        ]).

callback_mode() -> [state_functions].

-define(TIMEOUT, 60000).

-define(MAX_SEQ_ID, 18445618199572250625).

-define(TCPOPTIONS, [
    binary,
    {packet,    raw},
    {reuseaddr, true},
    {nodelay,   true},
    {active,    true},
    {reuseaddr, true},
    {send_timeout, ?TIMEOUT}]).

-record(state, {producer_group,
                topic,
                server,
                sock,
                queue_id,
                opaque_id = 1,
                opts = [],
                callback,
                batch_size = 0,
                requests = #{},
                last_bin = <<>>,
                producer_opts
                }).

start_link(QueueId, Topic, Server, ProducerGroup, ProducerOpts) ->
    gen_statem:start_link(?MODULE, [QueueId, Topic, Server, ProducerGroup, ProducerOpts], []).

send(Pid, Message) ->
    gen_statem:cast(Pid, {send, Message}).

send_sync(Pid, Message) ->
    send_sync(Pid, Message, 5000).

send_sync(Pid, Message, Timeout) ->
    gen_statem:call(Pid, {send, Message}, Timeout).

batch_send_sync(Pid, Messages) ->
    batch_send_sync(Pid, Messages, 5000).

batch_send_sync(Pid, Messages, Timeout) ->
    gen_statem:call(Pid, {batch_send, Messages}, Timeout).

%%--------------------------------------------------------------------
%% gen_server callback
%%--------------------------------------------------------------------
init([QueueId, Topic, Server, ProducerGroup, ProducerOpts]) ->
    State = #state{producer_group = ProducerGroup,
                   topic = Topic,
                   queue_id = QueueId,
                   callback = maps:get(callback, ProducerOpts, undefined),
                   batch_size = maps:get(batch_size, ProducerOpts, 0),
                   server = Server,
                   opts = maps:get(tcp_opts, ProducerOpts, []),
                   producer_opts = ProducerOpts
                   },
    self() ! connecting,
    {ok, idle, State}.

idle(_, connecting, State = #state{opts = Opts, server = Server}) ->
    {Host, Port} = parse_url(Server),
    case gen_tcp:connect(Host, Port, merge_opts(Opts, ?TCPOPTIONS), ?TIMEOUT) of
        {ok, Sock} ->
            tune_buffer(Sock),
            gen_tcp:controlling_process(Sock, self()),
            start_keepalive(),
            {next_state, connected, State#state{sock = Sock}};
        Error ->
            {stop, Error, State}
    end;

idle(_, ping, State = #state{sock = undefined}) ->
    {keep_state, State}.

connected(_EventType, {tcp_closed, Sock}, State = #state{sock = Sock}) ->
    log_error("TcpClosed producer: ~p~n", [self()]),
    erlang:send_after(5000, self(), connecting),
    {next_state, idle, State#state{sock = undefined}};

connected(_EventType, {tcp, _, Bin}, State) ->
    handle_response(Bin, State);

connected({call, From}, {send, Message}, State = #state{sock = Sock,
                                                        topic = Topic,
                                                        queue_id = QueueId,
                                                        producer_group = ProducerGroup,
                                                        opaque_id = Opaque,
                                                        requests = Reqs,
                                                        producer_opts = ProducerOpts
                                                        }) ->
    send(Sock, ProducerGroup, get_namespace(ProducerOpts), Topic, Opaque, QueueId, {Message, <<>>}, get_acl_info(ProducerOpts)),
    {keep_state, next_opaque_id(State#state{requests = maps:put(Opaque, From, Reqs)})};

connected({call, From}, {batch_send, Messages}, State = #state{sock = Sock,
                                                        topic = Topic,
                                                        queue_id = QueueId,
                                                        producer_group = ProducerGroup,
                                                        opaque_id = Opaque,
                                                        requests = Reqs,
                                                        producer_opts = ProducerOpts
                                                        }) ->
    batch_send(Sock, ProducerGroup, get_namespace(ProducerOpts), Topic, Opaque, QueueId, Messages, get_acl_info(ProducerOpts)),
    {keep_state, next_opaque_id(State#state{requests = maps:put(Opaque, From, Reqs)})};

connected(cast, {send, Message}, State = #state{sock = Sock,
                                                topic = Topic,
                                                queue_id = QueueId,
                                                producer_group = ProducerGroup,
                                                opaque_id = Opaque,
                                                batch_size = BatchSize,
                                                producer_opts = ProducerOpts,
                                                requests = Requests
                                                }) ->
    BatchLen =
        case BatchSize =:= 0 of
            true ->
                _ = send(Sock, ProducerGroup, get_namespace(ProducerOpts), Topic, Opaque, QueueId, {Message, <<>>}, get_acl_info(ProducerOpts)),
                1;
            false ->
                Messages = [{Message, <<>>} | collect_send_calls(BatchSize)],
                _ = batch_send(Sock, ProducerGroup, get_namespace(ProducerOpts), Topic, Opaque, QueueId, Messages, get_acl_info(ProducerOpts)),
                erlang:length(Messages)
        end,
    NRequests = maps:put(Opaque, {batch_len, BatchLen}, Requests),
    NState = next_opaque_id(State),
    {keep_state, NState#state{requests = NRequests}};

connected(_EventType, ping, State = #state{sock = Sock,
                                           producer_group = ProducerGroup,
                                           opaque_id = Opaque,
                                           producer_opts = ProducerOpts}) ->
    ping(Sock, ProducerGroup, Opaque, get_acl_info(ProducerOpts)),
    {keep_state, next_opaque_id(State)};

connected(_EventType, EventContent, State) ->
    handle_response(EventContent, State).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, _State) ->
    ok.


handle_response(<<>>, State) ->
    {keep_state, State};

handle_response(Bin, State = #state{requests = Reqs,
                                    callback = Callback,
                                    topic = Topic,
                                    last_bin = LastBin}) ->
    case rocketmq_protocol_frame:parse(<<LastBin/binary, Bin/binary>>) of
        {undefined, undefined, Bin1} ->
            {keep_state, State#state{last_bin = Bin1}};
        {Header, _, Bin1} ->
            NewReqs = do_response(Header, Reqs, Callback, Topic),
            handle_response(Bin1, State#state{requests = NewReqs, last_bin = <<>>})
    end.

do_response(Header, Reqs, Callback, Topic) ->
    {ok, Opaque} = maps:find(<<"opaque">>, Header),
    case maps:get(Opaque, Reqs, undefined) of
        {batch_len, Len} ->
            case maps:get(<<"extFields">>, Header, undefined) of
                undefined -> ok;
                _ ->
                    case Callback =:= undefined of
                        true  -> ok;
                        false ->
                            case Callback of
                                {M, F, A} ->
                                    erlang:apply(M, F, [result(Header), Topic, Len] ++ A);
                                Callback when is_function(Callback) ->
                                    Callback(result(Header), Topic, Len)
                            end
                    end
            end,
            Reqs;
        undefined ->
            %% ignore heart beat response
            Reqs;
        From ->
            gen_statem:reply(From, result(Header)),
            maps:remove(Opaque, Reqs)
    end.

result(Header) ->
    case maps:get(<<"code">>, Header, undefined) of
        0 -> ok;
        _ -> {error, Header}
    end.

start_keepalive() ->
    erlang:send_after(30*1000, self(), ping).

ping(Sock, ProducerGroup, Opaque, ACLInfo) ->
    {ok, {Host, Port}} = inet:sockname(Sock),
    Host1 = inet_parse:ntoa(Host),
    ClientId = list_to_binary(lists:concat([Host1, "@", Port])),
    Package = rocketmq_protocol_frame:heart_beat(Opaque, ClientId, ProducerGroup, ACLInfo),
    gen_tcp:send(Sock, Package),
    start_keepalive().

send(Sock, ProducerGroup, Namespace, Topic, Opaque, QueueId, Message, ACLInfo) ->
    Package = rocketmq_protocol_frame:send_message_v2(Opaque, ProducerGroup, Namespace, Topic, QueueId, Message, ACLInfo),
    gen_tcp:send(Sock, Package).

batch_send(Sock, ProducerGroup, Namespace, Topic, Opaque, QueueId, Messages, ACLInfo) ->
    Package = rocketmq_protocol_frame:send_batch_message_v2(Opaque, ProducerGroup, Namespace, Topic, QueueId, Messages, ACLInfo),
    gen_tcp:send(Sock, Package).


collect_send_calls(0) ->
    [];
collect_send_calls(Cnt) when Cnt > 0 ->
    collect_send_calls(Cnt, []).

collect_send_calls(0, Acc) ->
    lists:reverse(Acc);

collect_send_calls(Cnt, Acc) ->
    receive
        {'$gen_cast', {send, Message}} ->
            collect_send_calls(Cnt - 1,  [{Message, <<>>} | Acc])
    after 0 ->
          lists:reverse(Acc)
    end.


tune_buffer(Sock) ->
    {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]} = inet:getopts(Sock, [recbuf, sndbuf]),
    inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

merge_opts(Defaults, Options) ->
    lists:foldl(
        fun({Opt, Val}, Acc) ->
                case lists:keymember(Opt, 1, Acc) of
                    true ->
                        lists:keyreplace(Opt, 1, Acc, {Opt, Val});
                    false ->
                        [{Opt, Val}|Acc]
                end;
            (Opt, Acc) ->
                case lists:member(Opt, Acc) of
                    true -> Acc;
                    false -> [Opt | Acc]
                end
        end, Defaults, Options).

parse_url(Server) ->
    case binary:split(Server, <<":">>) of
        [Host] -> {binary_to_list(Host), 10911};
        [Host, Port] -> {binary_to_list(Host), binary_to_integer(Port)};
        _ -> {"127.0.0.1", 10911}
    end.

log_error(Fmt, Args) -> error_logger:error_msg(Fmt, Args).

next_opaque_id(State = #state{opaque_id = ?MAX_SEQ_ID}) ->
    State#state{opaque_id = 1};
next_opaque_id(State = #state{opaque_id = OpaqueId}) ->
    State#state{opaque_id = OpaqueId+1}.


get_acl_info(ProducerOpts) ->
    maps:get(acl_info, ProducerOpts, #{}).

get_namespace(ProducerOpts) ->
    maps:get(namespace, ProducerOpts, <<>>).
