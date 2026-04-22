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

-module(rocketmq_client_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

-export([ensure_present/3, ensure_absence/1, find_client/1]).

-define(SUPERVISOR, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SUPERVISOR}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 5
                },
    Children = [], %% dynamically added/stopped
    {ok, {SupFlags, Children}}.

%% ensure a client started under supervisor
ensure_present(ClientId, Hosts, Opts) ->
    ChildSpec = child_spec(ClientId, Hosts, Opts),
    case supervisor:start_child(?SUPERVISOR, ChildSpec) of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        {error, already_present} -> {error, client_not_running};
        {error, Reason} -> {error, Reason}
    end.

%% ensure client stopped and deleted under supervisor
ensure_absence(ClientId) ->
    case supervisor:terminate_child(?SUPERVISOR, ClientId) of
        ok -> ok = supervisor:delete_child(?SUPERVISOR, ClientId);
        {error, not_found} -> ok
    end.

%% find client pid from client id
%%
%% The client is registered locally under its ClientId atom (see
%% rocketmq_client:start_link/3), so whereis/1 resolves it in O(1)
%% without a gen_server:call into the supervisor. This avoids the
%% shared-supervisor serialisation that used to make every client's
%% status lookup queue behind any other client's start_child /
%% terminate_child / which_children on the same sup.
find_client(ClientId) ->
    case whereis(ClientId) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        undefined ->
            {error, {no_such_client, ClientId}}
    end.

child_spec(ClientId, Hosts, Opts) ->
    #{id => ClientId,
        start => {rocketmq_client, start_link, [ClientId, Hosts, Opts]},
        restart => transient,
        type => worker,
        modules => [rocketmq_client]
    }.
