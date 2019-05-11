%%%
%%%   Copyright (c) 2019, Klarna Bank AB (publ)
%%%
%%%   Licensed under the Apache License, Version 2.0 (the "License");
%%%   you may not use this file except in compliance with the License.
%%%   You may obtain a copy of the License at
%%%
%%%       http://www.apache.org/licenses/LICENSE-2.0
%%%
%%%   Unless required by applicable law or agreed to in writing, software
%%%   distributed under the License is distributed on an "AS IS" BASIS,
%%%   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%%   See the License for the specific language governing permissions and
%%%   limitations under the License.
%%%

%% @private
-module(brod_txn_SUITE).

%% Test framework
-export([ init_per_suite/1
        , end_per_suite/1
        , init_per_testcase/2
        , end_per_testcase/2
        , all/0
        , suite/0
        ]).

%% Test cases
-export([ t_produce/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("brod_int.hrl").

-define(HOSTS, [{"localhost", 9092}]).
-define(TOPIC, list_to_binary(atom_to_list(?MODULE))).

-define(config(Name), proplists:get_value(Name, Config)).

subscriber_loop(TesterPid) ->
  receive
    {ConsumerPid, KMS} ->
      #kafka_message_set{ messages = Messages
                        , partition = Partition} = KMS,
      lists:foreach(fun(#kafka_message{offset = Offset, key = K, value = V}) ->
                      TesterPid ! {Partition, Offset, K, V},
                      ok = brod:consume_ack(ConsumerPid, Offset)
                    end, Messages),
      subscriber_loop(TesterPid);
    Msg ->
      ct:fail("unexpected message received by test subscriber.\n~p", [Msg])
  end.

%%%_* ct callbacks =============================================================

suite() -> [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(brod),
  Config.

end_per_suite(_Config) -> ok.

init_per_testcase(Case, Config) ->
  try ?MODULE:Case({'init', Config})
  catch error : function_clause ->
    init_client(Case, Config)
  end.

init_client(Case, Config) ->
  Client = Case,
  Topic = ?TOPIC,
  brod:stop_client(Client),
  TesterPid = self(),
  ClientConfig = client_config(),
  ok = brod:start_client(?HOSTS, Client, ClientConfig),
  ok = brod:start_producer(Client, Topic, []),
  ok = brod:start_consumer(Client, Topic, []),
  Subscriber = spawn_link(fun() -> subscriber_loop(TesterPid) end),
  {ok, _ConsumerPid1} = brod:subscribe(Client, Subscriber, Topic, 0, []),
  {ok, _ConsumerPid2} = brod:subscribe(Client, Subscriber, Topic, 1, []),
  {ok, _ConsumerPid3} = brod:subscribe(Client, Subscriber, Topic, 2, []),
  [{client, Client},
   {client_config, ClientConfig},
   {subscriber, Subscriber} | Config].

end_per_testcase(_Case, Config) ->
  Subscriber = ?config(subscriber),
  is_pid(Subscriber) andalso unlink(Subscriber),
  is_pid(Subscriber) andalso exit(Subscriber, kill),
  Pid = whereis(?config(client)),
  try
    Ref = erlang:monitor(process, Pid),
    brod:stop_client(?config(client)),
    receive
      {'DOWN', Ref, process, Pid, _} -> ok
    end
  catch _ : _ ->
    ok
  end,
  Config.

all() -> [F || {F, _A} <- module_info(exports),
                  case atom_to_list(F) of
                    "t_" ++ _ -> true;
                    _         -> false
                  end].

%%%_* Test functions ===========================================================

t_produce(Config) when is_list(Config) ->
  Client = ?config(client),
  Topic = ?TOPIC,
  KV1 = make_unique_kv(),
  {ok, Transaction} = brod_txn:init(Client, <<"test-txn">>),
  {ok, Transaction1} = brod_txn:produce(Transaction, Topic, 0, [KV1]),
  KV2 = make_unique_kv(),
  {ok, Transaction2} = brod_txn:produce(Transaction1, Topic, 0, [KV2]),
  KV3 = make_unique_kv(),
  {ok, Transaction3} = brod_txn:produce(Transaction2, Topic, 1, [KV3]),
  KV4 = make_unique_kv(),
  {ok, Transaction4} = brod_txn:produce(Transaction3, Topic, 2, [KV4]),
  ok = brod_txn:commit(Transaction4),
  assert_receive(0, KV1),
  assert_receive(0, KV2),
  assert_receive(1, KV3),
  assert_receive(2, KV4).

%%%_* Help functions ===========================================================

assert_receive(Partition, #{ key := ExpectedK, value := ExpectedV}) ->
  receive
    {Partition, _, K, V} ->
      ?assertEqual(ExpectedK, K),
      ?assertEqual(ExpectedV, V)
  after 5000 ->
      ct:fail({?MODULE, ?LINE, timeout, ExpectedK, ExpectedV})
  end.

client_config() ->
  case os:getenv("KAFKA_VERSION") of
    "0.9" ++ _ -> [{query_api_versions, false}];
    _ -> []
  end.

%% os:timestamp should be unique enough for testing
make_unique_kv() ->
  #{ key => iolist_to_binary(["key-", make_ts_str()])
   , value => iolist_to_binary(["val-", make_ts_str()])
   }.

make_ts_str() -> brod_utils:os_time_utc_str().

%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
