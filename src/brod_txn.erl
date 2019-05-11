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

-module(brod_txn).

-export([ init/2
        , produce/4
        , commit/1
        , abort/1
        ]).

-type transactional_id() :: brod:transactional_id().
-type client() :: brod:client().
-type txn_ctx() :: brod:txn_ctx().
-type topic() :: brod:topic().
-type partition() :: brod:partition().
-type batch_input() :: brod:batch_input().
-type rsp() :: kpro:rsp().
-type seqno() :: kpro:seqno().

-type transaction() :: #{ client => client()
                        , ctx => txn_ctx()
                        , seqnos => #{}
                        }.

-spec init(client(), transactional_id()) ->
              {ok, transaction()} | {error, any()}.
init(Client, TxnId) ->
  case brod_client:get_txn_coordinator(Client, TxnId) of
    {ok, {Endpoint, ConnCfg}} ->
      case kpro:connect(Endpoint, ConnCfg) of
        {ok, Conn} ->
          do_init(Client, TxnId, Conn);
        {error, Reason} ->
          {error, Reason}
      end;
    {error, Reason} ->
      {error, Reason}
  end.

do_init(Client, TxnId, Conn) ->
  case kpro:txn_init_ctx(Conn, TxnId) of
    {ok, TxnCtx} ->
      {ok, #{client => Client
            , ctx => TxnCtx
            , seqnos => #{}
            }};
    {error, Reason} ->
      {error, Reason}
  end.

-spec produce(transaction(), topic(), partition(), batch_input()) ->
                 {ok, rsp()} | {error, any()}.
produce(Transaction = #{seqnos := Seqnos0, ctx := TxnCtx, client := Client},
        Topic, Partition, Batch) ->
  case maybe_send_partition(Transaction, Topic, Partition) of
    {ok, Seqno} ->
      Seqnos1 = maps:put({Topic, Partition}, Seqno + length(Batch), Seqnos0),
      Opts = #{txn_ctx => TxnCtx
              , first_sequence => Seqno},
      Vsn = 3, %% lowest API version which supports transactional produce
      Req = kpro_req_lib:produce(Vsn, Topic, Partition, Batch, Opts),
      {ok, Leader} =
        brod_client:get_leader_connection(Client, Topic, Partition),
      case kpro:request_sync(Leader, Req, 5000) of
        {ok, _Response} ->
          %% TODO: validate response
          {ok, maps:put(seqnos, Seqnos1, Transaction)};
        Error -> Error
      end;
    Error -> Error
  end.

-spec maybe_send_partition(transaction(), topic(), partition()) ->
                              {ok, seqno()} | {error, any()}.
maybe_send_partition(#{seqnos := Seqnos, ctx := TxnCtx}, Topic, Partition) ->
  case maps:get({Topic, Partition}, Seqnos, undefined) of
    undefined ->
      case kpro:txn_send_partitions(TxnCtx, [{Topic, Partition}]) of
        ok -> {ok, 0};
        Error -> Error
      end;
    Seqno ->
      {ok, Seqno}
  end.

-spec commit(transaction()) -> ok | {error, any()}.
commit(#{ctx := TxnCtx}) ->
  kpro:txn_commit(TxnCtx).

-spec abort(transaction()) -> ok | {error, any()}.
abort(#{ctx := TxnCtx}) ->
  kpro:txn_abort(TxnCtx).


%%%_* Emacs ====================================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
