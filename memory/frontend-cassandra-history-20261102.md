# Frontend Cassandra history reader — 2026-11-02

Branch: `t3/frontend-cassandra-history-reader`.

## Implementation

- Added `cadence-persistence::history_cassandra::CassandraHistoryNodeStore`,
  using the existing CassandraBlockingSession, configured consistency, Go's
  SELECT column/binding order and native driver page state.
- Wired the reader into Cassandra ConfiguredMatchingStores; the Frontend
  launcher already installs this shared history reader for workflow history
  and decision reads. Calls run through Frontend's existing spawn_blocking.
- Extended HistoryNodeStore with a default SQL select_page implementation.
  SQL retains node-ID cursors; Cassandra passes opaque tokens unchanged.
  Empty Cassandra pages preserve continuation state.
- Unit coverage: UUID partition/branch keys, exclusive node range, opaque
  paging state, page-size validation, transaction sign, and a continuation
  within one node's versions that drops stale transactions.

## Transaction sign correction to task premise

SQL stores negative transaction IDs and must negate on read. **Go Cassandra
does not negate transaction IDs**:
`go/common/persistence/nosql/nosql_history_store.go:AppendHistoryNodes`
sets TxnID directly from request.TransactionID;
`nosqlplugin/cassandra/history_events.go:InsertIntoHistoryTreeAndNode`
binds it unchanged, and SelectFromHistoryNode scans it unchanged.
`go/schema/cassandra/cadence/schema.cql` defines
PRIMARY KEY ((tree_id), branch_id, node_id, txn_id) with txn_id DESC.
The reader therefore preserves the raw Cassandra value. Applying SQL
negation would skip valid positive transactions against LastTransactionID=0.
The sign regression test explicitly checks this backend distinction.

## Validation (in progress)

No Cargo, rustfmt, Go or mixed runtime validation ran locally.

Initial head `9688897ee`:
- Frontend CI / runner formatting:
  https://github.com/auenkr/cadence-rust/actions/runs/41417802056
- Cassandra mixed gate:
  https://github.com/auenkr/cadence-rust/actions/runs/41417807003

The existing Cassandra workflow is Matching-only (Go Frontend/History/Worker).
It is being run as requested, but cannot establish Rust Frontend Cassandra
history acceptance. No Cassandra Frontend mixed lane exists in
.github/workflows or tests/gates. End-to-end Frontend Cassandra history,
decision pagination and restart/recovery remain uncovered.
