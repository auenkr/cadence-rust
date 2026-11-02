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

## Validation

No Cargo, rustfmt, Go or mixed runtime validation ran locally.

Initial head `9688897ee626fabf7e643246389896fb8478c3e0`:
- Frontend CI / runner formatting:
  https://github.com/auenkr/cadence-rust/actions/runs/41417802056
- Cassandra mixed gate:
  https://github.com/auenkr/cadence-rust/actions/runs/41417807003

The existing Cassandra workflow is Matching-only (Go Frontend/History/Worker).
It passed as requested, but cannot establish Rust Frontend Cassandra
history acceptance. No Cassandra Frontend mixed lane exists in
.github/workflows or tests/gates. End-to-end Frontend Cassandra history,
decision pagination and restart/recovery remain uncovered.

### Retained Cassandra gate result

https://github.com/auenkr/cadence-rust/actions/runs/41417807003 — **success**.
Head: `9688897ee626fabf7e643246389896fb8478c3e0`.
Rust cadence-matching SHA256:
`abd071833052dcd1786130bff1325f04093c002023592f58473f666daced6556`.
Fresh target: `/tmp/cadence-cargo-target-cassandra-41417807003`.
Artifact: `matching-cassandra-mixed-gate`; gate.exit/run.exit/probeExitCode
are all 0. results.json explicitly names Go Frontend/History/Worker with
Rust Matching, goMatchingStarted=false, acceptanceEligible=true and Cassandra
persistence. Container cleanup is recorded in cassandra-container-cleanup.txt.
This gate predates the additional paging test and formatting/assertion fixes;
production reader/wiring are unchanged.

### CI iteration

- 41417802056: cancelled when superseded; CI-generated format patch applied.
- https://github.com/auenkr/cadence-rust/actions/runs/41418190200:
  head `022c1f6bf`, failure in the new unit assertion's type comparison
  (PagingState exposes Option<&Arc<[u8]>>, not Option<&[u8]>).
  Fixed by comparing the underlying slice, and applied runner rustfmt patch.
- 41418810386 and 41418814535: superseded/cancelled redundant dispatches.
- Final fresh workspace CI:
  https://github.com/auenkr/cadence-rust/actions/runs/41418843296
  head `e569228282dfef6b542ad963c6794d6ca3ee9732`.
  **success**, clean_target=true. Artifact rust-ci-logs confirms head and
  target `/home/runner/work/_temp/cargo-target-final`; clippy.exit,
  test.exit, fmt.exit and workspace-format.exit are all 0. Clippy preceded
  workspace tests. All four new tests passed:
  - history_cassandra::tests::keys_and_opaque_page_state_preserve_go_query
  - history_cassandra::tests::cassandra_transaction_ids_are_not_sql_negated
  - history_cassandra::tests::validates_page_size_and_starts_without_state
  - history_reader::tests::native_paging_resumes_within_node_and_skips_older_transaction

Final evidence-only commit follows this tested code head; no Rust changes
were made after the green run. No merge or branch deletion performed.
