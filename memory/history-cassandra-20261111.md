# History on Cassandra (branch history-cassandra) - 2026-11-11

## Ported so far
- `rust/crates/cadence-persistence/src/history_shard_cassandra.rs`: `CassandraShardStore` (impl of `history_sql::ShardStore`),
  Go shard.go / shard_cql.go / nosql_shard_store.go: LWT create (IF NOT EXISTS), get (+ range_id repair when UDT range > column),
  update (IF range_id = ?) with ShardOwnershipLost mapping. `data` thriftrw blob is authoritative; typed `shard` UDT only carries
  shard_id/range_id/updated_at (gap: other typed UDT fields are not populated, Go-readers using typed fields would not see them).

## Not done (precise remaining work)
1. `GoExecutionStore` for Cassandra (Go workflow.go/workflow_cql.go/workflow_utils.go/workflow_parsing_utils.go/tasks.go):
   executions table rows (execution/current/transfer/timer/replication/cross-cluster/DLQ/policy), UDT mapping
   (workflow_execution, activity_info, timer_info, child_execution_info, request_cancel_info, signal_info, semaphore_info, checksum),
   LOGGED LWT batches with shard fencing and CAS-failure row inspection, chunked task deletes (100, consistency ALL).
   Row-type constants: Shard0 Execution1 Transfer2 Timer3 Replication4 DLQ5 CrossCluster6 ...Policy11; task ids -10 (execution), -11 (shard).
2. `GoHistoryV2Store` Cassandra writers (history_node/history_tree append, fork, delete, tree listing); reader exists (history_cassandra.rs).
3. Cassandra History visibility writer (HistoryEnv.visibility is concrete SqlVisibilityWriter; needs a trait).
4. Expose stores from `open_configured_default_store` (cadence-matching matching_task_store.rs Cassandra branch) and make
   `HistoryEnv::build` (cadence-history assembly/env.rs) build them when `history_sql_backend` is None.
5. Port Go unit tests workflow_parsing_utils_test.go / workflow_utils_test.go.
6. CI: rust-ci persistence + history; history-mixed-gate cassandra/grpc two_history false+true; mysql/grpc/false regression.

## CI runs
| run | head | conclusion |
|---|---|---|
| (none yet) | | |
