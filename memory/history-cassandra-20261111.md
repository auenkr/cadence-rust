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

## Execution store status (2026-11-11)
All GoExecutionStore methods are implemented in `history_execution_cassandra*.rs` (write: create/get/update/conflict-resolve; tasks: transfer/timer/replication/cross-cluster/failover markers/history-task paging/DLQ/policy/list_*). No `not_ported` remains.

CI (rust-ci, package=cadence-persistence):
- 43045248981 success (7d75e48fd)
- 43048349775 failure (clippy), fixed
- 43049181082 success (clippy fixes)
- 43050448856 success (task/DLQ/listing commit)

Gaps/decisions: semaphore maps skipped; `create_history_tasks` (no category in trait) returns error; `select_workflow_timer_tasks` returns empty (as SQL); DLQ/cross-cluster use store `shard_id`/`target_cluster` (builders `with_shard_id`/`with_target_cluster`); `complete_history_task` for timers lacks scheduled time; duplicate request -> ConditionFailed; checksum opaque.
Remaining (later pieces): history v2 writers, visibility trait + Cassandra writer, expose stores/wire env.rs, Go unit-test ports, gates.
