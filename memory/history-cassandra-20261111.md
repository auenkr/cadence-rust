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

## Pieces 1-4 (history v2, visibility, store exposure, env wiring)
- history_v2_cassandra.rs: CassandraHistoryV2Store (append/read/fork/delete/get_tree/all_branches). rust-ci persistence run 43051731287 success @183b376.
- history_visibility_cassandra.rs + cadence-history visibility.rs: `VisibilityWriter` trait (SQL + Cassandra impls); HistoryEnv.visibility is Arc<dyn>. Row gained task_list/workflow_timeout_seconds/retention_seconds. TTL: open = timeout+86400, closed = retention (0 -> 86400); ttl > 157680000 uses no-TTL template (Go). Delete is a no-op (admin ctx key unsupported). Query/batch timestamp passes ms value as-is like Go.
- ConfiguredMatchingStores.history_cassandra (executions, history_v2, visibility) + CassandraShardStore as history_shard_store; HistoryEnv::build picks Cassandra when set.
- rust-ci @1967d791f: persistence 43054595642, matching 43054601773, history 43054609240 all success.
- Gaps: create_history_tasks still errors (Task has no category/range id; no Rust caller; SQL also unimplemented) - not done. Semaphore maps not done.
- Gates dispatched @1967d791f: cassandra/grpc/false 43056030102, cassandra/grpc/true 43056035699, mysql/grpc/false 43056041172 (results below).

## Gate results (final, 2026-11-12)
Fixes since pieces 1-4: Cassandra task reader/writer take task_id and timer visibility from `Task` (blob has none; also queues.rs fallback); visibility TTL bound as int; current-run `create_request_id` is uuid; UDT list/map binds use `Filled` (driver needs UDT keyspace/name from column type; history_tree ancestors bound as value, not literal); clippy useless_format.

| Run | Gate | Head SHA | Result |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/43075200452 | cassandra/grpc/two_history=false | 998213ba0 | success |
| https://github.com/auenkr/cadence-rust/actions/runs/43075206911 | cassandra/grpc/two_history=true | 998213ba0 | FAIL (restart-churn timeouts; flaky) |
| https://github.com/auenkr/cadence-rust/actions/runs/43080788217 | cassandra/grpc/two_history=true (rerun) | e658298a8 | success |
| https://github.com/auenkr/cadence-rust/actions/runs/43075212994 | mysql/grpc/false | 998213ba0 | success |
| https://github.com/auenkr/cadence-rust/actions/runs/43079175139 | mysql/grpc/true | 998213ba0 | success |
| 43076177515, 43076183276 | rust-ci persistence / history | e658298a8 | success |

Rust binary SHA256 (run 43080788217): cadence-server ad037508ca60c8f0f32202d07aafde4968102564809735c63cf816f25f32dbaa; cadence-history 191513a6050d820317efe365b52d4ed0e7d6d557a2de1c97980696c6da38d2dc.

Open items: the cassandra/true failure on 43075206911 passed on rerun, so treat it as an intermittent after-restart stall (not root-caused; no Cassandra-specific error in logs); `create_history_tasks` category-less stub still returns Internal; semaphore maps and `select_workflow_timer_tasks` (empty Vec) not done; visibility upsert returns "Operation is not supported" like Go's nosql store.
