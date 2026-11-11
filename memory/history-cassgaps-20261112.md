# History Cassandra gaps (branch history-cassgaps, 2026-11-12)
Ported from Go nosql store / cassandra plugin:
1. `create_history_tasks` -> `CreateHistoryTasksRequest{shard_id, range_id, current_timestamp, tasks_by_category}`; transfer+timer only (others InvalidArgument = Go BadRequest); one logged batch with shard range_id CAS (`push_lease`); unapplied -> ShardOwnershipLost (actual range or -1). SQL keeps Go's "not implemented".
2. `select_workflow_timer_tasks` -> `SelectWorkflowTimerTasksRequest{shard,domain,workflow,run}`; reads `workflow_timer_tasks` from execution row, missing row -> empty.
3. `semaphore_map`: `SemaphoreInfoRecord` (typed, replaces opaque DataBlob in snapshot/mutation); create/upsert/delete/reset writes + read_execution; last entry per initiated_id wins (Go prepareSemaphoreInfosForWorkflowTxn).
Unit tests: module `semaphore_and_history_task_tests` in history_execution_cassandra_write.rs.
## CI evidence
Code head 23b4e036ae4b590a0154941bd67ddfdbfb67459b (already contained origin/master cbde9fa6c).
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/43100386549 success
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43100390755 success
- gate cassandra/grpc two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43101518302 success
- gate cassandra/grpc two_history=true: 43101523077 FAIL, 43104426824 FAIL (sdk-signal-after-restart, sdk-signal-done, sdk-complete-after-restart; no gate-summary-empty), then https://github.com/auenkr/cadence-rust/actions/runs/43107287166 SUCCESS on the same head.
- Baseline master (same gate cassandra/grpc/true, no changes): https://github.com/auenkr/cadence-rust/actions/runs/43107282363 FAIL -> the after-restart stall is a pre-existing intermittent (see history-cassandra-20261111.md), not caused by this branch. Still not root-caused: SDK logs "Corrupt signal received on channel" after History host restart.

