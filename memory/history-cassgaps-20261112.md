# History Cassandra gaps (branch history-cassgaps, 2026-11-12)
Ported from Go nosql store / cassandra plugin:
1. `create_history_tasks` -> `CreateHistoryTasksRequest{shard_id, range_id, current_timestamp, tasks_by_category}`; transfer+timer only (others InvalidArgument = Go BadRequest); one logged batch with shard range_id CAS (`push_lease`); unapplied -> ShardOwnershipLost (actual range or -1). SQL keeps Go's "not implemented".
2. `select_workflow_timer_tasks` -> `SelectWorkflowTimerTasksRequest{shard,domain,workflow,run}`; reads `workflow_timer_tasks` from execution row, missing row -> empty.
3. `semaphore_map`: `SemaphoreInfoRecord` (typed, replaces opaque DataBlob in snapshot/mutation); create/upsert/delete/reset writes + read_execution; last entry per initiated_id wins (Go prepareSemaphoreInfosForWorkflowTxn).
Unit tests: module `semaphore_and_history_task_tests` in history_execution_cassandra_write.rs.
## CI evidence
(filled below)
