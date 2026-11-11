# history-queuegaps (2026-11-11)

Branch `history-queuegaps` (from master 8e0b1064c, merged with master e50090050).

## Done
1. Hierarchical weighted round robin scheduler/pool ported to `rust/crates/cadence-history/src/task/hierarchical_wrr.rs`
   (Go `common/task/` ttl_channel, iwrr_schedule, iwrr_node, hierarchical pool + scheduler, with tests).
   `queue_provider.rs` `build_scheduler` picks hierarchical (default) or flat per
   `enable_hierarchical_weighted_round_robin_task_scheduler`, as Go `NewProcessor`.
   Not ported: metrics scopes, `scheduler_options.go` (FIFO/flat selector unused by history), flat-pool TTL cleanup.
2. Task reinjector bound: `ShardTaskReinjector` (shard_factory.rs), bound in `queue_exec`. It does Go's closed check and
   domain-entry resolution, then returns Go's SQL result `create history tasks not implemented for SQL`
   (Go SQL `CreateHistoryTasks` always errors; task-id allocation before it is unobservable on SQL).
   Real NoSQL reinjection (allocate ids, CreateHistoryTasks, update max read level) is not ported because the
   Rust persistence has no SQL path for it.
3. Timer standby `load_workflow`: `EngineTimerWorkflow` now implements `StandbyTimerWorkflow`
   (`last_write_version`, `history_resend_info`, `create_next_activity_timer`, `update_current_version`,
   `update_workflow_execution_as_passive` via new `EngineExecutionContext::update_as_passive`);
   `TimerStandbyDepsImpl::new` takes the execution cache.
   Known gap: `update_as_passive` always persists (Go does too); timer active `retry_workflow` /
   `update_with_new_as_active` still unported.

## CI evidence
- rust-ci (cadence-history) first run 43065928615 on 6776be11e: failed on 4 clippy lints, fixed in cbfd4f43.
- rust-ci (cadence-history) https://github.com/auenkr/cadence-rust/actions/runs/43066745231 head cbfd4f43c3b5ad632e7c48574ad078833f5c6d6c: success.
- Gate mysql/grpc/two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43068034385 head cbfd4f43.
- Gate postgres/grpc/two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43068040705 head cbfd4f43.

## Remaining

- Real task reinjection on a NoSQL persistence, metrics for hierarchical scheduler.
