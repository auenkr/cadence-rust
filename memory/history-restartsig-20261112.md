# history-restartsig (2026-11-12)

Bug: history-mixed-gate (grpc, two_history=true) intermittently failed after the History crash/restart with
`failed: ['sdk-signal-after-restart','sdk-signal-done','sdk-complete-after-restart','sdk-driver-exit']`
(cassandra runs 43099885426, 43101523077, 43104426824, 43107282363; ~25% of runs).

## Root cause
Not a signal bug. The `-sq` workflow hit its 5 min execution timeout before the driver sent the signal, so the
signal correctly returned WorkflowExecutionAlreadyCompleted. The driver was stalled in churnPhase waiting
(`run.Get`, no timeout) for a churn workflow that never got its first decision task: its transfer task was
never read. Rust History allocated transfer task ids (`allocate_transfer_ids`), released the shard lock, wrote to
the DB, then `update_max_read_level(max_id)`. With 4 concurrent churn writers, a writer with a higher id could
commit first and raise the read level past a lower, not yet committed id; the transfer queue then never read
that task (state stayed next_event_id=3 until WorkflowTimeoutTask 5 min later).
Go holds the shard lock across allocate -> persistence write -> updateMaxReadLevelLocked.

Evidence: failed runs 43111289965, 43111312855, 43114292959, 43114273772 (diag logs): stuck churn workflow
loaded with state=0/next_event_id=3, no DecisionTask transfer executed, WorkflowTimeoutTask 5 min later.

## Fix
`ShardContext::lock_task_writes()` (tokio mutex, owned guard) held in `engine_cache.rs` update path and
`engine_start.rs` create path from transfer-id allocation until the write and `update_max_read_level` finished.
Unit test: `shard_context::tests::task_write_gate_keeps_read_level_behind_uncommitted_ids`.
Not covered (follow-up): NDC hooks (`ndc_context_hooks.rs`), and `snapshot_of` callers for continue-as-new /
reset (engine_start.rs ~601, engine_decision.rs ~783) allocate ids outside the gate.

## Validation (head 9189015, CI only)
rust-ci cadence-history: run 43117232903 success.
history-mixed-gate grpc two_history=true, all success:
cassandra 43118220755 43118234547 43118247627 43118262818 43118277429 43118291327;
mysql 43118305395 43118318132 43118332100.
(Diagnostics were removed after; final head recorded in the git log.)
