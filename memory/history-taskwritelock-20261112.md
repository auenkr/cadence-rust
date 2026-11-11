# history-taskwritelock (2026-11-12)

Follow-up to history-restartsig: every task-id allocation path now holds `ShardContext::lock_task_writes()`
from allocation until the DB write and `update_max_read_level` finish (Go shard lock).

## Paths covered
- engine_cache.rs `persist` (update) and engine_start.rs create: already held (master). `persist` now accepts a
  caller-held gate (`held_gate`) so no nested acquisition.
- Continue-as-new: engine_decision.rs `update_with_new_run` and engine_start.rs
  `update_workflow_execution_with_new_as_active`: gate taken before `snapshot_of`, passed to
  `update_with_new_as_active(.., &gate)`, then `update_max_read_level(new run max)` before release.
- Resetter (engine_resetter.rs `persist`): gate held across hook allocation, update/create, and `tasks_committed`.
- NDC hooks (`EngineNdcContextHooks`): new `WorkflowContextHooks::begin_task_write()` (async, default no-op)
  acquires the gate; released in `tasks_committed` (after read-level update) / `write_failed`. Called from
  `create_with_retry`, `update_with_retry` and `conflict_resolve` in persistence/engine_ndc.rs.
- Conflict-resolve: previously allocated no task ids (tasks written with id 0); now allocates for reset/new/current
  as Go's ConflictResolveWorkflowExecution does, under the gate.
- Event-batch ids (`generate_task_ids` for history nodes) stay outside the gate; they never feed the read level.

Tests: existing `shard_context::tests::task_write_gate_keeps_read_level_behind_uncommitted_ids`; no new unit test
(EngineCore is not constructible in isolation); coverage is rust-ci plus the gates below.

## Validation (code head dc50d1f8d, CI only)
rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43121153020 success.
history-mixed-gate, all `GATE SUMMARY failed: []`:
- cassandra/grpc/two_history: 43122076312, 43122080458, 43122084492
- mysql/grpc/two_history: 43122088622, 43122093152
- mysql/tchannel/false: 43122097382
- postgres/grpc/false: 43122101702
(base https://github.com/auenkr/cadence-rust/actions/runs/<id> for each)
