# History gap: queue executor dependency adapters (branch `history-gap-queuedeps`, 2026-11-07)

Goal: replace `MissingExecutorsProvider` (queue_factory.rs) by a real `QueueExecProvider`, which needs real
implementations of the task executor dependency traits. See history-gap-queuefactory-20261107.md for context.

## Real (done on this branch)
- `HistoryEnv` now has `visibility: Arc<SqlVisibilityWriter>` (shared SQL backend) and `matching_resolver`.
- `ShardBuildCtx` now carries the shard's `core: Arc<EngineCore>` and `cache: Arc<EngineExecutionCache>`.
  `engine_factory::build_core(env, shard)` builds them before the queues; `build_engine(env, shard, core, cache, queues, repl)`
  reuses them, so queue executors and the engine share ONE execution cache (same per-workflow locks).
- `wiring/assembly/queue_deps.rs`:
  - `TransferDepsImpl` (`task::TransferDependencies`): matching `AddActivityTask` / `AddDecisionTask` over tonic
    (source = HISTORY), visibility record started/closed via `SqlVisibilityWriter` (memo = ThriftRW blob, close status
    persistence->thrift numbering), uninitialized = no-op and upsert = not supported (both exactly as Go's SQL visibility
    store), config accessors, `SqlTaskDlqWriter` (SQL: not supported, as in Go).
  - `CleanupDepsImpl` (`task::CleanupDependencies`): `get_workflow` over the real cache/context/mutable state
    (`EngineCleanupWorkflow`), delete execution / current execution / active-cluster-selection-policy / history branch /
    visibility via the SQL stores, `complete_history_task` per key.
- `engine_ndc::branch_request` is `pub(crate)`.

## Not done (explicit errors, TODO(link))
- `archive` / `archive_visibility` return an error (archiver client not ported). `history_archival_enabled` follows the domain
  flag, so history is NOT silently deleted for archival-enabled domains (task keeps retrying).
- `fetch_workflow_timer_tasks_for_cleanup` returns empty (only used with `system.enableWorkflowTimerTaskCleanup`, default false).
- `MissingExecutorsProvider` is still what `launcher.rs` uses: no `QueueExecProvider` exists yet because the Active/Standby
  traits are not implemented. Transfer/timer tasks are still NOT processed by the Rust History service.

## Remaining tasks (small, for simple workers; each = one trait impl in its own file under wiring/assembly/, CI-validated)
1. `TransferStandbyDependencies` (task/transfer_standby_task_executor.rs, 14 fns) + `TransferWorkflow` adapter over
   `EngineMutableState` (16 fns, `DecisionState`). Smallest of the four: do first.
2. `TimerStandbyDependencies` (task/timer_standby_task_executor.rs, 15 fns) + `StandbyTimerWorkflow`/`TimerWorkflow`
   adapter (shares `TimerWorkflow` with 4).
3. `TimerActiveDependencies` (task/timer_active_task_executor.rs, 11 fns) + `TimerWorkflow` adapter (37 fns) over
   `EngineMutableState` + `EngineExecutionContext` updates (needs `update_as_active`).
4. `TransferActiveDependencies` (task/transfer_active_task_executor.rs, ~25 fns): history.Client calls
   (RecordChildExecutionCompleted, RequestCancel, Signal, RemoveSignalMutableState, Start, ScheduleDecisionTask, Terminate)
   should call the in-process `HistoryEngine` via `ShardBuildCtx.engine_slot` or the history gRPC client;
   `ActiveTransferContext` / `ActiveMutableState` (25 fns) adapters; parent-close-policy client, workflow resetter
   (both can return a TODO(link) error first); `get_current_execution` via `GoExecutionStore::get_current_execution`.
5. `DepsQueueExecProvider` (implements `QueueExecProvider`): builds `QueueExecCommon` + `CategoryExecutors` for
   transfer/timer from items 1-4 using `queue_exec.rs` builders; wire in `launcher.rs` instead of `MissingExecutorsProvider`.
6. Archiver client (history + visibility archival) for `CleanupDepsImpl::archive` / `TransferDepsImpl::archive_visibility`.
7. Known gap from queue_exec.rs: `SqlHistoryTask` rows only carry identity fields (executors need task-type specific fields).

## Evidence
- CI run / head SHA / conclusion: see bottom (filled after the run).
- rust-ci.yml package=cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/42338723722 head 0db35e6b5387e47956ab64c3c8e041c18fa2e517 conclusion: success (clippy + tests).

## Update: branch `history-gap-queuestandby` (2026-11-07)
- Items 1 and 2 done in `wiring/assembly/queue_deps_standby.rs` (not wired into launcher.rs):
  - `TransferStandbyDepsImpl` (`TransferStandbyDependencies`) + `EngineTransferWorkflow` (`TransferWorkflow` over `EngineMutableState`,
    Go `loadMutableState` stale-cache refresh, visibility record for start/upsert/close, `getWorkflowExecutionStatus`), resend via
    `HistoryResender` (resender.rs), current time via `ShardContext::get_current_time`, DLQ via `SqlTaskDlqWriter`.
    Deviations: request-cancel/signal infos come from the loaded snapshot; `advanced_visibility_writing_enabled` = false (advanced
    visibility mode not in Rust config); Go's data-inconsistency log on a still-stale state is not emitted (task skipped).
  - `TimerStandbyDepsImpl` (`TimerStandbyDependencies`): config/clock/resend/DLQ real; `load_workflow` returns an explicit
    TODO(link) error because `EngineMutableState` has no user-timer infos, timer sequence or `update_as_passive`. This is the same
    missing piece as item 3 (`TimerActiveDependencies`): implement timer state in engine_state.rs first, then add `StandbyTimerWorkflow`.
- CI rust-ci.yml package=cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/42340036208 head 7e96479b843d851e78ba4299cca0209ef7b0a3d0 conclusion: success (code head; the memory-note commit only touches memory/).
