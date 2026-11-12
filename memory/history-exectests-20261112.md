# history-exectests (2026-11-12)

Branch `history-exectests` (from master 08ed59d23). Rust unit tests for the History task executors, ported from the Go suites. CI only.

## Design
Rust executors are trait based, so the tests fake the trait boundaries instead of Go's real mutable state + mocked persistence/matching/history clients:
- timer: `TimerWorkflow` / `TimerActiveDependencies` fakes; assertions on exact call sequences.
- transfer: `ActiveMutableState`, `ActiveTransferContext`, `TransferActiveDependencies`, `TransferDependencies` fakes; assertions on RPC requests and mutable-state calls.

## Ported
- `rust/crates/cadence-history/src/task/timer_active_task_executor_tests.rs` (23 tests): user timer fire/noop/resurrected; activity timeout (no-retry, retry StartToClose/ScheduleToStart, stale attempt, heartbeat, resurrected); decision timeout (schedule-to-start normal/sticky, start-to-close, attempts); workflow backoff (retry/cron); activity retry timer (+cross-domain); workflow timeout (cancel requested, retry, cron).
- `rust/crates/cadence-history/src/task/transfer_active_task_executor_tests.rs` (34 tests): activity (success, empty tasklist fallback, duplicate/not running, rate limited); decision (normal, sticky, sticky-worker-unavailable fallback, duplicate, rate limited); close execution (reply to parent via RecordChildExecutionCompleted, no parent, running, continued-as-new, error handling, RecordChildExecutionCompleted-only / RecordWorkflowClosed-only tasks, parent close policy terminate/cancel/abandon, worker batching above threshold, noop-error handling); cancel (success, failure, already-requested, duplicate, self-cancel); signal (success + RemoveSignal, failure, remove-signal ignore, duplicate, self-signal); start child (success, already-started failure, other failure, child already started, abandon w/ closed parent, closed-parent noop, duplicate, schedule-decision completed ignored, deleted target domain).

## Bugs found
None. Rust matched Go in every tested path.

## Gaps (not ported)
- Standby executors (transfer_standby_task_executor_test.go, timer_standby_task_executor_test.go).
- Transfer active: RecordWorkflowStarted / UpsertWorkflowSearchAttributes (+ContextHeader) need `ShardContext::get_service()` domain-scope cache fake; ResetWorkflow_*; CopySearchAttributes; global-domain version-mismatch paths; ephemeral tasklist decision case.
- Timer: version-mismatch/global-domain paths.
- Test `load_none_is_noop_and_busy_mapped` only covers the None-load noop.

## CI runs
- Timer tests first run (clippy dead-code failure): https://github.com/auenkr/cadence-rust/actions/runs/43162302347
- Timer tests green: https://github.com/auenkr/cadence-rust/actions/runs/43162769728
- Transfer tests green, 667 cadence-history tests (head ac12bd59b): https://github.com/auenkr/cadence-rust/actions/runs/43163962461
No non-test Rust changed, so no mixed gate required.
