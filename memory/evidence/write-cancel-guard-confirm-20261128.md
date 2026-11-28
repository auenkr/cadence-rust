# WriteCancelGuard confirmation (2026-11-28)

Code: `rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs` (guard), armed in `engine_cache.rs` (update, incl. new-run tasks) and `engine_start.rs` (create).

## Semantics vs Go
Go `execution/context.go` (`taskNotifier.on{Create,Update}WorkflowExecution`, `isNotifyTaskNeeded`) always runs the notifier after the persistence call, even when the caller ctx is cancelled; an ambiguous/ctx error notifies with `PersistenceError=true` (clears cached queue reader). Rust drops the request future on cancel, so the guard reproduces that: dropped while armed -> `notify_tasks_via(.., persistence_error=true)`; disarmed right after the write await (no await in between) so normal success/error paths notify exactly once. Matches Go. No bug found; no production code changed.

## Tests (real_engine_tests.rs `write_cancel_guard`, harness gained `create_pause`)
- dropped_update_write_notifies_with_persistence_error_and_loses_no_task: abort mid-write -> exactly one wake with persistence_error=true and task ids; nothing stored; retry stores the activity task once, notified without error.
- completed_update_write_notifies_once_without_error.
- dropped_create_write_notifies_with_persistence_error: abort mid-create; retry stores same tasks as a clean start (no dup/loss).
- timeout_cancellation_wakes_waiters: `tokio::time::timeout` cancellation.

## CI
Branch `write-cancel-guard-confirm`, rust-ci.yml package=cadence-history: run 46109973337 (https://github.com/auenkr/cadence-rust/actions/runs/46109973337) = success (clippy + test). Earlier run 46108622159 failed only on my wrong test assertion (start emits 2 transfer tasks), fixed.

Caveat: guard notifies even if the write actually never landed (harmless: spurious cache reset, same as Go ambiguous-error path).
