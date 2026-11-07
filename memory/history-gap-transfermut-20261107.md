# history-gap-transfermut (2026-11-07)

Branch `history-gap-transfermut`. Ports Go `transfer_active_task_executor.go` dependencies in
`rust/crates/cadence-history/src/wiring/assembly/queue_deps_transfer_active.rs`.

## Done
- New `engine_events_transfer.rs` (inherent `EngineMutableState` methods): child started, start-child-failed
  (WorkflowAlreadyRunning), external cancel-requested / signaled, request-cancel / signal external failed
  (UnknownExternalWorkflowExecution, decision id EMPTY_EVENT_ID), `decode_child_initiated_event`.
- `Inner.deleted_request_cancels` / `deleted_signals`; mutation fills `delete_request_cancel_infos` / `delete_signal_infos`;
  cleared in `finish_transaction`.
- `ev_completion_event` (real `get_completion_event`); `CloseEvent.full_event`.
- `record_child_execution_completed`, `child_execution_initiated_event`, `find_auto_reset_point` (bad binaries via domain cache),
  `current_version_history`, `get_child_execution_domain_id` (child domain id -> name lookup -> parent id),
  `advanced_visibility_writing_enabled`, child start delay/jitter/cron-overlap/first-run-at.
- `deps_ms.rs` untouched (avoids conflicts with history-gap-timermut).

## Remaining
- `send_parent_close_policy_request`, `reset_workflow` still return explicit errors.
- `allow_internal` always true; multi-host forwarding not done.

## CI evidence
- Run https://github.com/auenkr/cadence-rust/actions/runs/42343500500, head ff3f6fb74afa83f0039549074492cd8b9496684e
  (rust-ci, package=cadence-history): conclusion success.
- Earlier failures fixed: missing `StartChildRetryPolicy` import (run 42342731459), clippy needless_update (run 42343178609).
