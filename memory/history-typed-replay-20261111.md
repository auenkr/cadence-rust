# Typed replay dispatch: start, decision, activity — 2026-11-11

## Status

Implemented and pushed on branch `history-typed-replay`. The typed replay code head is `04c58d462403881d3472f44a56d0694d5008dec2` (based on `efbc0ea401e00c04075d32e36d892e63bc50a978`). Existing legacy `StateBuilder::apply_events` and its callers remain unchanged.

## Done

- Added `StateBuilder::apply_events_typed` for `TypedMutableState`, accepting domain ID, request ID, workflow/run identity, and a history batch.
- It updates incoming event version history and task ID metadata, applies the supported start, decision and activity `Replicate*` operations, preserves the first event ID as the activity batch ID, and sets batch boundary fields. Incoming events are not appended or assigned new event IDs.
- Dispatches only `WorkflowExecutionStarted`; DecisionTask Scheduled/Started/Completed/TimedOut/Failed; ActivityTask Scheduled/Started/Completed/Failed/TimedOut/CancelRequested/Canceled; and `RequestCancelActivityTaskFailed`. Other events return an explicit “not yet supported in typed replay” error.
- Suppresses active task generation in the typed replication paths while preserving passive state transitions, including transient decision retry state. The default remains unchanged for existing non-replay typed callers.
- Added tests for multi-batch start/decision timeout retry, activity schedule/start/failure/retry, first-batch IDs, pending activity indexes, decision attempt, event task IDs, no appended history, no generated tasks, and unsupported event errors.

## Gaps

- Timer, child, cancellation, signal, marker, close and semaphore event families remain unsupported in typed replay. This is the intended boundary for plan item 3a; see item 3b in `history-mutstate-unification-plan-20261109.md`.
- The typed entry point is not wired into the NDC builder or a production caller yet.
- Parent domain-name resolution for old start events is not represented by this entry point; it uses the event's `parent_workflow_domain_id` when present.
- CI validates the crate, not the mixed Go/Rust cluster or full History service parity. No service completion claim is made.

## CI evidence

- Command: `gh workflow run rust-ci.yml --ref history-typed-replay -f package=cadence-history`
- Workflow: https://github.com/auenkr/cadence-rust/actions/runs/42945892534
- Head SHA: `04c58d462403881d3472f44a56d0694d5008dec2`
- Conclusion: success; Format check, Clippy, Formatting and Test all succeeded.
