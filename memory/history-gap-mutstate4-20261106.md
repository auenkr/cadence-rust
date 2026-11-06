# history-gap-mutstate4 (2026-11-06)

Branch `history-gap-mutstate4`. Typed `TypedMutableState` event methods in
`rust/crates/cadence-history/src/execution/mutable_state_methods_*.rs`
(JSON facade untouched; `delete_user_timer` on the typed state now follows Go: never errors).

## Ported (Add*/Replicate*)
- common: checkMutability, createInternal/CallerError, ClearStickyness, UpdateWorkflowStateCloseStatus, event add helper, checkAndClearTimerFiredEvent.
- timer: GetPendingTimerInfos, GetUserTimerInfo, AddTimerStarted/Fired/Canceled/CancelTimerFailed, Replicate*, UpdateUserTimer (checked).
- timedout: AddTimeoutWorkflowEvent, ReplicateWorkflowExecutionTimedoutEvent.
- completed: IsWorkflowCompleted, AddCompleted/AddFail + Replicate*.
- cancellation: IsCancelRequested, GetRequestCancelInfo, AddWorkflowExecutionCancelRequested/Canceled, AddRequestCancelExternal{Initiated,Failed}, AddExternalWorkflowExecutionCancelRequested + Replicate*.

## Remaining gaps
- NOT ported (time): methods_{activity,child_workflow,signal,started,decision,semaphore}.
- All ported methods skip taskGenerator (GenerateWorkflowCloseTasks, RequestCancelExternal tasks), writeEventToCache, insertWorkflowRequest (strong idempotency), logging; GetCompletionEvent (events cache).
- Methods take an explicit `timestamp_unix_nanos` instead of the Go time source; log tag op strings are approximations.
- cadence-api lacks Semaphore attributes and RequestCancelActivityTaskFailed attributes.

## CI evidence
See final report / commit following this file for run URL and conclusion.

Run https://github.com/auenkr/cadence-rust/actions/runs/42234640207 (head 4b-code commit before this note), conclusion: FAILURE.
The only errors are in files not touched by this branch (pre-existing on origin/master): E0046 missing trait items in
wiring/assembly/engine_cache.rs (`update_workflow_execution_as_active`, `clear`) and engine_state.rs
(`add_workflow_execution_started_event`, `add_first_decision_task_scheduled`, `add_decision_task_scheduled_event`),
E0063 missing `failover_host`/`start_host` in wiring/assembly/engine_factory.rs. No errors reported in the new
mutable_state_methods_* files, but clippy/tests did not complete, so they are NOT verified green.
