# history-gap-execctx2 checklist (branch history-gap-execctx2)
Status: PARTIAL. Only history_builder.go is ported.
Green CI: https://github.com/auenkr/cadence-rust/actions/runs/41928431741 (clippy+test, cadence-history), conclusion success, head = tip of origin/history-gap-execctx2 after "history_builder: clippy fixes".

## history_builder.go -> execution/history_builder.rs
Done: every Add*Event with Go signature (started, activity scheduled/started/timed-out/canceled, continued-as-new, timer fired/canceled/cancel-failed, cancel requested/canceled, request-cancel-external x3, signal-external x3, upsert search attrs, marker, signaled, start-child + 6 child events).
GAP (blocked on cadence-api): AddRequestCancelActivityTaskFailedEvent and AddSemaphore{AcquireInitiated,Acquired,Released}Event. cadence-api has no attribute structs or HistoryEvent fields for them. Also Rust WorkflowExecutionStartedEventAttributes lacks ParentWorkflowDomainID, PrevAutoResetPoints, FirstScheduleTime-as-time, PartitionConfig/ActiveClusterSelectionPolicy mapping gaps; add_workflow_execution_started takes previous_run_id only (no reset points).
Note: Go uses msBuilder.CreateNewHistoryEvent; Rust HistoryBuilder owns id/version/timestamp.

## NOT started (still facades, see history-gap-execctx-20261105.md)
state_builder.go (581), state_rebuilder.go, workflow_repairer.go, integrity.go, cache.go, context.go (notifyTasks*, mergeContinueAsNewReplicationTasks, retry wrappers, emitSessionUpdateStats), events/cache.go, events/notifier.go.
