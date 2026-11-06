# history-gap-mutstate3 (2026-11-06)

New typed Go-shaped module: `rust/crates/cadence-history/src/execution/mutable_state_builder.rs`
(`TypedMutableState`, `TypedWorkflowMutation`, `TypedWorkflowSnapshot`). The JSON facade
`crate::mutable_state::MutableStateBuilder` is untouched so existing users keep compiling.

Ported from Go `mutable_state_builder.go`: newMutableStateBuilder defaults, Load (index rebuild),
version getters (current/last-write/start), state predicates, shouldBufferEvent,
CreateNewHistoryEvent (BufferedEventID), FlushBufferedEvents, reorderAndFilterDuplicateEvents,
started-id patching, trimEventsAfterWorkflowClose, cleanupTransaction,
CloseTransactionAsMutation (Rewrite* on deletes) / AsSnapshot, Update*/Delete* tracking for
activity/timer/child/cancel/signal/signalRequested.

Remaining gaps (not yet ported, honest list):
- mutable_state_builder_methods_{activity,timer,child_workflow,cancellation,signal,completed,started,timedout,decision,semaphore}.go Add*/Replicate* event methods not yet on the typed state (existing facade has some).
- Task generation on close (mutable_state_task_generator wiring), checksum generation, events-cache.
- cadence-api lacks Semaphore event attributes and RequestCancelActivityTaskFailed attributes.
- Started-id patching only covers activity completions; child-workflow patching and attempt in the duplicate key are simplified.
- Consumers (state_rebuilder, repairer, integrity, cache/context) not migrated yet.
