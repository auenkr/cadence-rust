# History NDC two-run / conflict-resolve / active persistence — 2026-11-11

Branch: `history-ndcpersist3` (base `9120a28ea`). Go source: `execution/context.go`, `ndc/*`.

## Done

- `NdcWorkflow::update_workflow_execution_with_new` with a new run (continued-as-new): both states are lent (`lend_state`) to `NdcWriter::write_update_with_new` -> generic `WorkflowContext` writes both histories, then one atomic update (mutation + new-run snapshot + current update). Success restores both states; failure clears both (Go).
- `update_workflow_execution_with_new_as_passive`, `update_workflow_execution_as_active` (UpdateCurrent / Active / mode New) implemented.
- `conflict_resolve_workflow_execution` via `NdcWriter::write_conflict_resolve` (reset state + optional new run + optional current-run mutation).
- Hooks: `strong_idempotency_check` reads `EnableStrongIdempotencySanityCheck` (new `EngineCore.strong_idempotency_sanity_check`); `notify_history` builds the Go notification and calls the engine (`EngineCore::notify_history_event`); `merge_continue_as_new_replication_tasks` ports the Go early-return conditions; task-id max read level is now max over both images.
- Tests (fake writer with CAS): CAN writes + reloads both runs; failed CAN writes nothing and recovers; conflict-resolve replaces state; conflict-resolve CAS mismatch recoverable.

## Gaps (explicit errors)

- `merge_continue_as_new_replication_tasks`: when the closing run has replication tasks, explicit error (replication task allocation/encoding is not ported; `allocate_task_ids` already rejects non transfer/timer categories).
- `request_type` = -1: the Rust `WorkflowRequest` carries no type/version, nothing to decode (validation fails loudly if requests appear).
- `flush_as_active`, `verify_and_repair`, `reapply_events` (workflow + signal reapplication), load with task version, `BypassCurrent` event reapply: explicit errors (item 5 not done).
- `SqlWorkflowContextStore` real writes and the `GoExecutionStore` atomic two-run SQL write are not unit-tested; need mixed Go/Rust gate.

## CI

- Run URL: https://github.com/auenkr/cadence-rust/actions/runs/42997350580
- Code head SHA: c6215026d8b88c37077d9fc4526c54551918f1fa
- Conclusion: success (clippy + cadence-history tests)
