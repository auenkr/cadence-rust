# history-ndctests (2026-11-11)

Branch `history-ndctests`.

## Task 1: replication task notifier
Go `task_notifier.go` notifies only transfer and timer task categories; replication is not notified. No Rust change needed.

## Task 2: ported Go tests (cadence-history, all passing)
- `ndc/test_support.rs` (new, cfg(test)): scriptable MockMs / MockWorkflow / MockEnv replacing gomock.
- `ndc/events_reapplier.rs`: events_reapplier_test.go cases (applied, noop, partial, error, closed workflow).
- `ndc/transaction_manager.rs`: backfill current active/passive open/closed, skip-reset cases, get_reapplicable_events, not-current, check_workflow_exists, get_workflow_current_run_id.
- `ndc/workflow_resetter.rs`: reset_workflow no-error / error.
- `reset/tests.rs` (new): resetter_test.go cases. Deviation: ported against `reset/` (where the logic lives), not the engine_resetter.rs adapter.

## Bugs found
None. Ported tests exposed no behavior differences from Go. Only CI fixes were a wrong import path (`cadence_api::tasks::WorkflowExecutionContinuedAsNewEventAttributes`) and clippy type_complexity allows in tests.

## CI
- Run 43056744825 success (f73a8a615), 43059031232 success on head ae14278b3 (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43059031232
- Intermediate failures 43056958653/43057086227/43057346139/43058273346 were the compile/clippy issues above.

## Remaining
- Go `new_workflow_transaction_manager_test.go` and `existing_workflow_transaction_manager_test.go` not ported (sub-managers take `&dyn NdcTransactionManager`; needs a mock tm + workflow mocks with more MockMs methods). TestCreateWorkflow/TestUpdateWorkflow likewise not directly portable.
