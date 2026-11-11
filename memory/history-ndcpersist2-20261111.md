# History NDC single-run create/update persistence — 2026-11-11

Branch: `history-ndcpersist2` (local worktree branch `ndcp2-work`)
Base: `0232320d1` (`origin/master`)
Go source: `go/service/history/execution/context.go`

## Done

- `wiring/assembly/ndc_context_hooks.rs` (new): `EngineNdcContextHooks` (concrete `WorkflowContextHooks` for `SqlWorkflowContextStore`), `NdcStateBox` (`TransactionState` over the dyn NDC state, `Error = HistoryError` so condition-failed/timeout/ownership-lost survive), `NdcWriter` seam + `EngineNdcWriter`.
- `wiring/persistence/engine_ndc.rs`: hooks gained `allocate_task_ids`, `tasks_committed`, `write_failed`. `create_with_retry`/`update_with_retry` allocate transfer/timer ids (Go `allocateTransferIDs`/`allocateTimerIDs`) before the write; after a durable write they advance max read level and notify queues; on failure the pending allocation is dropped and `ShardOwnershipLost` closes the shard.
- `NdcMutableState::close_transaction_as_mutation` added (default explicit error; `OwnedNdcMutableState` delegates). `transaction_committed` now also moves the update condition to the new `next_event_id` (Go `cleanupTransaction`).
- `EngineNdcWorkflow`: `create_workflow_execution`, `update_workflow_execution_as_passive` and `update_workflow_execution_with_new` (new == nil) now persist. Order: prepare -> write -> cleanup only after commit; queues notified only after success. Success drops the engine cache snapshot (`ctx.clear()`); any failure clears the workflow and the next load rebuilds from the DB.
- Tests (fake in-memory writer with CAS on `next_event_id`): create + reload, passive update + reload, CAS mismatch returns the condition-failed error, DB untouched, state recoverable on reload.

## Gaps (explicit errors or no-ops; not parity evidence)

- Explicit errors: continued-as-new two-run writes, `update_workflow_execution_as_active`, `with_new_as_passive`, conflict-resolve, `reapply_events` on the workflow, signal reapplication in the hook, `verify_and_repair`, `flush_as_active`, task categories other than transfer/timer, load with incoming task version.
- No-ops/defaults: `notify_history` (no history notifier wiring), stats hooks, `strong_idempotency_check` = false (config not reachable from `EngineCore`), `request_type` = -1 (no `WorkflowRequest` blob decoder).
- The real `SqlWorkflowContextStore` task allocation and `GoExecutionStore` writes are not unit-tested (44-method store fake not written); only the workflow level is covered. Needs the mixed Go/Rust gate.
- Persistence errors reach the NDC layer as `NdcError::Other(message)` (typed `ConditionFailed` is not preserved past `history_to_ndc`), except entity-not-exists and already-started.

## CI

- Run URL: https://github.com/auenkr/cadence-rust/actions/runs/42994928468
- Code head SHA: `252245ef8edc1e5fe4f5f403a40b70480103b30e`
- Conclusion: success (rust-ci.yml, `package=cadence-history`: clippy + tests). Two earlier runs failed only on a rustdoc lint in the new file's header, fixed.
