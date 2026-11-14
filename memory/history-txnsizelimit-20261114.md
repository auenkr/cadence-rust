# history-txnsizelimit (2026-11-14), branch `history-txnsizelimit`

## Go refs
- common/persistence/history_manager.go L263-271: `AppendHistoryNodes` returns `TransactionSizeLimitError{"transaction size of %v bytes exceeds limit of %v bytes"}` when serialized batch > `TransactionSizeLimit()`.
- Dynamic config `system.transactionSizeLimit`, default 14680064 (dynamicproperties/constants.go L3620; constants.DefaultTransactionSizeLimit = 14*1024*1024).
- decision/handler.go L574-602: on that error from the update, reload mutable state, `TerminateWorkflow(reason TRANSACTION_SIZE_EXCEEDS_LIMIT, details = err text, identity history-service)`, `UpdateWorkflowExecutionAsActive`, then return the original error.
- history/handler/handler.go L2151: handler maps it to `BadRequestError`.

## Rust changes
- `PersistenceError::TransactionSizeLimit` (cadence-persistence history_contract.rs), `HistoryError::TransactionSizeLimit` (engine/deps.rs), mapped in `to_history_error`.
- `SqlHistoryManager::append_history_nodes` (wiring/persistence/engine_ndc.rs) enforces the limit via `transaction_size_limit` closure (default 14 MiB; `with_transaction_size_limit`).
- `Config.transaction_size_limit` (`system.transactionSizeLimit`, 14680064), `EngineCore.transaction_size_limit`, used by the update-path history manager sites in engine_cache.rs.
- Real engine path `completed_run` in wiring/assembly/engine_decision.rs: terminate branch, then returns BadRequest with the original message.
- Not wired: other `SqlHistoryManager::new` sites (start/resetter/ndc) use the default limit constant, not the dynamic key.

## Tests (real_engine_tests.rs, harness gained `txn_size_limit` and `build_with_config`)
- respond_decision_completed_transaction_size_limit_terminates_workflow
- respond_decision_completed_attempts_exceed_limit_terminates_workflow (DECISION_ATTEMPTS_EXCEEDS_LIMIT)
- respond_decision_completed_closing_workflow_returns_empty_response
- respond_decision_completed_new_decision_has_sticky_enabled
- record_decision_started_on_closed_workflow_not_running_message

## CI (head SHA 87689c4b02537ec6a74252912e7cf9a19438314a)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43515433130 success
- history-mixed-gate two_history=true, all success with `GATE SUMMARY failed: []`:
  mysql/grpc 43516560975, mysql/tchannel 43516575290, postgres/grpc 43516590297, postgres/tchannel 43516604684, cassandra/grpc 43516619039, cassandra/tchannel 43516633359
  (https://github.com/auenkr/cadence-rust/actions/runs/<id>)
