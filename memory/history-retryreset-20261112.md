# history-retryreset (2026-11-12)

Audit items P1-1 and P1-2 of history-parity-audit-20261112.md.

## P1-1 timer-active workflow retry / cron
- `engine_timer.rs`: `EngineMutableState::retry_workflow_new_run` (Go executeWorkflowTimeoutTask attrs + retryWorkflow: force-close in-flight decision, `add_continue_as_new_event`) and `force_close_in_flight_decision` (also now called by `timeout_workflow`, as Go `timeoutWorkflow`).
- `queue_deps_timer_active.rs`: `retry_workflow` stores the new run; `update_workflow_execution_with_new_as_active` persists via new `EngineExecutionContext::update_with_new_run` (holds `lock_task_writes`; the ContinueAsNew decision path now delegates to it).
- Tests: `engine_timer::retry_workflow_tests` (retry initiator/attempt/backoff, cron initiator, in-flight decision force-closed, missing start event).

## P1-2 transfer-active auto-reset
- `queue_deps_transfer_active.rs::reset_workflow` downcasts the engine context/state (new `as_any` default hooks on `ActiveTransferContext`/`ActiveMutableState`) and calls `EngineWorkflowResetter`. BadRequest stays `TaskError::BadRequest` (executor treats as non-retryable). Test: `reset_err_tests`.

## Gate coverage
The history-mixed-gate has no workflow-timeout-with-retry-policy or bad-binary auto-reset scenario (gate not edited); coverage is unit tests plus regression of existing scenarios.

## CI (head 895355874914e59336337c7f615930fa35d4aed8, = merged with origin/master f765905bd)
- rust-ci cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43125856677
- gate mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43126804273 `GATE SUMMARY failed: []`
- gate cassandra/grpc/true: .../runs/43126817972 `failed: []`
- gate postgres/grpc/false: .../runs/43126831446 `failed: []`
- gate mysql/tchannel/false: .../runs/43126846307 `failed: []`
