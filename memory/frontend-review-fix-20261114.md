# Frontend review fix (branch frontend-review-fix, 2026-11-14)

Source: memory/frontend-final-review-opus-20261114.md. No cargo/go/docker was run locally.

| Finding | Fix |
|---|---|
| HIGH-1/2, MEDIUM-1/2, LOW-2 (validators on Register/Update/Deprecate/Delete/Failover/CreateSemaphore) | 270d9ad01: `DomainValidator` seam on `WorkflowApi` (service-core `workflow_api.rs`), called from `yarpc.rs` (Thrift) and `handlers.rs` (gRPC/JSON); `cadence-frontend/src/domain_guard.rs` runs the real `RequestValidator` (retention, required keys, token/admin, lockdown, failover timeout, semaphore checks in Go order). Test: `yarpc::schedule_dispatch_tests::domain_rpcs_run_installed_request_validator_on_live_path`. |
| HIGH-3 concurrent graceful failover check | 270d9ad01: `check_ongoing_failover` in `domain_guard.rs` (both Go messages, metrics `graceful_failover_initiation_*`). Tests: `domain_guard::tests::ongoing_failover_matches_go`, `yarpc::...graceful_failover_runs_ongoing_failover_check_on_live_path`. |
| HIGH-4 UpdateTaskListPartitionConfig | ca67dbbe0: Go-order validation `validate_partition_config_request` in `cadence-matching/src/grpc_admin.rs` (handler already existed there; IDL has no Thrift method). Test: `partition_config_validation_matches_go_order`. |
| HIGH-5 CountDLQMessages | Handler already existed in `grpc_admin.rs` with Go order (domain count, then history); a history error is returned as the original tonic Status, so no code change. No dedicated new test. |
| LOW-1 typed History errors in admin forwarding | NOT changed: `?` already forwards the History tonic Status unchanged in the CountDLQ path; `handler_admin_history.rs` `internal(status)` mapping left as is. Open item. |

Note: `InternalServiceError` is not declared for UpdateDomain etc. in the Thrift IDL, so a guard Internal error on those methods fails Thrift encoding (same as other existing internal errors there).

## CI evidence
- rust-ci service-core (head 777732d12): https://github.com/auenkr/cadence-rust/actions/runs/43592919759 success
- rust-ci frontend (pre-test-tweak head ce522c068): https://github.com/auenkr/cadence-rust/actions/runs/43592258373 success
- rust-ci matching (ce522c068): https://github.com/auenkr/cadence-rust/actions/runs/43592261561 success
- Frontend mixed gate mysql (head 777732d12): https://github.com/auenkr/cadence-rust/actions/runs/43592917059 success, log `GATE SUMMARY failed: []`
