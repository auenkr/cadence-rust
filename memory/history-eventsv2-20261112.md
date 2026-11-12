# History events v2 tests (branch history-eventsv2, 2026-11-12)

## Harness (`wiring/assembly/real_engine_harness.rs`)
- `MemExecutions::conflict_resolve_workflow_execution` implemented per Go `sql_execution_store.go:551-680` (BypassCurrent / UpdateCurrent, previous-run assertion, reset replace, optional current mutation + new snapshot). Known flaw: `set_current` runs before later condition checks.
- Fault injection: `Inner::create_errors` / `update_errors` (VecDeque) emulate gomock `.Return(nil, err)`; `create_calls` records (mode, previous run, previous LWV).
- `Harness::domain_status` (AtomicI32; 1 = deprecated) for deprecated-domain tests.

## Bug found and fixed (non-test code)
- Cassandra store never produced `DuplicateRequestError` (returned ConditionFailed). Added `PersistenceError::DuplicateRequest{request_type, run_id}`, produced in `history_execution_cassandra_write.rs::cas_error` from the request row (`row_type - ROW_TYPE_REQUEST_START`, `current_run_id`), mapped to `HistoryError::DuplicateRequest` in `engine_ndc.rs::to_history_error`.

## Tests (`real_engine_v2_tests.rs`, ports of history_engine2/engine3 tests)
Start: brand_new, brand_new_duplicate_request_error, _type_mismatch, still_running_dedup / non_dedup, not_running_prev_success (+duplicate_request_error, +type_mismatch), prev_success/prev_fail reuse-policy matrices. Deprecated domain: start, signal-with-start, signal. Signal-with-start: not_exist (+duplicate, +type mismatch), create_timeout, just_signal (+duplicate). RequestCancel: success, duplicate_request_error, already_cancelled same/different request id, completed fails.
Go note: SWS accepts Start- and Signal-type duplicates on create; mismatch test uses Cancel type.

Not ported: RecordDecisionTaskStarted*/RecordActivityTaskStarted* variants, marker decision, terminate, SWS_NotRunning / Start_DuplicateRequests variants, direct conflict_resolve unit test, Cassandra cas_error unit test.
NDC Corrupted: Rust already has WORKFLOW_STATE_CORRUPTED handling (mutable_state_builder, start policy); no gap found.

## CI
- rust-ci cadence-history green: https://github.com/auenkr/cadence-rust/actions/runs/43161812699
- Mixed gates (GATE SUMMARY failed: []): mysql/grpc/true 43160566582, cassandra/grpc/true 43160570183, postgres/tchannel/false 43160573894 (run on the non-test change; later commits are test-only).
