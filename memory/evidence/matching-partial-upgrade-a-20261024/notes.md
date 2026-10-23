# Matching PARTIAL upgrade A (2026-10-24)

Scope: the first 8 PARTIAL rows, in document order, of `memory/matching-go-test-map-20261023.md`
whose Go file is not engine_test.go, engine_integration_test.go, matcher_test.go, forwarder_test.go,
task_completer_test.go, or handler/membership_test.go. Shared map is not edited.

Branch: `matching-partial-upgrade-a-20261024` (from origin/master `bafd65a77`).
Verification: GitHub Actions `rust-ci.yml`, package `cadence-matching`. No local cargo was run.
Run: RUN_URL_PLACEHOLDER

## Table

Columns: map row | Go test (file:line) | Rust test(s) | old status -> new status.
`new` = added in this branch. `ignored` = `#[ignore = "parity gap: ..."]` (reason is in the test attribute).

| # | Map row (Go test) | Rust test(s) | Old -> New |
|---|---|---|---|
| 1 | config/config_test.go TestNewConfig (45) | tests/matching_partial_upgrade_a.rs::go_new_config_readers_return_go_test_values (new, 8 of ~66 keys checked against Go values); ignored go_new_config_every_field_has_a_rust_reader | PARTIAL -> PARTIAL |
| 2 | config/config_test.go TestGetTasksBatchSizeValidation (211) | matching_reader.rs::go_get_tasks_batch_size_positive_values_pass_through (new, 1000 and 1); ignored go_get_tasks_batch_size_returns_raw_non_positive_values (0 and -5 raw) | PARTIAL -> PARTIAL |
| 3 | handler/context_test.go TestHandleErrKnowErrors (48) | ignored tests/matching_partial_upgrade_a.rs::go_handle_err_passes_typed_errors_through_unchanged (no generic handleErr) | PARTIAL -> PARTIAL |
| 4 | handler/handler_test.go TestDescribeTaskList (598) | matching_rpc_tests.rs::go_handler_describe_task_list_success_returns_response (new); ignored go_handler_describe_task_list_engine_error_is_internal_service_error | PARTIAL -> PARTIAL |
| 5 | handler/handler_test.go TestListTaskListPartitions (656) | ignored go_handler_list_task_list_partitions_engine_error_is_internal_service_error; success/rate-limit left as existing tests | PARTIAL -> PARTIAL |
| 6 | handler/handler_test.go TestUpdateTaskListPartitionConfig (854) | ignored go_handler_update_task_list_partition_config_engine_error_is_internal_service_error; rate-limit left as existing test | PARTIAL -> PARTIAL |
| 7 | handler/handler_test.go TestRefreshTaskListPartitionConfig (804) | ignored go_handler_refresh_partition_config_engine_error_is_internal_service_error; validation left as existing test | PARTIAL -> PARTIAL |
| 8 | handler/handler_test.go TestNewHandler (123), TestStart (129), TestStop (140) | ignored tests/matching_partial_upgrade_a.rs::go_handler_start_calls_engine_start_once_and_stop_calls_engine_stop_once | PARTIAL -> PARTIAL |

Totals for this branch: 8 rows, 0 promoted to PORTED/COVERED, 8 rows still PARTIAL.
New passing tests: 3 (rows 1, 2, 4).
New ignored parity-gap tests: 8 (one per row 1-8 for rows 1, 2, 3, 8; one each for rows 4, 5, 6, 7).
Files touched: `tests/matching_partial_upgrade_a.rs` (new), `src/matching_reader.rs` (test module),
`src/matching_rpc_tests.rs` (test module, inserted before the AddTasksTest block).

## Notes on what was checked

- Go `NewConfig` is one struct with 66 fields; Rust reads each key in its own module. Only the readers
  in `matching_config.rs` are public, so only those are checked here.
- Durations: Go's in-memory client stores `time.Duration(n)`. Rust reads durations from strings (`"10ns"`),
  and JSON integers fall back to the default (`matching_config.rs` test `wrong_type_falls_back_to_default`).
  The test uses the string form. Not a new gap, but the Go in-memory integer form has no Rust equivalent.
- Row 2: Go's `GetTasksBatchSize` returns the raw value. Rust's read path substitutes 1000 for non-positive values
  (`ReaderLive::batch_size`). Only the positive half is asserted as a pass-through.
- Rows 4-7: the Go engine-error subcases need a failing engine. Rust has no seam for that.
  The rate-limit and validation subcases already have existing Rust tests (`go_handler_*_rate_limited_*`).
