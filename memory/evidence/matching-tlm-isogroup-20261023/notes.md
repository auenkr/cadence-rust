# TLM tests 10 and 12 (2026-10-23)

## Test 12 (TestGetIsolationGroupForTask)
- The policy was already a pure function: `cadence_common::task_isolation::TaskIsolation::select`. The closure in `grpc_matching.rs` only resolves inputs (known groups, drain state, recent poller, partition groups), so no refactor was needed.
- Compared line by line with Go `getIsolationGroupForTask`: the order of checks (disabled/sticky/empty, drain error, drained, unknown, no poller, partition, duration) and the arithmetic match. No Rust fix needed.
- The Go table has 15 cases, not 13. All 15 are ported as `go_get_isolation_group_for_task_table` in `task_isolation.rs`.
- Known difference not covered by tests: Go does `BinarySearch` on a sorted group list for "known", Rust uses `contains`. Same result for sorted input.

## Test 10 (TestRateLimitErrorsFromTasklistDispatch)
- Go injects a `dispatchTask` hook returning `ErrTasklistThrottled` and checks that `dispatchSingleTaskFromBuffer` returns (false,false), i.e. retries. Rust has no such hook; its throttled outcome comes from the dispatch limiter (`WaitOutcome::Throttled` -> `Attempt::Throttled`, retry with yield, `BUFFER_THROTTLE_COUNT`).
- Ported as `matching_reader_retries_throttled_dispatch_without_acking` using a zero-rate, zero-burst limiter: asserts the throttle metric and that the task is not acked.

## CI
- Commit e278c5655, run https://github.com/auenkr/cadence-rust/actions/runs/40014990614 (workflow_dispatch, package=cadence-matching), conclusion success. Push run 40014980241 may have been cancelled by the concurrency group.
- Caveat: whether the cadence-common test was in the run's scope was not checked (package=cadence-matching only).
