# matching_rpc_reports_arrival_qps_by_isolation_group flake (2026-10-24)

## Failure

- GitHub Actions run `40032734549` (branch `matching-engine-tests5-20261024`, head `8e0f230f7`), job `clippy-test`, `cargo test -p cadence-matching --lib`.
- Panic: `crates/cadence-matching/src/matching_rpc_tests.rs:2094:5`, `assertion left == right failed, left: 20.0, right: 0.0`.
- In that commit's file, line 2094 is `assert_eq!(queue.qps.qps(), 0.0)` in the test's open-window check.
- The test passed on rerun of the same code.

## Cause

- The test sets `matching.qpsTrackerInterval` to `50ms` for the `queue` task list and asserts that the open window is still invisible after five `add_decision_task` calls.
- The production tracker (`cadence-common/src/qps.rs`, `EmaQpsTracker`) uses the wall clock (`system_clock()` from `matching_registry.rs`), so windows are aligned to the tracker's creation time, which is the first add that creates the queue.
- If the five adds span a window boundary, the first window folds one arrival: `1 / 0.05s = 20.0`. This matches the logged value exactly.
- So the failure is a window-boundary race in the test: a 50ms window on a loaded CI runner is shorter than the adds plus the surrounding work.
- The production behavior is correct. The tracker reports only completed windows, which is the Go semantics the assertion encodes.

## Fix (test-only)

- The window is now `2s` (`b"\"2s\""`). The adds, which take milliseconds, always land in the first window.
- The open-window check reads `qps()` and `group_qps("z")`, then asserts `opened.elapsed() < window`, where `opened` is taken before the first add. Since the first window starts no earlier than `opened`, reads before `opened + window` must see zero. The guard runs after the reads so the reasoning holds.
- The fixed `sleep(80ms)` is replaced by a bounded poll (10s timeout) that waits until the root and group `qps` are positive. Windows close on the wall clock, so the poll does not depend on how long the test takes.
- The activity tracker assertion (default 10s window) now runs after the poll, so it is checked after the root window has closed.
- No production code changed. `git diff origin/master --stat` shows only `rust/crates/cadence-matching/src/matching_rpc_tests.rs`.

## Other tests checked

- `matching_rpc_forwarded_add_never_persists_and_reports_remote_failure` and `matching_rpc_native_partition_cache_controls_dispatch_rate_and_drained_admission` assert zero QPS on the default 10s window, which is safe for test runtime under 10s.
- `matching_rpc_reports_load_balancer_hints_only_for_old_task_lists` uses a 50ms window but sleeps 80ms before its positive check, so the window has closed by then. It is not affected by the race.

## Residual risk

- The open-window check still assumes the adds plus reads finish within 2s of `opened`. If the host stalls for more than 2s there, the guard fails with an explicit message rather than a misleading count mismatch.
- The tracker uses `SystemTime` while the guard uses `Instant`. This only matters if the wall clock jumps by more than the window during the test.

## Verification

Each run was `gh workflow run rust-ci.yml --ref matching-flaky-qps-20261024 -f package=cadence-matching`, run sequentially. The target test is logged as `... ok` in each run, and the crate shows `600 passed; 0 failed`.

1. https://github.com/auenkr/cadence-rust/actions/runs/40033840830 — success
2. https://github.com/auenkr/cadence-rust/actions/runs/40034081108 — success
3. https://github.com/auenkr/cadence-rust/actions/runs/40034350773 — success
