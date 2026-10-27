# Matching standby completion error exhaustion (2026-10-27)

UNVERIFIED: no local Cargo, Go, rustfmt, build, or test commands were run. The parent owns GitHub Actions dispatch and validation.

## Go behavior mirrored

`go/service/matching/tasklist/task_completer_test.go`, `TestCompleteTaskIfStarted` cases:

- `error - could not get domain by ID from cache`: domain cache is called `retryPolicyMaxAttempts + 1` times (4 with the Go test setting), and completion returns `error-getting-domain-by-id`.
- `error - could not fetch workflow execution from history service`: History is called 4 times, and completion returns `error-describing-workflow-execution`.

Go creates a policy with a 100ms initial interval and three maximum retries in this test helper. The production task completer retains its separate 1s initial, 10s max interval, 300s expiration policy.

## Rust implementation and tests

- `matching_standby.rs` now has a private `retry_completion` runner used by the production checker. A policy and sleeper are injectable so tests execute the same retry loop without real delay.
- `cadence-common/backoff.rs` exposes `Retrier::next_backoff_at` as an elapsed-time seam while production `next_backoff` keeps using its real elapsed clock. `retry_completion` uses this seam with real elapsed time in production and a simulated elapsed clock in its test.
- `domain_and_history_errors_retry_then_return_original_error_once` runs the retry runner for both error messages, asserts four attempts, checks the original status message survives exhaustion, and confirms `STANDBY_TASKS_COMPLETION_FAILURE` is recorded exactly once per exhausted checker.
- The production retry policy values are untouched.

## Validation

Not run here per instruction. Parent should dispatch CI for `cadence-matching`, inspect formatting/build/test results, and record the Actions run URL, head SHA, and conclusion before treating this as validated.
