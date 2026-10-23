# TestRatelimitBehavior port: subcase matrix (2026-10-24)

Go source: `go/service/matching/tasklist/matcher_test.go:1264` (`TestRatelimitBehavior`).
Rust target: `rust/crates/cadence-matching/src/matching_limiter.rs` (module `tests`).
Production code: unchanged.

Run: https://github.com/auenkr/cadence-rust/actions/runs/40035329154 (workflow `rust-ci.yml`, `package=cadence-matching`, branch `matching-ratelimit-matrix-20261024`, head `771e6ff17`, conclusion: success).
`cadence-matching` lib tests: 608 passed, 0 failed, 13 ignored. All eight `ratelimit_matrix_*` tests report `ok` in that run.

Earlier run 40035184351 failed on clippy (`manual-abs-diff` at `matching_limiter.rs:405`); fixed with `Duration::abs_diff` in commit `771e6ff17`.

## Subcase table

Go subcase names are the `t.Run` names inside `TestRatelimitBehavior`. "current" = `clock.Ratelimiter` through `taskMatcherImpl.ratelimit`, the production path.

| Go subcase | Rust test | Status | Run URL |
|---|---|---|---|
| `no deadline returns immediately if available` / `current` | `matching_limiter::tests::ratelimit_matrix_no_deadline_returns_immediately_if_available` | pass | run 40035329154 |
| `no deadline waits until available` / `current` | `matching_limiter::tests::ratelimit_matrix_no_deadline_waits_until_available` | pass | run 40035329154 |
| `canceling no deadline while waiting returns ctx err` / `current` | `matching_limiter::tests::ratelimit_matrix_canceling_no_deadline_while_waiting_returns_stopped` | pass | run 40035329154 |
| `returns immediately if canceled` / `current` | `matching_limiter::tests::ratelimit_matrix_returns_immediately_if_canceled` | pass | run 40035329154 |
| `sufficient deadline returns immediately if available` / `current` | `matching_limiter::tests::ratelimit_matrix_sufficient_deadline_returns_immediately_if_available` | pass | run 40035329154 |
| `sufficient deadline waits` / `current` | `matching_limiter::tests::ratelimit_matrix_sufficient_deadline_waits` | pass | run 40035329154 |
| `insufficient deadline stops immediately` / `current` | `matching_limiter::tests::ratelimit_matrix_insufficient_deadline_stops_immediately` | pass | run 40035329154 |
| `known different behavior` / `canceling while waiting with a deadline` / `new code stops quickly and returns unused tokens` | `matching_limiter::tests::ratelimit_matrix_canceling_while_waiting_with_deadline_stops_and_returns_unused_tokens` | pass | run 40035329154 |

Skipped (legacy `orig` code, no production path; not ported):

| Go subcase | Rust test | Status | Run URL |
|---|---|---|---|
| the seven `/ orig` subtests under the table cases above | none | skipped: legacy `rate.Limiter` path | n/a |
| `known misleading behavior` / `orig returned tokens are un-cancel-able if available immediately` | none | skipped: `orig` only | n/a |
| `known misleading behavior` / `orig returned tokens are un-cancel-able if it had to wait` | none | skipped: `orig` only | n/a |
| `known different behavior` / `canceling while waiting with a deadline` / `orig does not stop` | none | skipped: `orig` only | n/a |

## Counts

- Go subcases in scope (`current` + `new code`): 8. Rust tests added: 8. Pass: 8. Fail: 0. Ignored (parity gap): 0.
- Go `orig` subcases skipped: 10 (7 table `orig`, 2 `known misleading behavior`, 1 `orig does not stop`).

## Method and caveats

- Rust tests use the existing `fake_clock` and `limiter` helpers. The Go wall-clock sleeps are modeled by advancing the fake clock by the same nominal durations (`advance`). Real waits inside `wait_until` are timed with `Instant` and checked against Go's 10 ms precision (`assert_elapsed`).
- `tokio` in this crate has no `test-util` feature, so `start_paused` is unavailable and the timed cases take real time (about 1 s total).
- The 10 ms elapsed tolerance is Go's own value. A slow CI runner could make the timed cases flaky. None failed in run 40035329154.
- No parity gap was found: no Rust behavior differs from the Go `current` assertions, so no `#[ignore]` was added.
