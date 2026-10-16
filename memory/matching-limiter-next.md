# Matching dispatch limiter wiring checkpoint (2026-10-16)

Native queue owners now enforce Go's task-list dispatch rate. This connects the
previously pure `cadence-common` clock limiter (`rate_limiter.rs`) and scalar
report aggregate (`matching_rate.rs`) into the Matching reader, poller reporting
and Describe, matching Go `tasklist/task_list_limiter.go` + `matcher.ratelimit`.

## New module `cadence-server/src/matching_limiter.rs`

- `TaskListLimiter` owns one mutex over `{TaskListRate, RateLimiter}` and a clock.
  `MonotonicClock` anchors once to wall Unix nanos plus `Instant::elapsed`, so rate
  state never rewinds (Go `time.Time` monotonic component). Queue expiry still uses
  wall time; only rate state uses the monotonic clock.
- Initial value `TaskDispatchRPS=100000`, TTL 1 minute, min burst 1, partitions 1
  (constant provider; no partition scaling is claimed yet).
- `report_limit` calls `TaskListRate::report_with_clock` and only calls
  `RateLimiter::set_limit_and_burst` when Go would (accepted scalar or changed
  partition snapshot). Existing Wait reservations are not repaced on config change.
- `limit()` is the backing limit; `wait_until(stop, deadline)` mirrors Go
  `Ratelimiter.Wait`: checks stop first (no token consumed), `reserve_wait` under a
  short lock, then sleeps unlocked. A `Drop` guard cancels the reservation at the
  last observed time on cancellation WITHOUT advancing the real clock, so a
  cancelled wait restores its token (verified). A token acquired by a successful
  wait is consumed even if the later matcher handoff is cancelled.
- `WaitError::ZeroBurst` and `WaitError::Deadline` both map to `WaitOutcome::
  Throttled`, matching Go mapping both `errWaitZeroBurst` and
  `errWaitLongerThanDeadline` (both wrap `ErrCannotWait`) to `ErrTasklistThrottled`.

## Reader integration (`matching_reader.rs`)

- New `with_domain_activity_and_limiter` constructor; `ReaderConfig` gains
  `dispatch_timeout_buffer` (Go `taskDispatchTimeoutBuffer` = 100 ms).
- Per dispatch attempt the reader computes the Go `newDispatchContext` deadline:
  only when the backing `rps > 1e-7` AND the domain is active/unknown. Timeout is
  `max(AsyncTaskDispatchTimeout=3s, 1s / rps + 100ms)`. No deadline for a passive
  domain or near-zero rate.
- Each attempt: limiter `wait_until` then `matcher.offer`, wrapped in one
  `tokio::time::timeout` for the deadline, exactly like Go wrapping
  `ratelimit(ctx)` + channel handoff in the dispatch context.
- `Attempt::Throttled`/`Attempt::Retry` (throttled or dispatch deadline) yields and
  rechecks expiry on the next iteration; `Attempt::Stopped` ends the dispatch loop.

## RPC integration (`grpc_matching.rs`)

- Activity poll reports the worker `max_tasks_per_second` to the shared limiter
  only when present and non-negative, and uses it for poller history; a missing or
  negative rate keeps `TASK_DISPATCH_RPS` for poller history without touching the
  shared limiter. Decision polls report no rate (matching Go passing `nil`).
- `DescribeTaskList.status.rate_per_second` now reads `queue.limiter.limit()`
  instead of the hardcoded 100000.

## Validation

Focused: 5 new limiter unit tests, 70 cadence-server lib tests, strict all-target
workspace Clippy. `/tmp/cadence-lim-lib.exit=0`, `/tmp/cadence-lim-clippy.exit=0`,
`/tmp/cadence-lim-test.exit=0`. Full compatibility runner not yet rerun with the
wiring; the 29-stage runner must be rerun and a mixed Go-services/Rust-Matching
activity-pacing scenario added before this gate is claimed.

## Remaining Matching gates (still open)

- Partition configuration/provider semantics (limiter divides by a constant 1).
- Mixed rate scenario: Go SDK activity pacing with Rust Matching compared against
  an all-Go baseline (allow transition burst; do not assert the first two
  deliveries after a rate transition are spaced).
- Query delivery through the durable queue, sticky availability, forwarding
  (forwarded sync matching currently returns unimplemented), partition routing,
  isolation groups, semaphores.
- Membership/ownership, standalone lifecycle, transport health/graceful shutdown.
