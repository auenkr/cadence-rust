# Matching clock rate limiter checkpoint (2026-10-16)

Current uncommitted rate_limiter.rs ports Cadence common/clock/ratelimiter.go plus
resolved x/time/rate v0.14.0 single-token arithmetic. Full upstream BSD notice is
retained in module. Caller synchronizes state; no async/native wiring yet.

Owned Reservation retains original rate/time. State uses i128 Unix nanos for Go
zero time(year1) and int64-saturating duration subtraction. Duration.Seconds uses
integer sec/remainder split as Go does. Latest observed time never rewinds;
setters and cancellation use latest/reserved time without sampling real clock.
Tokens observes advancement but does not mutate bucket.last. Immediate Reserve
cancels denied future reservations under one call. reserve_wait plans delay,
rejects zero burst/deadline and returns reservation for caller to await/cancel.
Caller must check already-cancelled context before reserve_wait. Tokens acquired
by successful Wait are consumed even if subsequent matcher handoff is cancelled.

Original-Go tests/clock-rate-go_test.go overlays common/clock, exercising21
sequences including actual Wait with fake timers, deadlines, cancellation and
interleaved observation. Observes underlying TokensAt(latestNow) so the observation
itself does NOT advance wrapper time. Wait source wrapper records actual timer
interval. Rust compares token state, allowed flags, limit/burst/latest time,
Wait errors and timer delay. Fixtures in tests/fixtures/clock-rate.json and
runner tests/run-clock-rate-compat.py. Full runner now29 stages.

First validation PASSED21 Go/Rust sequences, all31 common tests and strict
all-target workspace Clippy (/tmp/cadence-clock-rate-final.exit=0). Last refinement
returns InfDuration for delay on invalid reservations; final focused repeat at
/tmp/cadence-clock-rate-accepted.log/.exit must be inspected before commit.
Latest full live validation remains367 tests/27 stages at DB-lock checkpoint;
subsequent scalar and clock modules are not yet connected to native dispatch.

Next native wiring design:
- Queue-owner TaskListLimiter owns Arc<Mutex<{TaskListRate,RateLimiter}>> and a
  monotonic clock anchored once to wall Unix nanos (Instant.elapsed+anchor).
  Do not use wall SystemTime repeatedly for rate state: Go time.Time comparisons
  carry monotonic component. Queue expiry still uses wall time (persisted expiry
  time lacks monotonic component).
- report_with_clock under limiter state lock; on changed scalar/partitions call
  bucket.set_limit_and_burst. Do not wake/repace existing Wait reservations when
  config changes; their timers and original reservation rate remain fixed.
- Async Wait checks stop/deadline before consuming. Calls reserve_wait under
  short lock, then drops lock before timer/select. Cancellation Drop guard calls
  bucket.cancel at reserved/latest time WITHOUT real-clock advancement. Disarm
  guard after successful delay, before handoff; failed/cancelled handoff does NOT
  return that Wait token (Go consumes it already).
- Reader dispatch attempt wraps limiter Wait + matcher.offer in one configured
  context deadline. Zero-burst/cannot-meet-deadline => ErrTasklistThrottled;
  yield/retry head as Go does, checking expiry each iteration. Stop exits.
- Timeout=max(configured3sec,1sec*bufferCount/rps+100ms) for active normal tasklist
  rps>1e-7; otherwise no deadline without isolation. Passive domain no deadline.
- Activity Poll metadata reports nonnegative rates; missing/negative preserves
  shared limiter and poller history uses configured default100000. Decision poll
  does not report a rate. Dynamic override/per-list config remains to bind.
- Native Describe.status.rate_per_second should read actual backing limit rather
  than hardcoded100000. Partition count provider initially root state must later
  use real partition config; do not claim scaling from constant1.
- Test async permit restoration, no refund after handoff cancellation, reports not
  revoking old head permits, zero rate/low-rate minimum deadline, plus mixed Go
  service SDK activity pacing compared with original Go (allow transition burst).
