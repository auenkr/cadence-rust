# Matching dispatch limiter source audit (2026-10-16)

Next Matching work after pending rewrite checkpoint. Nothing in this note claims
rate limiting is implemented. Do not replace Go common/clock/ratelimiter.go with a
simple token-bucket approximation: the wrapper deliberately changes x/time/rate
reservation/cancellation/time behavior. Native path currently has no dispatch
limiter, and dispatch deadline minimum for low rates remains to connect.

TaskListLimiter source tasklist/task_list_limiter.go:
- Default TaskDispatchRPS100000, TTL1minute, minBurst dynamic default1.
- Divide reported aggregate rate by current partition count; burst=max(ceil(rate),
  minBurst), except rate==0 =>burst0.
- ReportLimit within TTL first rejects higher values, then abs(current-rps)<.001
  refreshes lastReceivedValue only. Order matters: tiny increases are rejected
  before precision branch, tiny decreases within precision refresh rather than
  lowering. Outside TTL skip optimistic checks and reacquire time under lock.
- tryUpdate accepts any lower value, or TTL elapsed and lastReceivedValue is NOT
  after(now-TTL), i.e. equality permits raising. Accepted scalar refreshes last
  update+last received and snapshots partition count.
- TTL elapsed while recent low reports protect current rate still refreshes
  lastUpdate and snapshots partitions. Backing limiter only changes if scalar
  accepted or partition count changed.
- getTask reports override if positive; else maxDispatchPerSecond if present;
  only nonnegative result reports to limiter. Missing/negative request uses
  configured default for poller history without resetting shared limiter.

Common clock limiter source:
- latestNow starts Go zero time; lockNow clamps nondecreasing. SetLimit/SetBurst
  use latestNow without acquiring real current time, preserving replenishment.
- Reserve consumes only an immediately allowed reservation. A future reservation
  is cancelled immediately under lock and exposed as denied.
- allowedReservation.Used(false) cancels at reservedAt clamped to latestNow, not
  real clock. Interleaving operations can prevent full restoration.
- Wait rejects already-cancelled context before consuming. Reserve1 token; zero
  burst cannot wait; if future delay exceeds deadline cancel while still locked.
  Otherwise timer waits unlocked. Cancellation restores using reservedAt clamped
  to latestNow. Exposes ErrCannotWait distinct from ctx.Err.
- Matcher maps ErrCannotWait to ErrTasklistThrottled. Normal Offer/MustOffer use
  limiter; forwarded sync tasks skip rate limit; queries bypass rate limit.
- Reader dispatch timeout=max(min(asyncTimeout,isolationDuration),
  (1sec*numberOfBuffers)/rps+100ms); default async timeout3sec. If no isolation and
  effectively zero rate (<=1e-7), or passive domain, no timeout is applied.

Need actual-Go differential fixture sequences for reported-rate TTL boundaries,
precision asymmetry, partition changes, zero/fractional rates; and clock limiter
Allow/Reserve/Used/Wait cancellation/time monotonicity. Wire into native queues
and mixed-service SDK rate scenarios before claiming this gate complete.

Resolved Go module (go list -m -f '{{.Dir}}' golang.org/x/time) is v0.14.0 even
though direct go.mod minimum is v0.12.0. Inspect exact resolved source at
/home/ubuntu/go/pkg/mod/golang.org/x/time@v0.14.0/rate/rate.go for token arithmetic.
Go rate.Inf is math.MaxFloat64, not IEEE infinity. Initial tokens=burst and
last/lastEvent are Go zero time (year1, not Unix epoch). Duration subtraction
saturates at int64 nanoseconds; use an internal wide time or explicit sentinel.
TokensAt computes advance without changing limiter.last/tokens; wrapper's
latestNow still advances, which can prevent cancellation restoring tokens.
SetLimitAndBurst does two underlying updates at latestNow, never real clock.
Cancel ignores invalid reservations, infinite current limit, tokens0 and act time
strictly before cancellation. Restore = reservedTokens - reservedRate * duration
between lastEvent and this reservation's act time. Clamp resulting tokens to
current burst; update last. If cancelling latest event, previous event can replace
lastEvent only if not before cancellation time. Reservation uses its original
rate for cancellation even after current rate changes. Used(false) is not guarded
by a once flag; repeated calls can restore more than once, bounded by burst.
