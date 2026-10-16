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

Prepared scalar-report oracle (not yet committed or verified):
/tmp/cadence-matching-rate-go_test.go; input /tmp/cadence-matching-rate-input.json;
overlay /tmp/cadence-matching-rate-overlay.json. Ten sequences cover TTL, precision,
partition snapshots, fractional/zero/negative direct rates, backwards fake clock.
Initial oracle run was interrupted to serialize heavy Go package builds with the
DB-lock full regression. Restart with CADENCE_MATCHING_RATE_INPUT/OUTPUT pointing
to input and /tmp/cadence-matching-rate-output.json and go test -overlay ...
./service/matching/tasklist -run '^TestRustMatchingRateReports$' -count=1.
If output matches intended schema, move harness/input+regenerated output into tests
and implement scalar aggregate state; do not claim dispatch enforcement until
clock token/reservation behavior and reader/RPC wiring are also ported/tested.
Prepared scalar Rust draft: /tmp/cadence-matching-rate.rs (standalone rustc library
compile passed; NOT fixture-validated or integrated). Implements TTL/precision/
partition report state and Go64 float-to-int overflow convention for burst. Move
into cadence-common only after the current DB-lock checkpoint and Go oracle pass.

Native wiring notes: activity PollRequest metadata.max_tasks_per_second is already
recorded in PollLifecycle.info but is not reported to any shared dispatch limiter.
Native DescribeTaskList.status.rate_per_second is hardcoded100000. Decision polls
supply no worker rate. Those sites are in grpc_matching.rs. Queue owner can own a
shared scalar/bucket policy; default partition count1 until real partition state
is wired (do not claim partition scaling from a constant provider).

Timing caveat for mixed rate tests: MustOffer calls limiter.Wait BEFORE blocking
for a poller. A dispatch head may already own an old/default-rate token when a
poll reports a new low rate. Updating limit/burst does not revoke that head. The
next head can still consume the new burst immediately; do not incorrectly assert
that the very first two deliveries after rate transition must be spaced apart.
Preserve Go reservation/update timing rather than consuming at poll receipt.

## Scalar report checkpoint

Go oracle rerun passed serially (/tmp/cadence-rate-oracle-retry.exit=0), then
harness and fixture moved into tests/matching-rate-go_test.go and
tests/fixtures/matching-rate.json. cadence-common matching_rate now implements
report aggregation with11 Go sequences. run-matching-rate-compat.py compares
actual Go output then Rust fields/provider-read counts. report_with_clock reads
fresh timestamp for tryUpdate after optimistic check; fixed-clock report wraps it.
New matching-rate-differential stage brings full runner to28 stages.

/tmp/cadence-rate-final.exit =0: eleven actual-Go/Rust sequences plus strict
workspace all-target Clippy passed. First draft incorrectly assumed positive
float-to-int overflow became MinInt; actual Go arm64 oracle saturates to MaxInt64,
so corrected to Rust saturating cast. This architecture-specific edge remains
covered by differential, not assumed from language specification.

This checkpoint is pure aggregate state, not dispatch enforcement. Native poll
rate reports/Describe and bucket/reservation integration still need wiring.
Next implement clock limiter reservation semantics and connect shared owner policy
to reader's pre-handoff Wait, default/minimum dispatch timeout, activity reports
and DescribeTaskList before claiming native rate behavior. Full service remains
incomplete, as do all four roles. Latest full runtime validation remains prior
DB-lock checkpoint367 tests/27 stages; new scalar checks are focused only.
