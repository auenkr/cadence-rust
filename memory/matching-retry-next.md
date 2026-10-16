# Matching lease retry continuation (2026-10-16)

Prior startup checkpoint 595638987 is pushed. Active service remains Matching.

Implemented startup and rollover lease retry using Go's persistence policy:
50ms initial, factor two, 10s maximum, 30s expiration, 80/20 jitter from the existing
common policy port. SQLite driver failures are transient (Go SQL wraps them as
InternalServiceError/TimeoutError); SQLite never reports throttling. Condition and
serializer failures are nonretryable. Go ThrottleRetry returns the previous
attempt error when retry terminates after another failure; preserve that quirk.
Every terminal rollover failure stops the queue, not just condition failures.
CreateTasks remains one attempt, matching Go. This is scoped to SQLite leases;
reader/rewrite transient retries and other backends are not yet covered.

Original-Go oracle covers ten retry outcome/count cases with deterministic
scripted delays. Rust executes the same outcomes. Real SQLite trigger test
rejects initial lease updates until released and verifies eventual startup, then
corrupts metadata before rollover and verifies terminal serializer failure stops
future appends. Full policy timing/jitter is the existing common backoff port.

Final validation PASSED: 348 workspace tests, strict all-target workspace Clippy,
and all 27 compatibility stages, including actual mixed Go services/Rust Matching
and seven SDK scenarios. /tmp/cadence-retry-full.exit = 0; artifacts:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-retry-verified/results.json.
Focused /tmp/cadence-retry-final.exit = 0 also covers Go oracle and nine persistence
queue tests. Final full regression includes the timestamp correction.

Later lease attempts advance the supplied timestamp by monotonic elapsed time;
a unified injectable wall-clock source remains to port. Remaining:
reader buffering/stop coordination, dynamic per-list config, sync/query/forwarding,
isolation/rate limits, partition/membership/transport/backend production routing.
All four deployment roles remain incomplete.

Next reader/matcher integration source audit:
- matcher.go MustOffer rate-limits backlog and holds one dispatch head while
  synchronously handing it to a local poller/forwarder. A plain buffered poll
  channel changes that backpressure and cancellation behavior.
- Offer returns false+error for a failed normal sync response; OfferOrTimeout
  returns true+error for a response error. No-response tasks return false even
  after local handoff. Preserve those distinct bool conventions.
- Poll first nonblocking-selects isolated/default/query channels, then blocks or
  forwards; invalid isolation names fall back to default. PollForQuery must only
  consume query tasks (passive-domain use). DisconnectBlockedPollers cancels
  matcher polling; RefreshCancelContext creates a replacement cancellation epoch.
- Reader's buffers are batchSize-1 plus a separate dispatch head per isolation
  group. Pump must register each unexpired ReadItem immediately before trying to
  send that task into its buffer. Current read_batch registers a whole batch at
  once; do not call it with a large size and silently mark unsent rows read.
