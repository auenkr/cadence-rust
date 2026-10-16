# Matching failed-delivery rewrite retry (2026-10-16)

Prior background checkpoint bf85c5226 is pushed. Active service remains Matching.
Native task delivery now calls writer.append_rewrite rather than a single append
when History delivery fails. Request/response channel keeps typed private errors
so public append Status behavior is unchanged while retry classification retains
SQL and sticky-translated errors. Queue-full ResourceExhausted is ServiceBusy and
retryable; plain shutdown and append-context deadline are nonretryable.

Outer rewrite retry uses Go persistence policy50ms initial/10s max/30s expiration
and separate ServiceBusy throttle1s initial/10s max/no expiration. Delay is maximum
of the two policies; throttle count advances only on Busy. Async sleep uses Send
StdRng for80/20 jitter. Go previous-error convention preserved. Individual
CreateTasks attempts remain single; normal AddTask uses append without retry.
Original ack/GC only runs after replacement append succeeds. Exhausted rewrite
failure still stops queue, retaining original for recovery.

Validation focused PASSED strict workspace all-target Clippy +43 Matching tests
(/tmp/cadence-rewrite-validate.exit = 0). Async retry helper uses ten actual-Go
outcome/count fixtures. Real SQLite insert-rejection trigger test verifies normal
AddTask fails once, rewrite actually fails (allocated ID gap observed), original
remains unacknowledged, recovery permits rewrite then old row is acked/collected.

Final full regression PASSED: 366 workspace tests, strict all-target Clippy, all
27 compatibility stages including actual mixed Go services/Rust Matching SDK
scenarios. /tmp/cadence-rewrite-full.exit = 0; artifacts
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-rewrite-verified.
Next source audit gap: state lock spanning lease retries can block existing-row
admission. See matching-db-lock-next.md before proceeding to limiter port.

Remaining Matching gates: complete matcher sync/query/forwarding and isolation/rate
limits, per-list dynamic config, domain cache/membership, partition scaling,
semaphore integration, internal Thrift/TChannel and production backend selection.
Standalone default still embedded backend; mixed harness seeded metadata/test-only
discovery is not production routing. All four roles remain incomplete.
