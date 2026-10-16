# Matching reader retry continuation (2026-10-16)

Active service: Matching. User reconfirmed service-by-service gates: validate Rust
Matching against Go Frontend/History/Worker before progressing to History.

Reader bounded queries now share the SQLite persistence retry policy used for
leases: transient SQL failures retry per bounded range, preserving the ten-range
scan budget. CreateTasks remains single-attempt. Reader claims serialize on their
own mutex and snapshot max-read under state lock, then release state during SQL
and backoff; completion and appends can proceed. Empty-range ack advancement
compares the current ack after the scan (Go semantics), since completions may
advance ack while the read waits. Stop is checked again before registering rows.

Regression renames the SQLite tasks table temporarily to force real driver
failures, verifies completion remains possible, restores table and verifies
exactly-once claim recovery. Existing Go retry outcome and reader scan fixtures
cover retry classification and range scan semantics.

Validation PASSED: 349 workspace tests, strict all-target workspace Clippy,
all 27 compatibility stages including Go Frontend/History/Worker + Rust Matching
and seven SDK scenarios. /tmp/cadence-reader-full.exit = 0; artifacts
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-reader-retry-verified.
Final focused test includes recovery-wait and concurrency assertions:
/tmp/cadence-reader-final.exit = 0. /tmp/cadence-reader-focused.exit = 0 also.

Remaining: independent background reader pump, bounded per-isolation buffers and
unbuffered matcher rendezvous, retry resignal after exhausted reader failures,
rewrite retry classification, reader stop waits, injectable clocks/dynamic config,
sync/query/forwarding/rate limiting, partition routing/membership, internal Thrift,
and production backend selection. Current RPC dispatcher still reads batch=1 per
poll, with 100ms fallback. This is incomplete service migration.
