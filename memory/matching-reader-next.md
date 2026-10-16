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

## Admission clock follow-up

Dispatcher uses read_batch_with_clock so expiry checks read time after retries and
for each row. Fixed-time read_batch remains for deterministic fixture callers.
New boundary test advances time between two rows with identical expiry: equality
is deliverable, strictly later admission skips the row. All 11 queue tests and
strict workspace lint passed (/tmp/cadence-reader-clock.exit = 0). Mixed Go
Frontend/History/Worker + Rust Matching with SDK cases passed:
/tmp/cadence-reader-clock-mixed.exit = 0; artifacts matching-reader-clock-mixed
under /home/ubuntu/.cache/cadence-rust-test-artifacts.

## Incremental buffer admission follow-up

read_batch_into now registers rows one at a time immediately before a potentially
blocking callback. State mutex is released during callback; reader mutex retains
one pump's exclusive cursor. Callback false leaves its current row in-flight and
later fetched rows unread. Existing read_batch wrappers collect through this
path; production still uses batch=1 per poll. Background buffers/matcher remain
unconnected, and no claim of complete pump is made.

Actual-Go addTasksToBuffer oracle uses unbuffered buffer, consumes zero/one/two
rows, then cancels while the next row is blocked. Three fixtures verify the
blocked row is registered (read/backlog consumed+1), later rows remain unread and
admission returns false. Rust consumes the same fixtures. A separate test checks
callback state access, retained in-flight row and subsequent scan of remaining
rows. Validation: 13 queue tests + three actual-Go differential groups + strict
workspace all-target Clippy passed (/tmp/cadence-admit-oracle.exit = 0). Actual
mixed Go services/Rust Matching SDK run passed (/tmp/cadence-admit-final.exit = 0;
matching-admission-mixed artifacts); final oracle-only additions followed that
mixed run and do not change runtime code.
