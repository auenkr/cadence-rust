# Matching queue lock source audit (2026-10-16)

Source gap before current working-tree correction: queue state mutex spans ID rollover
lease retry and CreateTasks SQL. New background reader gets an owned read cursor
and no blocking buffer waits, but per-row registration still needs queue state;
therefore a lease retry can delay delivery of already durable rows. Go DB mutex
only covers each RenewLease/CreateTasks/UpdateState operation; AckManager.ReadItem
is independent. Go retry sleeps occur outside taskListDB.Lock. Native final ack
can also wait through a full30sec retry where Go can update between attempts.
This remains a Matching error-path parity gap, not covered by happy-path SDKs.

Correction now implemented in working tree (pending full validation):
- Separate writer serialization gate (entire append/allocation, including direct
  queue APIs) from short state mutex and DB metadata-operation gate.
- Each lease attempt acquires DB gate, snapshots state, does SQL without state
  held, updates range/metadata under short state lock before releasing DB gate.
  Retry backoff releases DB gate. Writer gate preserves sequential ID allocation.
- CreateTasks acquires DB gate and snapshots range; no state held through SQL.
  Update maxRead after finished writes, even errors, preserving existing fixtures.
- Ack persistence acquires same DB gate before range/state snapshot and SQL, so
  a self-renewal cannot cause spurious fenced updates from an old range snapshot.
  Reader SQL and registration do not need DB gate. GC remains independent.
- Direct rewrite_failed must hold writer gate across original validation, append
  and ack to preserve current concurrent-call behavior, using an internal append
  method which assumes writer gate already held.
- Keep stop lock-free; final teardown waits reader loops asynchronously.

Regression: queue rangeSize1, write a durable row, install SQLite trigger rejecting
UPDATE of task_lists for rollover, start second append/renewal in blocking worker.
While retries are underway reader must claim/complete first durable row without
waiting for recovery. Remove trigger; append succeeds. Existing Go retry/queue/
writer fixtures and mixed-service SDK checks must continue passing.

Implemented writer and database gates, each lease retry attempt releasing DB gate
before sleep; short state updates only. Creation timestamp for pending appends is
computed after acquiring DB gate so lock waits do not stale the timestamp. Ack
snapshot/update is DB-gated against self-renewal while reader admission remains
independent. Existing direct synchronous APIs retain serialized writes.

Focused validation PASSED15 queue tests and strict workspace all-target lint
(/tmp/cadence-lock-final.exit = 0). New real SQLite trigger rejects only renewal
range increases; durable row1 reads/completes/persists ack while retrying, then
recovery permits range2/write2 and verifies persisted ack1. Cleanup releases
trigger before asserting failure result to avoid stranded retry threads.

Full regression PASSED:367 workspace tests, strict workspace all-target Clippy,
all27 compatibility stages including actual mixed Go services/Rust Matching SDK
scenarios. /tmp/cadence-lock-full.exit = 0; artifacts
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-db-lock-verified.
Next: rate-report aggregation and clock limiter, see matching-rate-next.md.
