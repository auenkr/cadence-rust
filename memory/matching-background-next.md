# Matching background reader in progress (2026-10-16)

Active Matching; user explicitly requires continuing to complete all roles, one
service at a time. Do not move to History before Matching replacement gates.

Working tree now connects TaskReader to native QueueRegistry owners. SQL pump
starts at ownership startup, scans default 1000 rows/50000-ID ranges (actual Go
dynamic property defaults), uses batch-1 local buffer and a separate dispatch
head. Local rendezvous waits for actual poll acceptance; cancelled waits never
hide a task in a dropped oneshot. Delivery workers withdraw timed-out waits and
end poller lifecycle immediately; acquired tasks retain detached History/rewrite
ownership. Queue fatal hook stops writer AND reader. Final owner teardown stops,
persists ack, forces GC, then asynchronously joins two reader loops without waiting History RPCs.
Registry ack timer already notifies pump. SQL failures resignal after retry ends.

First draft used blocking_send in SQL worker, which could exhaust Tokio blocking
pool across backlogged lists. Corrected before commit: persistence ReaderTurn owns
a Send reader permit (gate implemented with Mutex<bool>/Condvar), fetched rows
stay unregistered until next() admission. SQL and short admission mutex work use
blocking workers; buffer wait and rendezvous are async. Permit spans batch and
async waits, no mutex guard/thread held. Drop leaves unadmitted rows unread.
Fixed-time/synchronous queue API wraps same loading and registration helpers.

Tests: background prefetch bounds for batch=1 and3, independent task acceptance,
cancelled offers/waits, stop/disconnect and join, single-blocking-thread pool
availability while buffer full, cross-thread owned cursor drop/recovery, native
dispatcher timeout ending poller lifecycle before another task arrives. Registry
tests now consume via reader rather than competing direct SQL read cursor.
Actual Go admission fixture now includes buffered case capacity2 with one dispatch
head consumed, read/backlog4 (head+buffer+blocked pump row).

Validation final PASSED: 364 workspace tests, strict all-target workspace Clippy,
all 27 compatibility stages including actual Go Frontend/History/Worker + Rust
Matching and seven SDK scenarios. /tmp/cadence-background-checked.exit = 0;
artifacts /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-background-checked.
Final focused run PASSED 41 Matching tests plus domain activity test, including
last notify_one refinement (/tmp/cadence-background-acceptance.exit = 0).
Original Go common/cache Test_IsActiveIn also passed (.exit = 0).
Interrupted/compile-failed drafts are retained as diagnostic logs only.

Scope remains local asynchronous backlog. Isolation groups/rates/forwarding, sync
matching, queries, dynamic config, membership/backend/transport and remaining
Matching gates are open. No complete Matching replacement is claimed.

Final design audit additions: reader loop joins use Notify and await outside SQL
blocking workers (owner_work helper), avoiding cleanup starvation when the pool
has one worker. Buffered rows recheck expiry before dispatch and acknowledge
expired rows without immediate GC. Active-domain heads retry three-second default
dispatch deadlines and recheck expiry; passive heads have no deadline. Current
seeded domain metadata supplies live activity decisions via new is_active_in
helper implementing local/pending failover/active-active attribute rules. Unknown
cache entries assume active as Go does. Production domain cache remains open.
Dispatch timeout minima for low rate limits will be connected with rate limiter.

Strict all-target workspace lint passed after correcting missed method rename
and manual-ok lint. Original Go common/cache Test_IsActiveIn passed separately
(/tmp/cadence-domain-activity-go.exit = 0). Current final full runner:
/tmp/cadence-background-checked.log/.exit; artifact directory
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-background-checked.
Final focused Matching + domain helper tests use
/tmp/cadence-background-acceptance.log/.exit (includes last notify_one change).
Earlier final/async-verified directories are interrupted/compile-failed drafts,
not successful final validation. Inspect latest .exit before claiming success.

Final implementation uses async Notify for loop joins and releases SQL blocking
worker before awaiting them via owner_work. All described final checks passed.
Next bounded gap: failed-delivery rewrite retry through the writer (Go reader
completeTask uses ThrottleRetry; normal CreateTasks stays a single attempt).
ServiceBusy append saturation also needs throttle backoff (1s up to10s) in addition
to persistence retry (50ms/10s/30s). Existing SQLite-only persistence retry omits
throttle policy correctly because SQL adapter never returns ServiceBusy itself.
Use typed writer errors internally so shutdown/plain context deadlines remain
nonretryable while SQL/internal/sticky-translated failures and queue-full Busy
are classified correctly. Go persistence.IsTransientError includes Internal,
ServiceBusy, persistence Timeout, DBUnavailable. Preserve prior-error convention.
