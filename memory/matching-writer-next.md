# Matching task writer: next implementation slice

Status: A standalone bounded Rust actor primitive is implemented and tested.
`DurableTaskQueue` call paths are not wired to it, so Matching writer parity is
incomplete. Original production Go files were not modified.

## Original Go contract

Source: `go/service/matching/tasklist/task_writer.go` (especially `newTaskWriter`,
`appendTask`, `allocTaskIDs`, `taskWriterLoop`, `getWriteBatch`, and
`sendWriteResponse`). The writer is created at
`go/service/matching/tasklist/task_list_manager.go:253`; manager startup invokes
`Start`, teardown calls `Stop` at ~335. Synchronous append callers are manager
normal AddTask (~596), sync AddTask (~623), and reader failed-delivery rewrite
(`task_reader.go:416`). `GetMaxReadLevel` is consumed by reader/manager read
boundaries. Preserve the synchronous `appendTask` API at these call sites.

* `appendCh` capacity is sampled exactly once in `newTaskWriter` from
  `OutstandingTaskAppendsThreshold()`. Go `make(chan, negative)` panics; zero is
  an unbuffered rendezvous; positive values bound waiting appends. This is not a
  live resizing setting.
* `MaxTaskBatchSize()` is read for every collection pass. The writer already
  has one request, then takes up to that many immediately available requests:
  actual maximum is `1 + max(value, 0)`. Zero and negative mean singleton
  batches, not disabled writes. Collection is nonblocking and does not wait for
  a full batch.
* `AppendTaskTimeout()` is sampled per append invocation before enqueue. One
  deadline bounds both channel enqueue and post-enqueue response wait. Queue
  saturation returns ServiceBusy with `task queue is full and timed out waiting
  to enqueue`; after enqueue it returns a deadline/cancellation-wrapped receive
  error. Caller cancellation after enqueue does not retract durable work.
  Per-request response channel has capacity one so an abandoned caller cannot
  block the writer.
* Stop atomically marks stopped and closes stopCh, but leaves appendCh open.
  Append checks stopped before queueing; stop while waiting for response returns
  `task list shutting down`. The actor checks stopped after receiving a request,
  exits without draining queued items, and response fanout selects stopCh to
  avoid blocking. Select races mean simultaneous stop/response or stop/enqueue
  readiness are intentionally nondeterministic. Do not promise a drain the Go
  writer does not perform.
* Actor allocates IDs for a full batch before CreateTasks. Allocation/lease
  failure responds to each request with the same error and leaves max-read-level
  untouched. IDs consumed before a failed persistence write are not reclaimed;
  max-read-level is nevertheless advanced through the allocated IDs after the
  CreateTasks attempt, even on persistence error. A persistence
  ConditionFailedError signals fatal (sticky task lists translate to the fixed
  InternalServiceError); other CreateTasks errors are returned to the batch.
  Lease renewal retries transient errors; CreateTasks itself is not retried here.

## Bounded Rust design

Keep `DurableTaskQueue::append` synchronous and source-compatible. Its existing
writer mutex already serializes persistence and defines owner lifetime; adding
an async API would force executor/runtime assumptions into synchronous matching
callers and failed-delivery rewrite. Add a per-queue writer actor only as an
internal implementation detail: bounded `sync_channel`/Tokio mpsc plus a
dedicated owned worker (or an equivalent bounded queue/condvar), with a
one-shot response per request. All current callers enqueue and block on their
response, preserving call ordering and result semantics. Do not hold the queue
state/connection mutex while waiting for queue capacity or a response; only the
actor takes persistence/ID ownership. The queue owns and joins/cancels its actor
at shutdown, and registry removal must not hold the global registry lock across
that wait. Never close the sender channel while append callers can still hold
an owner reference.

Read capacity when constructing each queue owner; reject negative configuration
at that boundary with an explicit config error rather than Rust panic. Capacity
zero must be an actual rendezvous (not silently clamped to one). Sample max
batch size on each actor collection and implement the Go convention of one
already-received item plus up to `max(value, 0)` immediately-ready items. Sample
timeout per append before enqueue; use a single monotonic deadline for enqueue
and response, return a distinct ServiceBusy queue-full error only when enqueue
did not happen, and retain accepted work after caller cancellation. Use an owned
response cell so cancellation never blocks actor fanout. Stop rejects new work,
wakes existing waiters, leaves accepted requests' persistence outcome fenced by
the same actor lifecycle, and follows source-compatible actor exit behavior;
do not label this a graceful drain unless service-level shutdown separately
implements one.

Preserve ordering of ID allocation, one batch CreateTasks, event/metrics,
max-read-level publication, and per-request response fanout. Do not implement
the DB mutex as a substitute for the channel: it has no bounded queue,
per-call enqueue deadline, live batching or accepted-request cancellation
semantics.

## Oracle generator and evidence

`tests/run-matching-writer-next-oracle.py` overlays
`tests/matching-writer-next-go_test.go` into the original `tasklist` package;
the overlay file does not modify tracked Go production source. The test invokes
the actual `newTaskWriter`, `getWriteBatch`, `appendTask`, and `Stop` methods.
The generator compares with `tests/fixtures/matching-writer-next.json`; use
`--regenerate` only when intentionally reviewing a Go-source behavior change.
Evidence copy and command output are under
`memory/evidence/matching-writer-next/`.

The test uses channels as gates for enqueue/response/stop ordering. Go's
`context.WithTimeout` uses the real runtime timer and cannot be driven by the
task-list mocked clock, so timeout boundaries use 15ms deadlines; ordering is
deterministic, exact timer scheduling is not. In particular, simultaneous
ready select cases remain Go runtime race semantics. The fixture includes
capacity 0/1/4/8; max batch 0/-1/1/4; full-queue deadline; accepted request
response deadline; cancellation after acceptance; stop after acceptance. This
slice does not yet inject allocation/CreateTasks errors; tests for those belong
to implementation acceptance below.

## Implementation acceptance tests

1. Construction samples capacity once; capacity 0 rendezvous blocks until a
   receiver, positive N admits exactly N pending requests, negative fails
   cleanly. Changing provider after construction does not resize it.
2. Live batch-provider updates affect the next collection; values -1/0/1/large
   yield batch counts 1/1/2/(1+large), and the actor never waits to fill a batch.
3. One per-call deadline covers both queue admission and persistence response;
   errors retain ServiceBusy-vs-deadline distinction. Cancellation after
   acceptance leaves one durable create and cannot stall subsequent writes.
4. Stop wakes blocked admission/response callers, rejects later requests, does
   not close a channel still referenced by callers, and demonstrates the
   documented queued-work race without claiming drain semantics.
5. Allocate-ID / lease error fanout, lease-range rollover, persistence error ID
   consumption and max-read-level, fatal condition failure/sticky translation,
   and no CreateTasks retry are asserted with persistence fakes.
6. Concurrent sync AddTask, normal AddTask, and reader rewrite share actor
   ordering; append caller cancellation and queue owner removal do not deadlock
   registry, owner, or persistence locks.

## Rust actor slice (2026-10-18)

Added `cadence_persistence::matching_writer_actor::MatchingWriterActor`, an
owned standard-library thread actor. It has fixed construction capacity,
`sync_channel(0)` rendezvous semantics, a live batch-size provider, synchronous
per-request response cells, one monotonic timeout for admission and response,
ServiceBusy on admission timeout, accepted-work retention after caller timeout
or cancellation, nonblocking response fanout, and stop without queue drain.
Negative capacity returns `WriterError::InvalidCapacity` at construction.
This preserves the Go startup failure boundary while presenting a typed Rust
error instead of Go's `make(chan, negative)` panic.

The primitive is intentionally not connected to `DurableTaskQueue`: that owner
currently exposes slice appends, has no Matching config-provider inputs, and
holds its writer mutex across synchronous persistence. Wiring requires converting
each append to an actor request while aggregating batch task IDs, source-domain
rows, pending-task timeouts, persistence errors, and caller results without
changing allocation and max-read-level order. Keep this concrete integration
work as the next Matching writer slice; do not add configuration getters that
pretend the actor is live.

Rust unit tests consume `tests/fixtures/matching-writer-next.json` produced by
the original Go writer, validate all 8 source traces and exercise rendezvous,
negative construction, timeout, and cancellation against a gated fake
persistence handler. The fixture-driven assertion is useful for documented Go
error strings and batch limits, but does not yet drive every trace event through
the actor. Stop while a synchronous handler is in progress joins that handler;
this is an ownership/lifecycle detail to reassess during integration. The
current primitive does not itself map caller context errors into Go's wrapped
`context canceled` / `deadline exceeded` strings; callers receive typed Rust
variants.

Evidence: `memory/evidence/matching-writer-next/`. Matching remains incomplete;
there is no mixed Go/Rust service test or integration test from this slice.

## Rust writer actor follow-up (commit pending)

Cherry-picked candidate `6c1bd6fa409ec4eabaf792b0fcb9b0ecf8fcce39` into
this worktree as the persistence-owned primitive. Follow-up fixes cover
cancellation while waiting for admission (as well as response wait), immediate
no-drain `stop()` with a separate explicit `join()`, and exact response-count
validation instead of silently truncating `zip` fanout. Added deterministic
fake-handler tests for each behavior. Capacity remains a Go-construction
contract: negative capacity returns `InvalidCapacity` at this generic Rust
primitive boundary; no clamping was added.

The actor's receiver uses a 10ms timed receive to notice atomic stop; channel
admission retries at 2ms and cancellation response polling at up to 10ms. The
standard-library bounded channel cannot wake these waits from the separate
atomic tokens, so these are bounded polling intervals, not wake-driven waits.
A custom condvar queue/token design remains preferable if tighter cancellation
latency is required. Explicit `join()` and `Drop` can wait for a synchronous
handler; only `stop()` is immediate.

Validation: `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-persistence
--lib matching_writer_actor` passed 7 tests. `cargo fmt --all -- --check` was
not usable as a repository-wide gate because it reports pre-existing formatting
diffs across unrelated crates; the edited actor file was formatted directly.
The Go fixture reader still validates fixture values only; it does not compare
Rust actor event traces with Go event traces. Thus all eight scenarios in the
fixture have not yet been run as a Rust-vs-Go differential suite, and actor
parity remains incomplete. Strict locked all-target persistence Clippy passed
with `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-persistence
--all-targets -- -D warnings` (exit 0). The Go oracle passed and reproduced all
8 fixture traces; a Rust-vs-Go event-by-event differential suite remains open.

## Actor continuation in matching-actor-parity checkout (2026-10-18)

Applied original candidate commits `6c1bd6fa409ec4eabaf792b0fcb9b0ecf8fcce39`
and `a5200df8328d5157011d060d987b6046db546959` (cherry-pick commits
`4a8591c0f` and `7e2448340`). The Rust persistence actor remains an isolated
primitive: no production queue wiring or server `matching_writer.rs` edits were
made. Negative capacity still returns `InvalidCapacity` at primitive
construction; no production fallback behavior was introduced.

Corrections in this continuation: `Drop` sets stop and detaches the worker
handle so it cannot synchronously join a blocked handler; controlled callers
can still use explicit `join()`. Admission cancellation now maps to
`ServiceBusy`, matching Go's enqueue-side `tCtx.Done()` branch. Cancellation
after admission remains `Cancelled`, and accepted work is retained. The
fixture test executes each Go fixture batch through the actor and compares its
persistence callback input with the exact fixture batch. A gate holds the
actor at its live batch-size sample while buffered requests are preloaded, so
caller scheduling cannot change those assertions. Go's runtime nondeterminism
for simultaneous select cases remains documented in the original evidence
above. Other actor tests execute response timeout, accepted cancellation,
rendezvous admission, stop and drop behavior with channel gates.

Evidence collected in this checkout:

* `python3 tests/run-matching-writer-next-oracle.py` — exit 0;
  `ok github.com/uber/cadence/service/matching/tasklist 0.129s`, then
  `Verified 8 event traces from original Go taskWriter`.
* `cd rust && CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-persistence
  --lib matching_writer_actor` — tmux `cadence-rust` window `writer44-final3` passed: exit 0, 9 passed,
  0 failed, 182 filtered, 0.18s.
* `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-persistence
  --all-targets -- -D warnings` ran after the focused tests in that serialized window and passed (exit 0).

Known limitation: admission, response cancellation, and worker stop detection
still use 2ms/10ms polling around `std::sync::mpsc`; no notification-driven
queue was added. Drop now returns promptly while a handler is blocked, but that
owned thread finishes only after its handler is released/returns.

Final validation for this continuation: Rust focused actor tests exit 0 (9
passed, 0 failed, 182 filtered, 0.18s); strict locked all-target persistence
Clippy exit 0. Full logs and exit markers are in
`memory/evidence/matching-writer-next/rust-final-focused-tests.*` and
`strict-clippy-final.*`.

## Admission after Stop parity repair (2026-10-18)

The isolated actor now preserves Go's initial-stop check while no longer
checking `stopped` during queue admission. If Stop ends the receiver after a
caller already entered admission, a disconnected Rust sender is treated like a
still-saturated open Go `appendCh`: the request remains unaccepted and waits
until its context deadline/cancellation, returning ServiceBusy. Callers that
start after Stop still immediately return `task list shutting down`. Accepted
callers waiting for a response still leave on Stop, and queued accepted work
is not drained. This does not change actor stop/join/drop behavior or connect
the actor to production queue paths.

Regression coverage holds a write handler and a second queued item to keep
capacity saturated, starts another append, then stops the actor before its
admission deadline. It asserts ServiceBusy only after the timeout ordering;
afterward, a newly started append returns Stopped immediately. A second gated
case stops a saturated actor and then cancels the already-waiting caller; it
returns ServiceBusy through the enqueue cancellation path. The Go overlay runs
the original `appendTask` with the same saturated queue ordering and asserts
`task queue is full and timed out waiting to enqueue` for both timeout and
cancellation, after Stop; a post-stop caller remains covered by existing
Rust initial-stop coverage. Original Go production files were not changed.

Evidence in `memory/evidence/matching-writer-next/writer45/`:

* `python3 tests/run-matching-writer-next-oracle.py` exit 0; the actual Go
  overlay passed and verified all 8 preexisting event traces plus the new
  after-Stop timeout/cancellation assertions.
* `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-persistence --lib
  matching_writer_actor` exit 0; 10 passed, 0 failed, 182 filtered. The new
  test asserts that the timeout waiter remains pending for at least 100ms of
  its 120ms deadline after Stop, while post-stop admission returns immediately.
* `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-persistence --all-targets
  -- -D warnings` exit 0.

This is still only the standalone actor primitive. No DurableTaskQueue wiring,
production fallback policy, or broader Matching parity claim is included.

## Filtered append capacity and zero-capacity production admission (2026-10-18)

The production Matching `TaskWriter` now receives the filtered
`matching.outstandingTaskAppendsThreshold` setting from the physical queue's
domain/list/type provider. Go defines its default as exactly 250
(`go/common/dynamicconfig/dynamicproperties/constants.go:4001-4005`). The
provider includes this signed value, and each writer samples it during owner
construction. Existing owners retain their selected capacity; newly created
owners sample the current property. Negative values fail immediately at
construction through signed-to-capacity conversion, matching Go's
`make(chan, negative)` failure boundary. Positive capacities retain the
configured Tokio mpsc buffer size.

Capacity zero has a separate wake-driven rendezvous path. A semaphore indicates
that the actor is ready for its next request; a single mutex-protected slot and
Notify carry the actual request. The actor removes the request and acknowledges
admission only when it receives it. A drop guard withdraws an unaccepted slot
when its caller is canceled or times out. This avoids making Tokio's required
positive-capacity mpsc channel behave as a hidden buffer during a rendezvous.
After admission, the existing one-shot result remains buffered and caller
cancellation does not retract the append. Stop continues to leave the channel
open, rejects new appends immediately, and interrupts accepted result waits.

Production tests added for zero-capacity admission while the actor is gated,
admission timeout, cancellation before acceptance, persistence of an accepted
task across Stop, construction-time capacity sampling, non-resizing existing
owners, negative construction failure, and positive buffer capacity. The live
writer config test now checks the capacity filter by domain, task list, and
task type and verifies the 250 default. The existing live batch sample test
accounts for the additional construction-time provider call.

Evidence for this slice is under `memory/evidence/matching-writer-next/writer53/`:

* `focused-pass.log` / `.exit`: `CARGO_BUILD_JOBS=1 cargo test --locked -p
  cadence-server --lib matching_writer` — exit 0, 8 passed, 0 failed.
* `config.log` / `.exit`: `CARGO_BUILD_JOBS=1 cargo test --locked -p
  cadence-server --lib matching_rpc_live_writer_config_is_filtered_and_resampled`
  — exit 0, 1 passed, 0 failed.
* `go-oracle.log` / `.exit`: `python3
  tests/run-matching-writer-next-oracle.py` — exit 0; original Go test passed
  and verified all 8 event traces.
* `clippy-final.log` / `.exit`: `CARGO_BUILD_JOBS=1 cargo clippy --locked -p
  cadence-server --all-targets -- -D warnings` — exit 0.

Matching is still incomplete. This bounded slice does not claim mixed-cluster
SDK parity or service completion; the full mixed regression and Matching parity
audit remain separate work.

## Zero-capacity sender handoff repair (2026-10-18)

Applied candidate `9bd9cfdd7ae663b6a3e778869fe222f508c4600a` to isolated
checkout commit `9162aa7dd`, then repaired its zero-capacity rendezvous. The
candidate's reusable semaphore plus one request slot could accumulate permits
across receive passes and overwrite an accepted request when concurrent
appenders published. The replacement uses a mutex-protected FIFO of *waiting
senders*: publication adds an unaccepted sender, and the actor atomically
removes one and acknowledges it. Cancellation/timeout removes an offer only if
it remains waiting. There is no reusable permit or accepted request buffer.

The actor's zero-capacity batch drain now runs Go's live max-batch loop over
already-waiting senders. The Go overlay adds an actual unbuffered-channel case:
with eight blocked senders and max batch 3, `getWriteBatch` takes three beyond
the first and accepts all eight across two batches. The oracle records counts,
not sender order, because Go scheduling makes request order nondeterministic.
The Rust test gates the actor, queues 24 simultaneous contenders, verifies all
24 handoffs exactly once over three successive receive passes, and checks that
the waiting queue empties. A cancellation guard test verifies that an offer
withdrawn before receive leaves no request to accept. Existing production
coverage still checks the busy actor, canceled admission, accepted persistence
across Stop, construction-time filtered capacity, and batch sampling.

Evidence under `memory/evidence/matching-writer-next/writer54/`:

* `go-oracle.log` / `.exit`: `python3 tests/run-matching-writer-next-oracle.py`
  — exit 0, original Go package passed and verified 9 traces, including the
  new unbuffered ready-sender batch trace.
* `focused-retry.log` / `.exit`: `CARGO_BUILD_JOBS=1 cargo test --locked -p
  cadence-server --lib matching_writer -- --nocapture` — exit 0, 10 passed,
  0 failed, 192 filtered.
* `clippy.log` / `.exit`: `CARGO_BUILD_JOBS=1 cargo clippy --locked -p
  cadence-server --all-targets -- -D warnings` — exit 0.

The original `writer53` Go trace had only buffered channel cases. The new
`writer54` oracle covers the previously untested ready-rendezvous batch case.
Mixed Go/Rust cluster validation, the full Matching audit, and service
completion are still outstanding.
