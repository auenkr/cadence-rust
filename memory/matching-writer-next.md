# Matching task writer: next implementation slice

Status: Go oracle and design evidence only; no Rust implementation or build in
this round. Matching remains incomplete. Original production Go files were not
modified.

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

No Rust work was performed, no service was advanced, no commit/push was made.
