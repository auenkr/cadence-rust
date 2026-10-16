# Matching writer continuation (2026-10-16)

Active service remains Matching. All four roles are incomplete.

The previously uncommitted writer skeleton is now connected to native decision/
activity AddTask RPCs and failed-delivery rewrites. Each queue owner has a bounded
250-request append channel, a sequential actor draining up to 100 additional
requests per batch (Go's actual off-by-one convention), and five-second append
budget. Enqueue saturation yields detailed ServiceBusyError; response timeout is
a deadline error. Caller cancellation after enqueue leaves the durable write and
notification alive. Actor receiver stays parked after stop, as Go does not close
appendCh. Queue fatal-stop watcher also stops writer and wakes registry controller.

Persistence batches preserve each request's source domain, reserve IDs across
lease ranges, compute creation/expiry timestamps after ID allocation and perform
one fenced SQL transaction. Failed writes retain allocated IDs/max read boundary.
Rewrites use the same actor and acknowledge original only after successful append.
Sticky lease condition failures translate to Go's InternalServiceError message.

New tests: actual-Go getWriteBatch oracle (seven cases), cancellation durability,
independent enqueue vs response timeout behavior. tests/run-compatibility.py now
includes the writer differential stage. Final validation PASSED: 345 workspace tests, strict all-target workspace
Clippy and all 26 compatibility stages, including actual mixed Go services/Rust
Matching and seven existing SDK scenarios. Evidence:
/tmp/cadence-writer-full.log and /tmp/cadence-writer-full.exit;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-writer-verified.
/tmp/cadence-writer-full.exit = 0. Two unused local timestamps discovered during
workspace compilation were removed before strict lint and differential/live builds.

Next: reader background buffer/pump and dispatch admission, per-owner startup
concurrency, dynamic per-list configuration and persistence transient retries.
Current dispatcher still calls read_batch(batch=1) per poll with 100ms fallback;
Go reader independently scans on buffered notifications/start/ack timer, with a
batchSize-1 channel plus one dispatch head per isolation group. Reads advance ack
only under existing scan/expiry rules. Reader stop persists ack and forces GC,
then waits for reader loops; manager Stop does not wait for all History RPCs.
Sync matcher, query broker, forwarding, isolation/rate limits, membership,
production backend selection and internal Thrift remain incomplete.

Startup source audit: Go handler/engine.go getTaskList holds taskListCreationLock
only through construction/registry Register, releases it BEFORE mgr.Start and its
lease I/O. Existing callers can retrieve manager and wait on startWG in operations
(task_list_manager.go lines ~406/426/455/547/673). Rust registry currently holds its
map lock through DurableTaskQueue::open. A correct continuation needs per-key
single startup + cancellation-independent completion, ready/error signaling,
shutdown ownership fencing, and no registry-wide lock during lease I/O. Do not
merely unlock around SQL without preventing duplicate lease acquisition.
CreateTasks itself has no writer retry in Go db.go; lease renewal and reader I/O
use ThrottleRetry. Preserve this distinction when adding retry classification.
