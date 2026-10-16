# Matching teardown and idle liveness continuation (2026-10-16)

Prior pushed checkpoint: cbd02ab7c (333 tests / 24 compatibility stages).
New queue stop flag is atomic rather than guarded by the database/state mutex.
Admission stop and fatal notifications therefore do not block behind in-flight
queue state operations. A nonblocking/idempotent stop waker notifies the registry.

Registry unload detaches identity-checked owners before final ack/GC cleanup and
releases the map lock for cleanup. Stopped-owner replacement also releases it,
then rechecks shutdown and concurrent registration before acquiring a new lease.
Regular ack flushing snapshots owners and identity-checks fatal removals; service
shutdown drains the map/stops admission before slow cleanup. Old cleanup cannot
unregister a replacement; a controlled race test checks that its final ack stays
fenced while old completed-row GC leaves the new task/lease intact.

Added common::liveness::Liveness with original Go Unix-nanosecond wrapping
subtraction and strict `< ttl` predicate (expires at equality). Go differential
overlay checks 15 observations across positive/zero/negative TTLs, refresh and
clock rollback. Default native TTL is five minutes, checked every TTL/2. Go's
actual Manager uses IdleTasklistCheckInterval as TTL; do not confuse the older
MaxTasklistIdleTime property with this live code path.

Only nonforwarded AddTask and each new GetTask wait mark alive, as in Go. Describe
lookups do not refresh it. Dispatcher has a separate wait-start flag so empty
100ms storage scans without poller observation do not refresh liveness repeatedly.
Idle expiration detaches/stops the owner and performs normal final ack/GC cleanup;
unacknowledged rows remain durable and recover on the next lease.

The owned maintenance loop chooses both ack and idle deadlines per owner. A
registered/stop notification is enabled before reading state, avoiding lost
wakeups. New owners reschedule the timer; fatal owners are reaped promptly
without waiting for the next ack minute.

First focused run before final wakeup/closed-poll changes passed the liveness
differential, queue tests, 24 Matching tests and strict lint:
/tmp/cadence-teardown-focused.exit = 0.
Source review of Go handler/engine.go (decision/activity ErrNoTasks/errPumpClosed
branches) showed that a closed pump returns empty success, not Unavailable.
Dispatcher now preserves that behavior; explicit poll cancellation stays separate.
The new mixed Go Frontend probe waits for an idle poll registration, advances a
test-only liveness clock through a fixture trigger, checks empty success, then
unloads an owner with queued work and confirms recovery/completion on reload.
No workflow logic changes; the clock advance only avoids a five-minute test wait.

The first full run matching-idle-final was deliberately interrupted before this
closed-poll correction. Do not use it as final transport validation.
Fresh frozen-source validation PASSED: 342 workspace tests, strict workspace
all-target clippy and all 25 compatibility stages. /tmp/cadence-idle-verified.exit
= 0; /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-idle-verified/results.json.
The mixed probe reports idleUnloadAndRecovery=passed: the blocked poll returned
empty success through Go Frontend, queued work survived unload/reload and completed,
and all seven existing SDK scenarios passed with no Go Matching process.

Source clarification: Go Manager.stopWG waits for the initial partition-config
notification, not every in-flight History RPC. Reader.stopWg tracks background
reader tasks. Writer.Stop closes its stop channel without a writer-loop join.
Do not invent a stronger manager-wide History wait than the original behavior.

Remaining: transport-level graceful shutdown and concurrent stop completion,
background reader buffering/writer batching and per-owner controller concurrency, production dynamic domain/task-list properties,
Go-style nonblocking lookups while other lists perform lease/start operations
(native creation still holds the map lock), production discovery and native
selection. Queries, sync delivery/forwarding, throttling/isolation/partition
controllers, semaphore and internal Thrift Matching transport remain incomplete.
No service migration gate is complete.
