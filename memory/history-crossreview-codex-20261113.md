# Independent History cross-review — Codex — 2026-11-13

Reviewed base: `bf1cba71c6cdef2f528dee9beef75c164e61dda9` (`origin/master`).
Review branch: `history-xreview-codex`.

Read `AGENTS.md`, `go/AGENTS.md`, and `memory/history-parity-audit-20261113-final.md`.
This is a source-only correctness review, not a parity certification. No source code was
changed; no Cargo, Go, Docker, builds, tests, or gates were run locally or dispatched.
The scenarios below are source-derived reproductions, not claims of executed tests.
The audit's existing P2 exclusions are omitted. Line numbers refer to the reviewed base.

Found **9 P1 issues and 1 P2 issue**, with no P0 finding.

## 1. P1 — Normal active writes never persist outbound replication tasks

- Rust: `rust/crates/cadence-history/src/wiring/assembly/engine_close.rs:243`,
  `rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs:417`,
  `rust/crates/cadence-history/src/wiring/assembly/engine_start.rs:332`.
- Go: `go/service/history/execution/mutable_state_builder.go:1749`,
  `go/service/history/execution/mutable_state_builder.go:1774`,
  `go/service/history/execution/mutable_state_builder.go:1837`.
- Difference: Go closes an active transaction by generating HistoryReplicationTask rows
  for its event batches and SyncActivityTask rows for changed activities when replication
  is enabled. The production engine close result contains only transfer/timer tasks;
  its mutation defaults `tasks_by_category` to empty. Both the update writer and new-run
  snapshot writer add only categories 1 and 2. The separate typed/NDC implementation
  does not intercept `EngineExecutionContext::update_as_active`.
- Failure scenario: enable replication for a global domain spanning clusters A and B,
  start a workflow on Rust History in A, signal it and complete an activity. No category-3
  task for these writes is persisted, so the replication reader cannot ship the start,
  signal, completion, or heartbeat changes to B. A subsequent failover loses progress
  or cannot find the workflow. This also applies to normal starts/continue-as-new.

## 2. P1 — Every incoming non-start history batch is rejected before NDC conflict resolution

- Rust: `rust/crates/cadence-history/src/wiring/assembly/ndc_workflow.rs:579`;
  caller: `rust/crates/cadence-history/src/ndc/history_replicator.rs:111`.
- Go: `go/service/history/ndc/history_replicator.go:373`,
  `go/service/history/execution/context.go:350`.
- Difference: Rust unconditionally returns an internal error for `task_version.is_some()`.
  The production history replicator passes `Some(task.get_version())` for every batch
  whose first event is not WorkflowExecutionStarted. Go loads the state and runs
  StartTransaction with the incoming version, including decision failover handling.
- Failure scenario: Go cluster A replicates the initial start batch of a run to Rust
  cluster B, then sends its next decision/activity batch. The second batch always fails
  with `load with incoming task version needs StartTransaction decision failover` and
  retries without applying anything. Branch preparation, conflict resolution, and the
  missing-state reset branch are all unreachable for such batches, regardless of version.

## 3. P1 — Asynchronous timer and external-operation results bypass decision buffering

- Rust: `rust/crates/cadence-history/src/wiring/assembly/engine_events.rs:26`;
  event insertion: `rust/crates/cadence-history/src/wiring/assembly/engine_events.rs:72`.
- Go: `go/service/history/execution/mutable_state_builder.go:863` (especially its default case).
- Difference: Go buffers events except its explicit workflow-close/decision/decision-command
  exceptions. Rust uses an incomplete allowlist that omits TimerFired,
  StartChildWorkflowExecutionFailed, ExternalWorkflowExecutionSignaled,
  SignalExternalWorkflowExecutionFailed, ExternalWorkflowExecutionCancelRequested,
  RequestCancelExternalWorkflowExecutionFailed, and SemaphoreAcquired.
- Failure scenario: a workflow has a previously started timer and an in-flight decision
  triggered by a signal. The timer expires while the worker is processing that decision.
  Rust assigns TimerFired a permanent ID and persists it between DecisionTaskStarted
  and DecisionTaskCompleted, instead of buffering it. The running worker has not seen
  that event, and mutable-state buffered-event detection cannot account for it when
  deciding whether to schedule the follow-up decision. Go keeps the event buffered
  until that decision closes and then delivers it in the next task. External signal,
  cancel, and child-start failure results have the same ordering/delivery defect.

## 4. P1 — CancelTimer cannot retract an already buffered TimerFired event

- Rust: `rust/crates/cadence-history/src/wiring/assembly/engine_decision_ops.rs:450`.
- Go: `go/service/history/execution/mutable_state_builder_methods_timer.go:128`
  (lookup/retraction at lines 140–154).
- Difference: Rust only looks in the pending timer map. Go also searches and removes
  TimerFired from persisted buffered events, new buffered events, and the history builder
  when the timer has already left that map, then records TimerCanceled successfully.
- Failure scenario independent of finding 3: take over a Go-written execution with an
  in-flight decision and a persisted buffered TimerFired. The worker completes that
  decision with CancelTimer. Rust returns a BadRequest from the operation, which
  `decision/task_handler.rs:216` converts into CancelTimerFailed; the TimerFired stays
  buffered and is later delivered. Go removes the firing and records TimerCanceled.
  Fixing the buffering allowlist alone does not fix this cancellation race.

## 5. P1 — Typed replay interprets decoded milliseconds as nanoseconds

- Rust: `rust/crates/cadence-history/src/execution/mutable_state_methods_timer.rs:62`,
  `rust/crates/cadence-history/src/execution/state_builder.rs:111`.
- Go: `go/service/history/execution/mutable_state_builder_methods_timer.go:76`,
  `go/service/history/execution/state_builder.go:204`.
- Difference: `cadence-thrift/src/history_blob.rs:41` divides wire event timestamps by
  1,000,000. Both `SqlHistoryPager::page` (`wiring/persistence/engine_ndc.rs:380`, via
  `cadence-persistence/src/history_reader.rs:234`) and the replication batch decoder
  return those millisecond events directly to typed replay. Typed replay and the typed
  Replicate methods instead use the timestamp as nanoseconds. For example, TimerStarted
  computes `event.timestamp + timeout_seconds * 1_000_000_000` with no unit conversion.
  Decision scheduled/started times and activity/start-event times have the same boundary.
- Failure scenario: reset a workflow to a decision after it started a still-pending
  one-hour timer. An event timestamp of `1_800_000_000_000_000_000` ns is decoded as
  `1_800_000_000_000` and rebuilt into a timer expiry of `5_400_000_000_000` ns,
  in January 1970. Reset refreshes timer tasks from that row, so the one-hour timer
  fires immediately after reset instead of retaining its original deadline. This also
  corrupts replay-derived times during replication/rebuild.

## 6. P1 — Archival history JSON writes millisecond event timestamps into Go's nanosecond field

- Rust: `rust/crates/cadence-history/src/wiring/assembly/archiver_wiring.rs:68`
  (also `:119`), `rust/crates/cadence-common/src/cloud_archiver/write.rs:226`.
- Go: `go/common/archiver/filestore/historyArchiver.go:174`,
  `go/common/archiver/filestore/util.go:44`, `go/common/types/history.go:1207`.
- Difference: the archive source serializes events from `read_history_branch` directly
  with serde. That reader uses `history_blob::decode`, whose event timestamps are
  milliseconds. `go_history_batch` adjusts some JSON tags/zero values but never converts
  `timestamp` back to nanoseconds. Go JSON marshals the original int64 nanosecond timestamp.
  This is an archival blob value incompatibility, not the audit's excluded non-blob tags.
- Failure scenario: archive a closed workflow from Rust History to filestore (also
  affects the common cloud batch source), then read it with Go Frontend/SDKs. The archive's
  `timestamp` is smaller by 1,000,000 and is returned as a date near 1970. Event timing
  and replay time differ from the same history before archival. This requires no S3/GCS
  credentials and is separate from fixing typed replay in finding 5.

## 7. P1 — gRPC drops typed internal-service error details and branch-change mapping

- Rust: `rust/crates/cadence-history/src/wiring/server/errors.rs:119`, `:165`, `:177`;
  branch-change producer: `rust/crates/cadence-history/src/engine/poll_mutable_state.rs:109`.
- Go: `go/common/types/mapper/proto/errors.go:313`, `:334`, `:341`, `:358`
  (decoding at `:143`, `:173`, `:201`).
- Difference: RetryTaskV2Error and ShardOwnershipLostError get Code::Aborted but no
  protobuf detail. EventAlreadyStarted also gets no detail. CurrentBranchChanged is
  not classified at all, so becomes Internal and loses its branch token. Go encodes
  these using the **shared/v1** protobuf detail types, which its decoder uses to recover
  the concrete errors. Rust's detail builder handles none of them and its type URL
  construction only supports `uber.cadence.api.v1.*`.
- Failure scenario: a Go service calls Rust SyncActivity for a run whose history is
  missing. NDC correctly constructs RetryTaskV2Error, but gRPC emits an untyped ABORTED
  status. Go cannot recover the resend interval/run identifiers and its replication
  executor cannot trigger history resend (`go/service/history/replication/task_executor.go:174`).
  Likewise a Go history poller encountering an NDC branch change receives Internal
  instead of CurrentBranchChangedError with the replacement token, breaking recovery.

## 8. P1 — Shard ownership responses cannot carry the owner needed for redirects

- Rust: `rust/crates/cadence-history/src/wiring/assembly/controller.rs:42`,
  `rust/crates/cadence-api/src/errors.rs:41`,
  `rust/crates/cadence-history/src/wiring/server/thrift_g3.rs:70`.
- Go: `go/service/history/shard/controller.go:593`,
  `go/service/history/handler/handler.go:2136`, `go/client/history/client.go:1177`.
- Difference: Rust's ShardOwnershipLostError is a message-only type. `check_ownership`
  puts the owner address into human-readable message text, and Thrift serializes only
  Message, never Owner. Go carries the resolved host address in the Owner field and
  its client follows that field; it does not parse the message. Persistence ownership
  errors also remain `HistoryError::PersistenceShardOwnershipLost` and fall through to
  InternalServiceError rather than Go handler's ownership conversion.
- Failure scenario: a Go Frontend/Matching/history client has a stale ring entry after
  shard movement and sends an RPC to the previous Rust owner. On Thrift it receives
  ShardOwnershipLostError with an empty Owner; `executeWithRedirect` tries to resolve
  that empty address and fails instead of forwarding to the known new owner. On gRPC,
  finding 7 additionally prevents recognizing the ownership exception at all.

## 9. P1 — Production typed replay rejects all semaphore events

- Rust: `rust/crates/cadence-history/src/execution/state_builder.rs:197`;
  production callers: `wiring/assembly/ndc_adapters.rs:155`, `wiring/assembly/ndc_rebuild.rs:217`.
- Go: `go/service/history/execution/state_builder.go:448`.
- Difference: the production typed replay entry returns TypedReplayNotYetSupported for
  SemaphoreAcquireInitiated, SemaphoreAcquired, and SemaphoreReleased. Go replays each
  into pending semaphore state. The other, untyped StateBuilder entry having semaphore
  support does not fix the production typed entry used by reset and NDC rebuild.
- Failure scenario: a workflow acquires and releases a semaphore, then later requests
  reset to a decision after those events. The Rust reset rebuilder aborts on the first
  semaphore event and the reset returns InternalServiceError. Even a completely released
  semaphore blocks the reset. Go rebuilds that same history successfully. Conflict
  rebuilds and replication batches containing these valid events are affected too.

## 10. P2 — Cache eviction can create two workflow locks for the same key

- Rust: `rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs:572`, `:604`, `:608`.
- Go: `go/service/history/execution/cache.go:117`, `:279`, `:298`.
- Difference: eviction determines whether an entry is in use by counting references
  to `e.lock`, while `entry()` returns a reference to the enclosing `Arc<Entry>`.
  The lock Arc is cloned only afterward, outside the entries-map mutex. Go pins the
  cache entry on Get/PutIfNotExist before attempting the workflow lock.
- Failure scenario: with the shard cache at its 10,000-entry capacity, thread A gets
  workflow W's Arc<Entry> and is preempted before line 608. Thread B misses on another
  workflow and evicts W because its lock strong count is still 1. Thread C requests W
  and installs/acquires a new entry. A resumes and acquires the old entry's lock.
  A and C now both hold supposedly exclusive workflow contexts for W and can perform
  competing mutations/history appends. Persistence CAS may reject a competing write,
  but it does not restore Go's single-context execution/locking contract. The same
  race can split the empty-run current-workflow lock. Pinning must happen while the
  map mutex is held; checking the mutex Arc count after checkout is insufficient.

## Validation limits

All findings above were traced through concrete production adapters, not inferred only
from stale TODO comments or test doubles. No runtime reproduction was executed. Existing
mixed-gate success in the final audit does not exercise the global-domain replication,
reset-with-pending-timer/semaphore, archival timestamp, typed-error, or cache-race scenarios
identified here. Focused CI/mixed-service reproductions are still needed when fixing them.
