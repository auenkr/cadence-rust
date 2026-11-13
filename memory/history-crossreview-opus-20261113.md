# History parity cross-review (Opus, independent) — 2026-11-13

Base: origin/master bd9161312. Read-only review (no cargo/go/docker). Codex review was not read.
Verified = I checked both sides myself. Plausible = reported by a sub-reviewer, read on one side only or partly.

## P1
1. **Wrong failure reason on history size/count limit** (verified). Rust `rust/crates/cadence-history/src/wiring/assembly/engine_decision_ops.rs:194` writes reason
   `"FAILURE_REASON_HISTORY_SIZE_EXCEEDS_LIMIT"`. Go `go/service/history/decision/checker.go:184` writes `common.FailureReasonHistorySizeExceedsLimit` = `"HISTORY_EXCEEDS_LIMIT"` (`go/common/util.go:107`).
   Scenario: a workflow exceeds the history count limit. Its WorkflowExecutionFailed event carries a reason that differs from Go, so SDK and client reason matching breaks. The correct constant exists in `decision/mod.rs:13` but this live path does not use it.
2. **Replication DLQ put/size/delete always use shard 0** (verified). The store is built with shard 0 at `rust/crates/cadence-history/src/wiring/assembly/env.rs:153` (`SqlWorkflowStore::new(.., 0, 0)`). `wiring/persistence/replication.rs:190-212` ignores `req.shard_id` and `_shard_id`. Page reads and range deletes do use the request shard.
   Go `go/common/persistence/sql/sql_execution_store.go` (PutReplicationTaskToDLQ / GetReplicationDLQSize / RangeDelete) uses `request.ShardID`.
   Scenario: a failed replication task on shard 7 is written under shard 0. ReadDLQ/MergeDLQ for shard 7 cannot see it, and the DLQ size for shard 0 counts every shard. The Cassandra path has the same shard 0 default (`history_execution_cassandra.rs:54`).
3. **RespondDecisionTaskFailed terminates the workflow on max attempts** (verified). Rust `wiring/assembly/engine_decision.rs:896-931` terminates with `DECISION_ATTEMPTS_EXCEEDS_LIMIT`. Go `go/service/history/decision/handler.go:253-291` only adds DecisionTaskFailed. The attempt check exists only in `failDecisionHelper` (`handler.go:867-894`), which only the completion path calls.
   Scenario: a worker that keeps failing decisions gets its workflow terminated by Rust. Go keeps retrying it.
4. **ShardOwnershipLost from persistence reaches clients as InternalServiceError** (plausible). Only `wiring/server/thrift_g2.rs:42` maps `HistoryError::PersistenceShardOwnershipLost`. `wiring/server/errors.rs:8-27,169` and `tchannel.rs:259` fall through to Internal.
   Go `go/service/history/handler/handler.go:2136-2144` returns ShardOwnershipLostError with the owner.
   Scenario: the shard is stolen mid-update. Frontend/matching see Internal instead of Aborted with an owner, so they do not redirect or retry.
5. **Persistence timeout/internal errors are not treated as "possibly successful"** (plausible). Rust `wiring/persistence/engine_ndc.rs:85,496` maps Timeout to internal, and `is_timeout()` is never true. Go `go/service/history/common/errors.go:32-47` and `execution/context.go:485,625,758,892` notify new tasks on any error outside the definitive list.
   Scenario: a DB write commits but the call times out. Rust skips the transfer/timer notification and the task waits for the next queue poll, so the workflow is delayed.
6. **No transient retry on create/update workflow execution** (plausible). Rust `engine_ndc.rs` `create_with_retry` / `update_with_retry` make one attempt. Go `execution/context.go:1181-1270` retries IsTransientError (InternalService, ServiceBusy, DBUnavailable) with backoff.
   Scenario: a MySQL deadlock or connection reset on UpdateWorkflowExecution fails the SDK RPC in Rust. Go retries it.

## P2
7. Size/blob-limit fail event uses `next_event_id - 1` as completedID (`engine_decision_ops.rs:193,202`). Go uses the DecisionTaskCompleted id (`checker.go:130-153`). This differs once earlier decisions in the batch have appended events.
8. `is_conflict_error` also includes CurrentWorkflowConditionFailed (`engine/deps.rs:146-151`). Go wraps only ConditionFailedError (`execution/context.go:1308`). Rust retries 5x and returns MaxAttemptsExceeded where Go returns Internal immediately.
9. DomainNotActiveError never uses the active-active form or ActiveClusters (`engine/deps.rs:352-366` vs `go/common/cache/domainCache.go:866-891`).
10. Decision Scheduled/Started/Failed handlers skip the active-domain check (`engine_decision.rs` vs `handler.go:108,157,253` getActiveDomainByWorkflow). On a passive cluster Rust succeeds where Go returns DomainNotActiveError.
11. DecisionTaskFailed on a closed workflow returns EntityNotExists. Go returns `workflow.ErrAlreadyCompleted` (`handler.go:278`). Error messages differ in case and trailing period ("Workflow execution already completed.").
12. queuev2 state is persisted lossily: only the per-queue min is kept, and virtual queues/predicates collapse on reload (`shard_adapters.rs:355-400`, `cadence-persistence/src/sqlblobs.rs:1478-1520` vs `go/service/history/shard/context.go:394-460`). The result is reprocessing, and the Rust->Go handover reads a state Go did not write.

## Checked, no difference found
Decision attribute checker (`decision/checker.rs`, `decision/mod.rs`) vs `checker.go`: activity timeout normalization, retry extension, timer, cross-domain, and length checks.
Timer gates, shard ack-level min/max and persist, replication fetcher/reader/ack manager/DLQ ack bounds, processing queue collection, transfer queue base.
StartWorkflow (reuse policy, dedupe, cron/delay), SignalWithStart, Terminate, Reset, Describe, PollMutableState, Query, RecordActivityTaskStarted.

Not fully covered: child parent-close-policy execution, retry/cron continue-as-new details, failover processors, taskdlq processor.
