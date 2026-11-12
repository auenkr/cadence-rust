# History parity audit v2 (Go vs Rust), 2026-11-12

Baseline: origin/master f8914f198 + branch history-reaudit2. Method: (1) read memory/history-parity-audit-20261112.md and every
`memory/history-*-20261112.md` gaps section; (2) mechanical scan: every non-test Go func in `go/service/history/**` (excl. mock/generated/
simulation/testing) -> snake_case identifier searched in `rust/**` (script /tmp/fn.py; 234 misses, almost all constructor/private-helper
renames); (3) manual read of the misses that look like behaviour (mutable state close transaction, shard, workflow ctx).
No cargo/go/docker run; "ported" = Go logic present, validated by CI only.

## Fixed in this branch (known Go differences from earlier notes)
| Item | Change |
|---|---|
| ClusterAttributeNotFound text | `engine/deps.rs cluster_attribute_not_found_message`: Go `%s` of `*types.ClusterAttribute` (no String()) = `&{Scope Name}`, nil = `%!s(*types.ClusterAttribute=<nil>)`. Used by `active_cluster_manager.rs` and `engine_start.rs`. Unit test added. |
| Selection-policy cache eviction | `active_cluster_manager.rs PolicyCache` is now a port of Go `cache.lru` (access-ordered list; `evictExpiredItems` from the back; expiry `now > created+ttl`; Put on existing key refreshes createTime and moves to front; after insert evict back while `len > 1000`). Test `cache_evicts_in_access_order_like_go`. |
| Archival blob nested omitempty | `cadence-common cloud_archiver/write.rs go_history_batch`: also drops `workflowExecutionStartedEventAttributes.attempt` when 0. A struct-by-struct comparison of Go `json` tags vs Rust serde (`rust/crates/cadence-api service_types.rs`, script /tmp/cmp.py) showed this is the only other omitempty mismatch reachable from `HistoryEvent` (others: request/response types not in history blobs). |
| Buffered events limit | Go `closeTransactionHandleBufferedEventsLimit` was missing from `wiring/assembly/engine_close.rs close_transaction` (active policy): when persisted buffered events >= `history.maximumBufferedEventsBatch`, force-close the in-flight decision (ForceCloseDecision) and schedule a new one. New `StateParams.max_buffered_events_batch` wired in `engine_factory.rs`. **P1 -> ported.** |

## Remaining gaps
### P0
None known.

### P1 (Go behaviour missing)
1. `G/execution/mutable_state_builder.go startTransactionHandleDecisionFailover` (+ `canReplicateEvents`): Rust `start_transaction` is a stub returning `Ok(false)`
   (`R/wiring/assembly/ndc_state.rs:317`, `ndc_context_hooks.rs:74`; engine path has no equivalent). Affects an in-flight decision whose version is lower than
   the current version after failover (standby -> active): Go fails it with `FailoverCloseDecision` and reschedules. NOT ported here (needs cluster metadata in the
   typed mutable state; ~100 Go lines, cases 1-5).
2. Task-DLQ processor, archival workflow signal client, semaphores on replay, failover marker persistence, request type/version in `WorkflowRequest`,
   ACSP blob decode in NDC `vector_clock_inputs`: unchanged from audit 20261112 section 2 (P1 rows 3,4,5,6,7,12); several were closed by later branches
   (p1b2..p1b5, retryreset, archgaps, acmanager) per their notes.

### P2
- `closeTransactionWithPolicyCheck` (DomainNotActive on active close when current version's cluster != current cluster): engine handlers check active status earlier; not in `engine_close.rs`.
- `validateNoEventsAfterWorkflowFinish`, `generateChecksum`/`verifyMutableStateChecksum` (checksum is not written/verified on the engine path), `DeleteTransferTasks/DeleteTimerTasks`, `logDataInconsistency`.
- Shard `task_notifier` / `failoverMarkerSkipReason` / `ReloadMutableState` have no same-named Rust function (behaviour is inlined in `shard_context.rs` and the engine cache; not behaviour-verified one by one).
- Engine/events/query/queue test volume (see audit 20261112 P2 and enginepkg*/exectests/standbytests notes). Metrics gaps: aggregator, archival, cache metrics.
- Archival: `HistoryBatchSource` reads whole history up front; blob size estimate via JSON; no real S3/GCS; `nonRetriable`/ProgressManager not on inline path.
- Dead `Unwired*` stubs; stale `TODO(link)` comments.

## Package notes
decision: Go `task_handler.go handleDecision*` names differ (Rust `decision/task_handler.rs` dispatch); engine: only constructors/helpers missing; execution: 87 misses = constructors, retry wrappers, checksum, replication-task builders (inline in Rust `mutable_state_builder.rs`/`engine_close.rs`); ndc/queue/queuev2/replication/reset/resource/handler/query: constructors and private helpers only.
