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
1. FIXED in history-decfailover (2026-11-12). `G/execution/mutable_state_builder.go startTransactionHandleDecisionFailover` (+ `canReplicateEvents`): Rust `start_transaction` is a stub returning `Ok(false)`
   (`R/wiring/assembly/ndc_state.rs:317`, `ndc_context_hooks.rs:74`; engine path has no equivalent). Affects an in-flight decision whose version is lower than
   the current version after failover (standby -> active): Go fails it with `FailoverCloseDecision` and reschedules. NOT ported here (needs cluster metadata in the
   typed mutable state; ~100 Go lines, cases 1-5).
2. CLOSED in history-p1batch (2026-11-12, memory/history-p1batch-20261112.md): task-DLQ processor, archival signal client, semaphores on replay,
   failover marker persistence, request type/version in `WorkflowRequest`, ACSP blob decode in NDC `vector_clock_inputs` were all already present (file pointers there).
3. Archival JSON tag `firstScheduleTimeNano` (Go `*time.Time` RFC3339 string) fixed in history-p1batch (`go_history_batch`); no other history-event tag mismatch.

### P2
- `closeTransactionWithPolicyCheck` (DomainNotActive on active close when current version's cluster != current cluster): engine handlers check active status earlier; not in `engine_close.rs`.
- `validateNoEventsAfterWorkflowFinish`, `generateChecksum`/`verifyMutableStateChecksum` (checksum is not written/verified on the engine path), `DeleteTransferTasks/DeleteTimerTasks`, `logDataInconsistency`.
- Shard `task_notifier` / `failoverMarkerSkipReason` / `ReloadMutableState` have no same-named Rust function (behaviour is inlined in `shard_context.rs` and the engine cache; not behaviour-verified one by one).
- Engine/events/query/queue test volume (see audit 20261112 P2 and enginepkg*/exectests/standbytests notes). Metrics gaps: aggregator (FIXED, aggregator metrics branch), archival, cache metrics.
- Archival: streaming read (FIXED), blob size (FIXED), inline options (FIXED) by today's archival branches; remaining: no real S3/GCS network client; `nonRetriable`/ProgressManager not on inline path (matches Go inline).
- Dead `Unwired*` stubs: FIXED (removed). Stale `TODO(link)` comments remain.

## Package notes
decision: Go `task_handler.go handleDecision*` names differ (Rust `decision/task_handler.rs` dispatch); engine: only constructors/helpers missing; execution: 87 misses = constructors, retry wrappers, checksum, replication-task builders (inline in Rust `mutable_state_builder.rs`/`engine_close.rs`); ndc/queue/queuev2/replication/reset/resource/handler/query: constructors and private helpers only.

## P2 status after history-p2gaps (2026-11-12; details memory/history-p2gaps-20261112.md)
| Item | Status | Pointer |
|---|---|---|
| closeTransactionWithPolicyCheck | DONE | `wiring/assembly/engine_close.rs close_transaction_with_policy_check`, `engine_state.rs ClosePolicy`, wired in `engine_factory.rs` |
| generateChecksum (write path) | DONE (gen only) | `engine_close.rs close_transaction` (probability from `history.mutableStateChecksumGenProbability`, JSON blob like Go `SerializeChecksum`); verify-on-load + invalidateBefore OPEN |
| validateNoEventsAfterWorkflowFinish | DONE | `engine_close.rs validate_no_events_after_workflow_finish` |
| DeleteTransferTasks/DeleteTimerTasks | DONE | `execution/mutable_state_builder.rs` (unused by Go callers outside tests) |
| logDataInconsistency | PARTIAL | `mutable_state_builder.rs log_data_inconsistency` (log only; DataInconsistentCounter and Go call sites not wired) |
| failoverMarkerSkipReason | DONE | `wiring/assembly/failover.rs failover_marker_skip_reason` + drop log |
| task_notifier / ReloadMutableState | OPEN (inlined, not 1:1; shadow-mode drop log absent) | `shard_context.rs`, `engine_cache.rs load_workflow_execution` |
| archival client metrics | DONE | `archiver_client.rs` (`ArchiverClient` scope counters, wired in `env.rs`) |
| cache metrics (HistoryExecutionCache) | OPEN | Go `execution/cache.go` |
| frontend soak port-bind retry | DONE | `tests/gates/run-frontend-soak.py` |
| non-blob JSON tag mismatches | OPEN, not wire-affecting (SDKs use thrift/proto; only HTTP/JSON surfaces) | listed in history-p1batch note |
| real S3/GCS endpoints | OPEN, needs credentials | - |
