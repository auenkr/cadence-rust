# History cross-review fixes (branch history-xrfix) — 2026-11-13

Base origin/master 48b50a828. All Cargo work ran on CI only. Head SHA at gate dispatch: 8e5a3e440743a3c2d6c8000f64426be3f2e74504.
rust-ci green on 8e5a3e440: https://github.com/auenkr/cadence-rust/actions/runs/43400393142

| Finding | Status | Commit subject | Test |
|---|---|---|---|
| Opus 1 history-size failure reason | FIXED | history: Go history-limit failure reason ... | `history_limit_failure_uses_go_reason` |
| Opus 2 / DLQ always shard 0 | FIXED | same commit | `dlq_shard_tests` (replication.rs) |
| Opus 3 DecisionTaskFailed terminates | FIXED | same commit | `respond_decision_failed_over_max_attempts_does_not_terminate` |
| Opus 4 / Codex 8 ShardOwnershipLost mapping + owner | FIXED (partial: persistence-layer ShardOwnershipLost carries an empty owner, the shard is unknown there; the controller path carries the owner) | history: shard ownership owner field, typed gRPC error details | controller owner assert, `typed_detail_tests`, thrift_g3 field-20 assert |
| Codex 7 gRPC typed error details, CurrentBranchChanged | FIXED | same commit | `typed_detail_tests` |
| Codex 3 async events bypass buffering | FIXED | history: Go buffering rules for async events | `buffer_tests::async_results_are_buffered_like_go` |
| Codex 4 CancelTimer vs buffered TimerFired | FIXED | same commit | `cancel_timer_retracts_buffered_timer_fired` |
| Codex 5 typed replay ms read as ns | FIXED | history: typed replay ms->ns at production callers | `millis_to_nanos_scales_event_timestamps` |
| Codex 9 production replay rejects semaphore events | FIXED | same commit | `typed_replay_supports_semaphore_events` (obsolete "unsupported" test removed) |
| Codex 6 archival JSON timestamp ms vs ns | FIXED (shared `go_history_batch` scales `timestamp` ms -> ns for both inline and cloud archivers) | archival: history event timestamp ms->ns in Go JSON | `go_history_batch_scales_timestamp_ms_to_ns` |
| Codex 2 NDC rejects every non-start batch | FIXED (load + active-failover-version `UpdateCurrentVersion` + decision failover StartTransaction, flush as active, re-check) | history: NDC non-start batch loads with StartTransaction | `persistence_methods_and_task_version_load...` now loads with a task version |
| Opus 5 timeout / maybe-committed not "possibly successful" | FIXED (persistence Timeout now maps to `HistoryError::PersistenceTimeout`, so `is_timeout()` is true and tasks are notified; clients still see Internal) | history: persistence timeout maps to possibly-successful | `persistence_timeout_is_possibly_successful` |
| Opus 6 no transient retry on create/update | FIXED (retry_transient: Internal + ServiceBusy, exponential backoff, never on timeout; request rebuilt per attempt) | same commit | `transient_errors_retry_but_timeouts_do_not` |
| Codex 1 active writes omit outbound replication tasks | OPEN — large cross-cutting change (HistoryReplicationTask/SyncActivityTask generation in engine close paths engine_close/engine_cache/engine_start, category 3); needs its own branch and mixed replication gate | – | – |
| Codex P2 cache eviction double lock | OPEN | – | – |
| Opus P2 7 completedID in size-limit fail event | OPEN | – | – |
| Opus P2 8 is_conflict_error includes CurrentWorkflowConditionFailed | OPEN | – | – |
| Opus P2 9 active-active DomainNotActive form | OPEN | – | – |
| Opus P2 10 decision handlers skip active-domain check | OPEN | – | – |
| Opus P2 11 DecisionTaskFailed on closed workflow error type | OPEN | – | – |
| Opus P2 12 queuev2 lossy persistence | OUT OF SCOPE | – | – |

Fix details are in the commits; no REJECTED findings (every P1 examined was confirmed against Go).

## Gate runs (history-mixed-gate.yml, head 8e5a3e440)
- mysql / grpc / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402455019 — success, `GATE SUMMARY failed: []`
- mysql / grpc / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402459685 — success, `GATE SUMMARY failed: []`
- mysql / tchannel / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402464585 — success, `GATE SUMMARY failed: []`
- mysql / tchannel / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402469340 — success, `GATE SUMMARY failed: []`
- postgres / grpc / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402474594 — success, `GATE SUMMARY failed: []`
- postgres / grpc / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402479841 — success, `GATE SUMMARY failed: []`
- postgres / tchannel / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402485345 — success, `GATE SUMMARY failed: []`
- postgres / tchannel / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402490316 — success, `GATE SUMMARY failed: []`
- cassandra / grpc / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402495026 — success, `GATE SUMMARY failed: []`
- cassandra / grpc / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402500329 — success, `GATE SUMMARY failed: []`
- cassandra / tchannel / two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43402504942 — success, `GATE SUMMARY failed: []`
- cassandra / tchannel / two_history=false: https://github.com/auenkr/cadence-rust/actions/runs/43402510201 — success, `GATE SUMMARY failed: []`

All 12 combos green on head 8e5a3e440743a3c2d6c8000f64426be3f2e74504 (no merge needed: origin/master had no newer commits).
Note: Codex P1 #1 (outbound replication tasks) is OPEN, so mixed-cluster replication behaviour is not covered by these gates.
