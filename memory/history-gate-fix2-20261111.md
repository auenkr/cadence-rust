# history-gate-fix2 (2026-11-11)

Branch `history-gate-fix2` (from master e7a6f053f). Baseline gate run 43000919671: 166/171 (mysql, grpc, single History).

## Fixes
1. **sdk-refresh-workflow-tasks** ("MutableStateTaskRefresher is not wired"): new `wiring/assembly/engine_refresher.rs` (`EngineTaskRefresher`, port of Go `RefreshTasks` over `EngineMutableState`), wired in `engine_factory.rs`. Follow-up: it initially read the completion/initiated events by `next_event_id-1` / `id`, which gave "history not found" when the event is not the first of its batch; now uses `ev_completion_event()` and `initiated_event_batch_id`.
2. **sdk-api-poll-activities timeout / sdk-driver-exit**: root cause was `scheduled_event_batch_id` (and `initiated_event_batch_id`, `completion_event_batch_id`) set to the DecisionTaskCompleted event id. Go stores the first event id of the history batch; after a failed decision the batch starts at the transient decision-scheduled event, so `RecordActivityTaskStarted` read `[completed_id, sid+1)` and got "Workflow execution history not found" (matching then never delivered the task). Fixed in `engine_decision_ops.rs` (first pending event id). Added a warn log for RecordActivityTaskStarted failures in `grpc_g2.rs`.
3. **admin-probe-get-cross-cluster-tasks-deprecated**: `handler_impl.rs` returns INTERNAL "The cross-cluster feature has been deprecated." (respond variant without the period).
4. **admin-probe-reapply-events-unknown-workflow**: `reapply_events` now does GetEngine, then DeserializeBatchEvents (error text `DeserializeBatchEvents encoding: "thriftrw", error: ...`), then engine call (Go order).
5. Extra: `queue_deps.rs` visibility upsert is a no-op when search attributes contain `CadenceChangeVersion` (Go `IsNopUpsertWorkflowRequest`). The "Operation is not supported" errors for CloseExecutionTask of cron/schedule workflows (visibility upsert) were still seen in run 43008427072 and not investigated further.

## CI evidence
- Final code head `26994d029d68bf19d0267db205384df717c39ec4`: rust-ci (cadence-history) success https://github.com/auenkr/cadence-rust/actions/runs/43010402564
- Gate (mysql, grpc, two_history=false) on same head: https://github.com/auenkr/cadence-rust/actions/runs/43010408023 -> failure, failures=3 (was 5). All five originally targeted steps pass.
- Earlier gate runs: 43003615060 (2 failures: poll-activities, driver exit), 43005854377 (diagnostic logging), 43007710457 / 43008427072 (intermediate).

## Remaining gate failures (previously hidden behind the poll-activities abort; out of this task's scope)
- sdk-api-reset-workflow-execution: "WorkflowResetter is not wired".
- sdk-api-admin-workflow-describe (tchannel + grpc): CLI json unmarshal of mutable state (`BufferedEvents` serialized as a number, Go expects an event list).
- sdk-driver-exit: consequence of the above.
