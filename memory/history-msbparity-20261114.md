# history-msbparity (2026-11-14), branch `history-msbparity`

Static line-level comparison (no local cargo/go/docker). **PARTIAL** — not every listed file was fully re-read, see "Not covered".

## Result per Go file / function

| Go | Rust | Result |
|---|---|---|
| mutable_state_decision_task_manager.go L133-884 (all Replicate*/Add* decision fns, FailDecision, DeleteDecision, UpdateDecision, GetDecisionInfo, GetDecisionScheduleToStartTimeout, CreateTransientDecisionEvents, afterAddDecisionTaskCompleted) | execution/mutable_state_methods_decision.rs | MATCH on state/attrs/tasks (attempt reset, transient scheduled/started, timestamps, original-scheduled timestamp, reset-request insert, sticky-no-increment on schedule-to-start). **No decision-started/completed/scheduled divergence found**, so nothing here explains the soak re-start loop (sched=2 started 14k times). Note Rust `ReplicateDecisionTaskCompletedEvent` uses `env.max_reset_points` (0 -> default), Go uses config only when domain entry present; equivalent. |
| mutable_state_builder_methods_decision.go L36-354 (wrappers, CheckResettable, addBinaryCheckSumIfNotExists) | same file | MATCH. CheckResettable omits the semaphore-hold branch (L181-185) — rust note says semaphores not modelled in `pending_*`; not changed. |
| log/tag/values.go L40-85 action tags -> `createInternalServerError`/`createCallerError` message `"<tag> operation failed"` (builder.go L2223-2228) | execution/mutable_state_methods_{decision,activity,cancellation,child_workflow,signal,started}.rs, semaphore.rs | **FIXED**: Rust used invented tags ("add-decision-task-started-event", "add-activity-task-*", "add-child-workflow-*", "add-external-workflow-*", "add-request-cancel-external-workflow-failed-event", "add-workflow-execution-started-event", "SemaphoreAcquired"). Now the exact Go tag strings (e.g. `add-decisiontask-started-event`, `add-activitytask-timed-event`, `add-childworkflow-initiation-failed-event`, `add-externalworkflow-cancel-failed-event`, `add-externalworkflow-signal-requested-event`, `add-workflow-started-event`, `add-semaphore-acquired-event`). Error text is returned to SDK/callers. |
| mutable_state_builder_methods_activity.go L205-746 | execution/mutable_state_methods_activity.rs | Control flow/errors MATCH. **Divergences found, NOT fixed** (see below). |

Unit test: `decision_tests::go_action_tag_error_messages` (mutable_state_methods_decision.rs).

## Divergences found but NOT fixed (need follow-up; production path is wiring/assembly/engine_events.rs, not the typed fns above)
1. Go `AddActivityTaskStartedEvent`/`addTransientActivityStartedEvent`/`AddActivityTaskTimedOutEvent` always set `FailureOptions{FailureCategory: ai.LastFailureCategory.Ptr(), NextRetryIntervalSeconds}` (Go L350-353, 376-379, 539-542); typed Rust sets `failure_category: None`. engine_events.rs L268 only emits options when category!=0 or interval!=0 (Go always emits) and maps 0->Poll,1->Standard,2->Fatal; Go enum is Standard=0 (shared.go L184). Check Go ordering vs Rust `category_from_i32` before touching.
2. Go `RetryActivity` stores `ai.LastFailureCategory`; typed Rust `retry_activity` does not (engine_events.rs L491 does).
3. Go `ReplicateActivityTaskScheduledEvent` sets `TaskListKind` and has pending-activity count limit checks (L226-246, `ErrTooManyPendingActivities`); typed Rust skips (documented in its comment); production path not verified.
4. Go `AddActivityTaskScheduledEvent` returns `ActivityLocalDispatchInfo` for local dispatch (L264-267); handled in engine_decision_ops, not re-read.

## Not covered (no line-level re-read done)
mutable_state_builder.go core (StartTransaction/CloseTransactionAs*, version histories, sticky, pending-info maps), mutable_state_util.go, methods_{cancellation,child_workflow,completed,signal,started,timedout,timer,semaphore}.go (only action tags checked), wiring/assembly/engine_events.rs.

## CI (head SHA 162747db50c991ad1855784d88938e0354d1f26b; code identical, memory-only commit follows)
- rust-ci package=cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43530378440
- history-mixed-gate two_history=true, all success AND `GATE SUMMARY failed: []`: mysql/grpc 43531231815, mysql/tchannel 43531244961, postgres/grpc 43531257413, postgres/tchannel 43531270912, cassandra/grpc 43531283533, cassandra/tchannel 43531295832 (https://github.com/auenkr/cadence-rust/actions/runs/<id>)

# Part 2 (branch `history-msbparity2`)

## Fixed (each with unit test)
| Item | Change |
|---|---|
| (a) FailureOptions + category enum | Go persisted `FailureCategory` is Standard=0, Poll=1, Fatal=2; Rust API enum is Poll=0, Standard=1 (wire). Added `execution/retry.rs::failure_category_{to,from}_go_i32` (test `go_persisted_category_mapping`). Started (typed + engine_events non-transient + transient) and TimedOut (typed + engine_timer) events now ALWAYS carry `FailureOptions{category, nextRetryInterval}` and `last_failure_reason`/`details` like Go (previously None/omitted when zero). |
| (b) LastFailureCategory | typed `ActivityInfo.last_failure_category` (Go encoding) set in typed `retry_activity` and `replicate_activity_info`; bridge/replication builders round-trip it (was preserved-from-old only, and engine path stored the Rust enum value, wrong for Go readers). Test `retry_stores_go_encoded_category...`. |
| (c) check_resettable | semaphore pending holds -> BadRequest "...pending semaphore holds." (test `check_resettable_rejects_pending_semaphore_holds`). |
| (d) TaskListKind | typed ActivityInfo.task_list_kind set on replicated scheduled; engine_decision_ops schedule_activity now sets `task_list_kind` (was always 0 -> Go sticky kind lost). Test `scheduled_activity_records_task_list_kind`. |
| (e) pending activity limit | Already in production path (engine_decision_ops::schedule_activity, validation-enabled gated, text compared by task_handler); typed path left without (single check, no double-check). |
| Engine error text | Production engine `invalid history action ...` messages replaced by Go `<action-tag> operation failed` via `wiring/assembly/mod.rs::invalid_history_action_message` (activity/decision/timer/child/workflow close paths). Test `invalid_action_uses_go_tag_text`. |

## Re-read coverage
Re-read: engine_events.rs activity production (completed/failed/canceled/started/retry attrs match Go), activity method file. NOT re-read line-by-line this round (no divergence checked): mutable_state_builder.go transactions/version histories/sticky, mutable_state_util.go, timer/signal/child/completed/cancellation files beyond tags. Remaining Go-reader compatibility: category persisted ints now Go-compatible; thrift FailureOptions field 10 unchanged. Go<->Rust blob swap gate passed.

## CI (head SHA da38f70ad35f7a39fc78dc6e5994acc5d060f898; code identical to tested head, memory-only commit)
- rust-ci cadence-history: success https://github.com/auenkr/cadence-rust/actions/runs/43535978859
- history-mixed-gate two_history=true, success AND `GATE SUMMARY failed: []`: mysql/grpc 43536901509, mysql/tchannel 43536904623, postgres/grpc 43536908255, postgres/tchannel 43536911688, cassandra/grpc 43536914702, cassandra/tchannel 43536917966
- history-swap mysql/grpc: success, `GATE SUMMARY failed: []` 43536921859
