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

## Part 3 (branch history-msbparity3)

Compared Go vs Rust line by line: mutable_state_builder_methods_{timer,signal,child_workflow,completed,cancellation}.go
vs execution/mutable_state_methods_*.rs, history_builder.go event constructors, action tag/error text
(createInternalServerError / createCallerError / ErrWorkflowFinished). Timer, completed, cancellation, common: no gap.

Gaps fixed (each with a unit test):
1. SignalExternalWorkflowExecutionInitiated lacked `childWorkflowOnly` (thrift field 70, proto, API type, history builder,
   persistence builder); transfer task `TargetChildWorkflowOnly` now derives from the event attribute.
2. Typed signal-failed event dropped `control`.
3. StartChildWorkflowExecutionInitiated: empty domain falls back to parent domain (production path), ParentClosePolicy
   unset persists ABANDON (both paths), typed path now carries cron_overlap_policy.

Known unfixed gap: `active_cluster_selection_policy` missing from Rust child decision/initiated attributes.
Not re-read in this part: mutable_state_builder_methods_started/timedout, mutable_state_util, core builder (transactions, version histories, sticky).

Evidence (head 82723f8f01d00661c4be90105aa0f6d5c1312d5b):
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43541365304 success
- gate mysql/grpc two_history: https://github.com/auenkr/cadence-rust/actions/runs/43542229604 success, GATE SUMMARY failed: []
- gate cassandra/tchannel two_history: https://github.com/auenkr/cadence-rust/actions/runs/43542232719 success, GATE SUMMARY failed: []

## Part 4 (branch history-msbparity4)

Compared Go vs Rust line by line: mutable_state_builder_methods_started.go (all 3 fns), mutable_state_builder_methods_timedout.go,
mutable_state_util.go (FailDecision/ScheduleDecision/FindAutoResetPoint/GetChildExecutionDomain{Name,ID}/trimBinaryChecksums),
checkMutability/ErrWorkflowFinished in mutable_state_builder.go. Util, timedout, ReplicateWorkflowExecutionStartedEvent field mapping: no gap.

Fixed (unit test noted):
1. `active_cluster_selection_policy` on StartChildWorkflowExecutionDecisionAttributes (thrift 170, proto field 17) and
   ...InitiatedEventAttributes (thrift 210, proto field 22): API structs, thrift from/to, proto in/out (decision, history, cross-cluster),
   typed + production history paths, history builder. Removed two `unimplemented` rejections in proto. Tests: thrift round trip
   (`convert.rs`), `initiated_event_carries_active_cluster_selection_policy`.
2. Typed `AddWorkflowExecutionStartedEvent` passed generateDelayedDecisionTasks=true; Go passes false (backoff timer is made by
   AddFirstDecisionTaskScheduled). Test `add_started_event_does_not_generate_delayed_decision_tasks`.
3. Production close/terminate/continue-as-new/timeout on a finished workflow returned `<tag> operation failed`; Go checkMutability returns
   ErrWorkflowFinished "invalid mutable state action: mutation after finish" (engine_continue/timer/events/decision_ops). No dedicated unit test
   (engine state not constructible cheaply); covered by gates.

Remaining gaps / not re-read: mutable_state_builder.go core beyond checkMutability (StartTransaction/CloseTransactionAs*, version histories, sticky,
pending-info maps, ~2400 lines). Typed `AddWorkflowExecutionStartedEvent` does not write events cache (perf only).

Evidence (head 3508b67a42a72835554431e20bf5b8fde34cd4f3):
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/43544749418 success; cadence-proto .../43544752718 success
- gate mysql/grpc two_history .../43545549870 success, GATE SUMMARY failed: []
- gate cassandra/tchannel two_history .../43545557499 success, GATE SUMMARY failed: []

## Part 5 (branch history-msbparity5)

Compared Go mutable_state_builder.go vs Rust (execution/mutable_state_builder.rs typed path, wiring/assembly/engine_close.rs production path):
StartTransaction, CloseTransactionAsMutation/Snapshot, prepareCloseTransaction ordering (policy check, buffered-events limit, workflow reset,
flush, activity/user timer tasks), cleanupTransaction, prepareEventsAndReplicationTasks / eventsToReplicationTask / syncActivityToReplicationTask
(history task then sync-activity tasks, contradicting-version error text, passive guard), updateWithLastWriteEvent/updateWithLastFirstEvent,
closeTransactionWithPolicyCheck, generateChecksum, UpdateCurrentVersion, GetStartVersion, GetLastWriteVersion, VersionHistory.AddOrUpdateItem,
validateNoEventsAfterWorkflowFinish, ClearStickyness. Mutation/snapshot persisted fields and replication task order: no gap.

Fixed (unit test each):
1. Production close (`engine_close.rs`) updated the current version history inline and silently accepted a lower version / non-increasing event id;
   Go `AddOrUpdateItem` returns BadRequest "cannot update version history with a lower version ..." / "cannot add version history with a lower event id ...".
   Now `add_or_update_version_item`. Test `version_history_update_matches_go_add_or_update_item`.
2. Typed `update_current_version` diverged from Go: no completed-workflow branch (Go uses last write version), and running case used incoming version
   directly instead of max(last item version, incoming) unless forced. Test `update_current_version_matches_go`.

Remaining / not re-read: Load/CopyToPersistence, SetVersionHistories, sticky task list accessors (IsStickyTaskListEnabled TTL), pending-info map accessors,
startTransactionHandleDecisionFailover (exists in mutable_state_failover.rs / engine_failover.rs, not diffed this round), production close does not
call Go's closeTransactionWithPolicyCheck for passive as no-op only by `active` flag (equivalent).

Evidence (head 4c002c4bdfee81cfd36b9202c5dc24399316d4d1, memory-only commit follows):
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/43547380963 success
- gate mysql/grpc two_history https://github.com/auenkr/cadence-rust/actions/runs/43547399664 success, GATE SUMMARY failed: []
- gate cassandra/tchannel two_history https://github.com/auenkr/cadence-rust/actions/runs/43547403011 success, GATE SUMMARY failed: []

## Part 6 (branch history-msbparity6)

Compared Go mutable_state_builder.go vs Rust: Load, CopyToPersistence, fillForBackwardsCompatibility, SetVersionHistories, IsStickyTaskListEnabled
(typed `mutable_state_methods_decision.rs` + production `engine_state.rs`: `!now.After(lastUpdated+ttl)` matches), pending-info accessors
(GetActivityInfo/ByActivityID, child, request-cancel, signal, pending timer infos, GetUserTimerInfoByEventID, GetPendingRequestCancelExternalInfos:
simple map lookups, no gap), reorderAndFilterDuplicateEvents, startTransactionHandleDecisionFailover vs `engine_failover.rs` (logic identical, all 5 cases).

Fixed (unit test each):
1. `reorder_and_filter_duplicate_events` seeded its uniqueness set with the transaction's committed events (Go starts empty) and only keyed 9 completion
   types. Now keys exactly Go's set (ActivityTaskStarted with attempt, ChildWorkflowExecutionStarted, StartChildWorkflowExecutionFailed,
   ExternalWorkflowExecutionCancelRequested, RequestCancelExternalWorkflowExecutionFailed, ExternalWorkflowExecutionSignaled,
   SignalExternalWorkflowExecutionFailed + completions). Test `reorder_filters_duplicates_and_moves_completions_to_tail_like_go`.
2. Load did not reorder/dedup persisted buffered events (Go: `reorderAndFilterDuplicateEvents(state.BufferedEvents, "load")`); fixed in typed `load` and
   production `engine_state.rs`. Test `load_reorders_buffered_events_like_go`.
3. Added missing `engine_failover` unit tests (in `engine_close.rs` tests): noop cases, version mismatch, active->passive fail+reschedule, case 3/4 buffered-event
   errors, case 5. Tests `failover_*`.

Remaining gaps: Rust Load does not clear checksum on `shouldInvalidateChecksum` in the typed path (production path does it in `verify_loaded_checksum`);
fillForBackwardsCompatibility (DomainNameDEPRECATED -> DomainID) not ported (legacy pre-#4601 data only). No other gaps found in mutable_state_builder*.go.

Evidence (head = commit "clippy fix in reorder test"):
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/43549470698 success
- gate mysql/grpc two_history https://github.com/auenkr/cadence-rust/actions/runs/43550264195 success, GATE SUMMARY failed: []
- gate cassandra/tchannel two_history https://github.com/auenkr/cadence-rust/actions/runs/43550267251 success, GATE SUMMARY failed: []

## Part 7 (branch history-msbparity7)

Closed the two Part 6 typed Load gaps:
1. Added a checksum-aware typed load path that receives the persisted snapshot checksum and configured invalidation cutoff. It clears the checksum when `last_updated_time_unix_nanos < cutoff_secs * 1e9`, matching Go `shouldInvalidateChecksum`. Unit test: `typed_load_clears_checksum_before_invalidation_cutoff_like_go`.
2. Ported Go `fillForBackwardsCompatibility`: typed child records now retain the deprecated persisted domain name, and typed Load resolves it to a domain ID only when the ID is empty. Lookup failures assign the empty result as Go does. Persistence bridge reads/writes the deprecated name. Unit test: `typed_load_fills_legacy_child_domain_id_like_go`.

The required mixed gate initially exposed an additional API mismatch: Rust propagated an invalid DLQ replication-task conversion as INTERNAL, while Go logs the per-workflow error and returns an empty successful response for that workflow. Matched the Go handler behavior to make the full SDK/admin probe gate pass.

Evidence on final head `e04123b0cbbffbf1f155aec3b381e1b97b4543c2`:
- `rust-ci.yml` cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43556390443 — success.
- `history-mixed-gate.yml` MySQL/gRPC, `two_history=true`: https://github.com/auenkr/cadence-rust/actions/runs/43557102789 — success; `GATE SUMMARY failed: []`; gate exit code 0.
- Rust binary SHA256: cadence-server `4bb7ffdc148290b5626f319b65fcb8a08c3f3108ef16b44fef372f3be4b37c98`; cadence-history `a56ac742cc5e0e01b8d22f939da7c453bddb6ffb2e1f88fdc8ec04358f2abab0`.
- First mixed gate attempt https://github.com/auenkr/cadence-rust/actions/runs/43554959188 failed only `admin-probe-get-dlq-replication-messages-bad-type`; that Go/Rust handler mismatch was fixed before the passing rerun above.
