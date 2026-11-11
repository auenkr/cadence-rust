# History parity audit (Go vs Rust), 2026-11-12

Baseline: origin/master 84658e5b3. Method: reads of memory/history-parity-audit-20261111.md plus the 14 follow-up reports
(decisiongaps2, ndctests2, parentclose, queuegaps, auditp0, auditp1, enginetests, cassandra, cassgaps, enginepkg, enginepkg2,
churnflake, restartsig, taskwritelock) and `nice -n 15 ionice -c3 grep` over `rust/crates/cadence-history/src`
for `todo!|unimplemented!|TODO|Unwired|not implemented|not supported|not wired|not ported|Nop|Noop|Ok(Vec::new())`.
No cargo/go/docker was run; "ported" means code with the Go logic is present, not behavior-verified.
Go root `G/` = `go/service/history`; Rust root `R/` = `rust/crates/cadence-history/src`.

## 0. File coverage
Every non-test Go file in the 26 `G/` packages still has a Rust counterpart (section 2 of the 2026-11-11 audit is unchanged;
`wiring/assembly/*` supplies the adapters). Remaining non-parity is therefore behavior stubs, listed below.
`TODO(link)` markers in `engine/deps.rs`, `queue/deps.rs`, `task/deps*.rs`, `decision/*.rs`, `reset/*.rs`, `queuev2/deps.rs:533`,
`events/mod.rs:71` are stale doc comments on trait shims (the traits are implemented in `wiring/assembly`), not missing behavior.
`unimplemented!()` hits (`ndc/transaction_manager.rs:546`, `ndc/workflow_resetter.rs:217-235`) are inside `#[cfg(test)]` mocks.
`Unwired*` structs (`engine_factory.rs:37-75`, `replication_builders.rs:308`) are defined but never constructed in production
(grep: only their own definitions) - dead code, delete in a cleanup (P2).

## 1. Closed since the 2026-11-11 audit
| 2026-11-11 item | Closed by |
|---|---|
| P0-1 GetReplicationMessages, P0-2 RemoveTask, P0-3 RatelimitUpdate | history-auditp0 (aggregator metrics still not emitted, P2) |
| P0-4 gRPC CloseShard/DescribeQueue/RemoveTask/ResetQueue | was stale: already served (auditp0) |
| P0-5 close tasks on replicate path, P0-6 delayed decision tasks | already wired; tests added (auditp0) |
| P0-7 Cassandra stores | history-cassandra + history-cassgaps (execution store, history v2, CreateHistoryTasks, SelectWorkflowTimerTasks, semaphore map) |
| P1-1 local activity dispatch, search-attr task, client version | history-decisiongaps2 (generator caveat fixed here, section 4) |
| P1-2 insertWorkflowRequest on replicate signal | auditp1 (request payload/type still empty, see P1-7) |
| P1-4 critical-attempts log/metric, terminate log | auditp1 |
| P1-5 replication queue state unsupported | matches Go (auditp1) |
| P1-6 DescribeQueue state string | auditp1 `queue_state_string.rs` |
| P1-8/9 dynamic-config fallback, membership | auditp1 (OpenFeature arm still bails, P2) |
| P1-10 adapter stubs: EngineEventsReapplier | auditp1 (other stubs below) |
| P1-12 WRR scheduler, reinjector, timer standby load_workflow | history-queuegaps (NoSQL `create_history_tasks`: cassgaps) |
| P1-13 parent close policy worker client | history-parentclose (no TLS, uses cluster rpcAddress) |
| P1-14 inter-host TLS client | history-ndctests2 (not gate-verified with TLS) |
| P1-15 error mapping | auditp1 `wiring/server/errors.rs` |
| Task-id/read-level race (restartsig, ~25% gate flake), AlreadyStarted retry (churnflake) | history-restartsig, history-taskwritelock, history-churnflake |
| P2 tests | enginepkg(+2): engine 118 tests, enginetests: shard 49, replication ~96, ndctests2 |
| Cron "Operation is not supported" | not a bug: Go SQL visibility behaves identically (parentclose) |

## 2. Remaining gaps

### P0 (breaks SDK/cluster behavior or a gate)
None known. All mixed gates (mysql+cassandra, grpc+tchannel, two_history) were green on `GATE SUMMARY failed: []` after taskwritelock.
The nearest to P0 is P1-1 below (workflow-level retry policy never retries), which a workflow-retry SDK test would expose.

### P1 (Go behavior missing, not hit by the gate)
| # | Go file:func | Rust file | Port instruction (one worker) |
|---|---|---|---|
| 1 | `G/task/timer_active_task_executor.go: executeWorkflowTimeoutTask` -> `retryWorkflow` (`ms.GetRetryBackoffDuration`, `AddContinueAsNewEvent`, `UpdateWorkflowExecutionWithNewAsActive`) | `R/wiring/assembly/queue_deps_timer_active.rs:220,223` (`retry_workflow`, `update_workflow_execution_with_new_as_active` return `todo_err`) | Implement both on `EngineTimerWorkflow` by reusing the continue-as-new path in `wiring/assembly/engine_decision.rs` `update_with_new_run` / `engine_start.rs update_workflow_execution_with_new_as_active` (they already hold `lock_task_writes`). Add unit test: workflow timeout with retry policy produces a new run (Retry initiator); gate: SDK workflow with RetryPolicy + ExecutionStartToCloseTimeout. |
| 2 | `G/task/transfer_active_task_executor.go: processResetWorkflow` -> `reset.WorkflowResetter.ResetWorkflow` | `R/wiring/assembly/queue_deps_transfer_active.rs:961` (`todo_err("WorkflowResetter.ResetWorkflow")`) | Wire `ResetWorkflow` to the existing `engine_resetter.rs` resetter (same one used by `reset_workflow_execution`). Used by auto-reset on bad binary checksum. Unit test + admin reset of bad binary. |
| 3 | `G/engine/engineimpl/history_engine.go` task DLQ processor (`startDLQProcessor/stopDLQProcessor`), `G/taskdlq/*` | `R/wiring/assembly/engine_factory.rs:326-327` (`start_dlq_processor`, `stop_dlq_processor` return `Ok(())`) | Build the `taskdlq` processor over the DLQ persistence store (`HistoryTaskDLQ`) and start/stop it in `EngineLifecycleParts`. Note comment at `engine_factory.rs:314` says no persistence manager is wired; wire `cadence-persistence` DLQ manager from `HistoryEnv`. Test: enqueue a DLQ row, processor drains/ack as Go. |
| 4 | `G/task/*` archival: `common/archiver` client, `G/task/timer_task_executor_base.go: archiveWorkflow`, `transfer_active_task_executor.go` close-execution archival | `R/wiring/assembly/archiver_client.rs:203-283` (`NOT_PORTED`; only `file://` inline works; `default_client()` has no history source; archival workflow signal client errors) | (a) set `history_source` from the history-v2 reader so inline history archival works for `file://`; (b) port the archival signal client (SignalWithStart `cadence-archival` workflow in system domain, as `parent_close_policy_client.rs` does); (c) s3/gcs schemes remain unsupported unless Go build uses them. Also requires the Go Worker (kept) to process archival signals. Test: domain with archival enabled closes workflow -> file archived. |
| 5 | `G/decision/task_handler.go: handleDecisionAcquireSemaphore` / `execution/mutable_state_builder_methods_semaphore.go` | `R/wiring/assembly/engine_decision_ops.rs:769,777` (`not_ported("AcquireSemaphore")`), `execution/state_builder.rs:440` (replay unsupported) | Add `AcquireSemaphore` handling on `EngineMutableState` (semaphore map already persisted by cassgaps on Cassandra; SQL path via blob). Port Go semaphore add/replicate events (`SemaphoreAcquired/Released`...). Only needed for SDK versions using semaphores; low priority but Go-supported. Gen: gRPC converter already maps the decision. |
| 6 | `G/engine/engineimpl/history_engine.go: ReplicateFailoverMarkers` / `G/shard/context.go: ReplicateFailoverMarkers` | `R/wiring/assembly/engine_start.rs` header note ("has no shard implementation and is a no-op") | Persist failover markers in shard context (`UpdateFailoverLevel`/`AddingPendingFailoverMarker`, task category replication) as Go; test: graceful failover marker round trip. Matters only for graceful domain failover across 2 clusters. |
| 7 | `G/execution/mutable_state_builder.go: insertWorkflowRequest` (types/version) | `R/execution/mutable_state_methods_signal.rs:57`; `wiring/assembly/ndc_context_hooks.rs:14` (request type decode always Start); `engine_start.rs:479` (`Ok(None)` request lookup) | Extend Rust `WorkflowRequest` with request type/version, persist and decode (strong idempotency for start/signal/cancel/terminate, flag default false). Test: duplicate signal request id dedup on active and passive. |
| 8 | `G/execution/mutable_state_builder.go: AddDecisionTaskCompletedEvent` auto-reset points for binary checksum | `R/wiring/assembly/engine_decision.rs:256` ("auto-reset-point handling of the binary checksum is not ported") | Port `addBinaryCheckSumIfNotExists` auto-reset-point creation in `ms.AddDecisionTaskCompletedEvent` (`ResetPoints` with `CreatedTimeNano`, `Resettable`, `maxResetPoints` cap). Pairs with item 2. |
| 9 | `G/execution/mutable_state_builder.go` SetHistoryTree, ACSP blob, events cache write | `R/execution/mutable_state_methods_started.rs:237,288`; `mutable_state_methods_common.rs:7` | SetHistoryTree(RunID) and ACSP blob are needed for search-attribute/active-active visibility; events cache write is perf only (P2). Port ACSP blob into `AddWorkflowExecutionStartedEvent` first. |
| 10 | `G/execution/mutable_state_builder.go: GenerateActivityTransferTasks` w/ local dispatch fallback; `mutable_state_methods_activity.rs:134,164` | same | Verify `schedule_activity` resolved values path covers replicate; remove stale comments if covered (decisiongaps2 handled the active path). |
| 11 | `G/queue` NoSQL timer active `retry_workflow` ties to P1-1; `G/task/standby_*` -> `R/wiring/assembly/queue_deps_standby.rs:77,351` (`Ok(None)` returns) | `R/wiring/assembly/queue_deps_standby.rs` | Read the two `Ok(None)` sites against Go `getStandbyPostActionFn`/`checkWorkflowStillRunning`; likely correct, add unit tests. |
| 12 | `G/ndc/*` owned-state ops unsupported: `wiring/assembly/ndc_state.rs:94-274` (`get_current_branch_token`, ACSP decode in `vector_clock_inputs`, `update_workflow_state` to completed/corrupted) and `ndc_context_hooks.rs:262,396` (task category unsupported in NDC persistence, `reapply_events` unsupported) | `R/wiring/assembly/ndc_state.rs`, `ndc_context_hooks.rs` | Port each operation from `G/execution/mutable_state_builder.go` (`UpdateWorkflowStateCloseStatus`, `GetCurrentBranchToken`) and the `GetActiveClusterSelectionPolicy` blob decode; extend NDC persistence categories to timer/replication/visibility as Go `workflow.go`. NDC multi-cluster replication of completed runs and active-active workflows is the user-visible area. |
| 13 | `G/replication`: `G/ndc/history_resender.go` remote admin client | `R/replication/deps.rs:496,504` default errors for `reapply_events` / `get_workflow_execution_raw_history_v2` | Confirm the production admin client (`wiring/assembly/replication_clients.rs`) overrides both (default errors are fine only for fakes). If not, implement over admin gRPC. |
| 14 | `G/service/history/service.go` OpenFeature dynamic config | `R/launcher.rs:118,175` | Return error (as Go when provider unsupported) or implement; Go OSS defaults to file-based so low risk. |
| 15 | `G/task` timer-queue/transfer retry "first retry has 0 backoff" TODOs | `R/queue/{timer,transfer}_queue_processor.rs:228/209` | Same TODO exists in Go; no action. |

### P2 (metrics, logging, perf, tests, hygiene)
1. Remaining Go test volume: engine history_engine*_test (decision/activity record/respond) not ported (enginepkg2 note); events, query, workflowcache, failover, queue (117), queuev2 (149), task executor tests (208), taskdlq (41), execution (349 vs ~35+).
2. `ratelimit_aggregator.rs` metrics emission (auditp0); mutable-state metrics use domain id instead of name tag (auditp1); parent-close TLS + `publicClient.hostPort`.
3. `events cache` write on completed/cancel/timeout (perf only); `engine/describe_workflow_execution.rs:41` (same TODO in Go).
4. Delete dead `Unwired*` stubs and stale `TODO(link)` comments (section 0), `ndc/mod.rs:221-293` default-error trait methods once all implementors override.
5. Go `simulation/`, `testing/`, `templates/`: no Rust equivalent (test-only).
6. Pre-existing intermittent gates: GitHub artifact upload outages (churnflake) are infra, not History.
7. Frontend soak FAIL (query "init"/tail latency) is tracked in fe-soak-fix-deferred, not History.

## 3. Suggested order
P1-1 + P1-2 + P1-8 (workflow retry timer, auto reset) as one worker each pair; P1-3 (task DLQ processor); P1-4 (archival); then P1-12/13 (NDC edge cases); P1-5/6/7 last.
Every item needs: unit tests in-crate, `rust-ci cadence-history`, and a history-mixed-gate lane with an SDK scenario where noted.

## 4. Generator fix (rust/tools/generate-grpc-decisions.py)
The audit premise (generator still emits `request_local_dispatch` rejection) was partially stale: the generator already maps the field
because `cadence_api::tasks` now contains it, but regenerating diverged from the committed `decisions.rs` in two ways:
(1) the hand-written `AcquireSemaphore` arm was dropped; (2) the generator would also emit a non-compiling
`active_cluster_selection_policy: enum_in(.., e::crate::types::ActiveClusterSelectionPolicy::from_i32)` where `decisions.rs` has
`.map(selection_policy_in)`. Fixed in the generator: `ActiveClusterSelectionPolicy` -> `.map(selection_policy_in)` and the
AcquireSemaphore arm is emitted. `decisions.rs` was regenerated; the only diff is cosmetic
(`request_local_dispatch: v.request_local_dispatch` instead of a `let` binding). Reproduce:
`python3 rust/tools/generate-grpc-decisions.py && git diff --exit-code` (needs `rustfmt`).
