# History parity audit (Go vs Rust), 2026-11-11

Baseline: origin/master 8e0b1064c. Documentation only; no cargo/go run. Method: file listing + grep of
`not wired|not yet ported|TODO|unsupported|unimplemented!|todo!|Internal` in `rust/crates/cadence-history/src`.
Rust root below = `rust/crates/cadence-history/src` (abbrev `R/`). Go root = `go/service/history` (abbrev `G/`).
Rust is ~73k lines in 278 files (+ `wiring/` for assembly, persistence, resources, servers).
Status = by-file code presence, not behavioral verification: "ported" means a Rust counterpart with the logic exists.
Test counts: Go = `func Test*` count in the package; Rust = `#[test]`/`#[tokio::test]` in src (Rust tests live inline).

## 1. Summary per package

| Go package | Go src/test fns | Rust counterpart | Status | Rust tests | Main gaps |
|---|---|---|---|---|---|
| handler | 2 / 48 | `R/handler/handler_impl.rs`, `wiring/server/*` | partial | 1 (+84 wiring) | 3 RPCs are Internal "gap": GetReplicationMessages, RemoveTask, RatelimitUpdate; gRPC missing 4 RPCs |
| engine (engineimpl) | 31 / 185 | `R/engine/*`, `wiring/assembly/engine_*` | partial | 0 inline | no standalone engine tests; deps shims (`engine/deps.rs` TODO(link)); see section 3 |
| execution | 30 / 349 | `R/execution/*`, `R/mutable_state*.rs` | partial | 35 | "writeEventToCache/close tasks not wired" in completed/cancel/timedout/terminated; delayed decision, insertWorkflowRequest, SetHistoryTree, ACSP blob |
| task | 19 / 208 | `R/task/*` | partial | 7 | executor tests mostly missing; `deps.rs` TODO(link) shims |
| queue (v1) | 21 / 117 | `R/queue/*` | partial | 0 inline | failover/standby/active timer processors folded into `timer_queue_processor.rs`; WRR scheduler/reinjector/timer standby in flight (history-queuegaps); `deps.rs` shims |
| queuev2 | 25 / 149 | `R/queuev2/*` | ported (files 1:1) | 1 | Go tests (149) essentially unported |
| decision | 3 / 56 | `R/decision/*` | partial | 0 | `checker.rs:5`, `handler.rs:239` TODO(link); local activity dispatch, search-attr task, client version in flight (history-decisiongaps2) |
| ndc | 10 / 94 | `R/ndc/*`, `wiring/assembly/ndc_*` | ported (files 1:1) | 16 | sub-manager tests in flight (history-ndctests2); `test_support.rs` and `workflow_resetter.rs:217-235` unimplemented!() are test mocks only |
| replication | 10 / 84 | `R/replication/*` | ported (files 1:1) | 0 inline | tests missing; GetReplicationMessages handler not wired |
| reset | 1 / 13 | `R/reset/*` | ported | 13 | none known |
| shard | 5 / 87 | `R/shard.rs`, `R/shard_context.rs`, `wiring/assembly/controller.rs`, `shard_factory.rs` | partial | 0 | `shard_context.rs:533` replication queue state unsupported; controller/notifier tests missing; notifying_execution_manager only via adapters |
| events | 3 / 9 | `R/events/mod.rs`, `wiring/assembly/engine_events*.rs` | partial | 0 | `events/mod.rs:71` TODO(link); events cache/notifier tests missing |
| query | 2 / 7 | `R/query/mod.rs` | ported | 0 | tests |
| workflow | 3 / 2 | `R/execution/workflow*.rs` | ported | 0 | errors.go sentinel mapping not verified |
| workflowcache | 2 / 6 | `R/workflowcache/mod.rs` | ported | 0 | tests, metrics |
| failover | 2 / 16 | `R/failover/*`, `engine/register_domain_failover_callback.rs` | ported | 0 | tests |
| config | 1 / 2 | `R/config.rs`, `config_collection.rs` | ported | 0 | key-by-key diff not done; OpenFeature unsupported (`launcher.rs:118,175`) |
| common/constants/lookup | 2+2+1 | `R/common.rs`, `constants.rs`, `lookup.rs` | ported | 0 | none |
| taskdlq | 4 / 41 | `R/taskdlq/*` | ported | 0 | tests |
| wrappers | 6 / 3 | `R/wrappers/ratelimited.rs`, `wiring/server/{grpc*,thrift*,tchannel}.rs` | partial | 32 | see RPC table |
| resource/service/lifecycle | 2+1 | `R/service.rs`, `launcher.rs`, `wiring/resources/*` | partial | 4 | single-host no-op membership default (`service.rs:47`), OpenFeature |
| simulation/testing/templates | 1+2+0 | none | missing (test-only) | 0 | P2 |

Test parity overall: Go ~1,300 test fns vs Rust ~200 `#[test]` in cadence-history. Most Go `_test.go` files have no Rust equivalent except reset (13/13), ndc partial (16/94), execution partial (35/349).

## 2. Go file to Rust counterpart (non-test files)

engine/engineimpl: `history_engine.go` -> `engine/{runtime,deps,deps_ms,types,interface,lifecycle}.rs` + `wiring/assembly/engine_factory.rs`; `start_workflow_execution.go` -> `engine/start_workflow_execution.rs` + `engine_start.rs` (ported); `signal_workflow_execution.go` -> `signal_workflow_execution.rs`; `respond_decision_task_completed/failed.go` -> `wiring/assembly/engine_decision*.rs` (ported); `respond_activity_task_*.go`, `record_activity_task_started.go` -> `engine/activity_responses.rs`, `record_activity_task_started.rs`; `request_cancel/terminate/reset_sticky/remove_signal` -> `workflow_mutations.rs`, `engine_close.rs`; `record_decision_task_started.go` -> `engine_decision_ops.rs`; `reset_workflow_execution.go` -> `reset_workflow_execution.rs`, `engine_resetter.rs`; `describe_*`, `poll_mutable_state`, `query_workflow`, `reapply_events`, `refresh_workflow_tasks`, `notify_tasks`, `register_domain_failover_callback`, `dlq_operations`, `get_replication_messages`, `describe_queues/reset_queues` (-> `queue_operations.rs`, TODO(link) at :29 queue state string repr), `describe_mutable_state` (-> `mutable_state_admin.rs`): all present. All ported.

execution: every `*.go` has a `.rs` (context, cache, checksum, history_builder, state_builder, state_rebuilder, task_generator/refresher, timer_sequence, retry, integrity, workflow_repairer, per-method files). `mutable_state_builder_methods_semaphore.go` -> `semaphore.rs` (state_builder.rs:440 test notes semaphore replay explicitly unsupported without persistence state).

task: all 19 Go files have Rust files (`task.go` -> `history_task.rs`). queue: all Go files have Rust files; `timer_queue_{active,failover,standby}_processor.go` and `queue_processor_util.go` -> `timer_queue_processor*.rs`, `util.rs`. queuev2/ndc/replication/taskdlq/decision/reset/query/workflowcache/failover/events/shard: file-for-file (`shard/controller.go` -> `wiring/assembly/controller.rs`; `events/cache.go,notifier.go,blob.go` -> `events/mod.rs` + `engine_events*.rs`; `shard/task_notifier.go` -> `engine/notify_tasks.rs`).

## 3. History RPCs (Go handler, 46) vs Rust

Layers: handler trait impl (`R/handler/handler_impl.rs`), TChannel/Thrift (`wiring/server/{tchannel,thrift_g1-3}.rs`), gRPC (`wiring/server/grpc*.rs`, 40 methods).

| RPC | Rust handler | Thrift | gRPC |
|---|---|---|---|
| Health, Start/Stop/PrepareToStop, CreateEngine | full (lifecycle) | n/a | n/a |
| StartWorkflowExecution, SignalWithStart, SignalWorkflow, Terminate, RequestCancel, ResetWorkflow, ResetStickyTaskList | full | yes | yes |
| RecordActivityTaskStarted/Heartbeat, RespondActivityTaskCompleted/Failed/Canceled | full | yes | yes |
| RecordDecisionTaskStarted, RespondDecisionTaskCompleted/Failed | full | yes | yes |
| DescribeWorkflowExecution, GetMutableState, PollMutableState, QueryWorkflow, DescribeMutableState | full | yes | yes |
| ScheduleDecisionTask, RecordChildExecutionCompleted, RemoveSignalMutableState, RefreshWorkflowTasks, ReapplyEvents | full | yes | yes |
| ReplicateEventsV2, SyncActivity, SyncShardStatus, NotifyFailoverMarkers, GetFailoverInfo | full | yes | yes |
| Count/Read/Purge/MergeDLQMessages, GetDLQReplicationMessages | full | yes | yes |
| DescribeHistoryHost | full | yes | yes |
| CloseShard, ResetQueue, DescribeQueue | full | yes | MISSING (not in grpc_g1-3) |
| **RemoveTask** | **Internal gap** (`handler_impl.rs:343-344`) | dispatches to gap | MISSING |
| **GetReplicationMessages** | **Internal gap** (`handler_impl.rs:245-246`); engine fn exists `engine/dlq_operations.rs:26` | dispatches to gap | method present, returns gap |
| **RatelimitUpdate** | **Internal gap** (`handler_impl.rs:499-500`) | dispatches to gap | method present, returns gap |
| GetCrossClusterTasks, RespondCrossClusterTasksCompleted | Internal "deprecated" error, matches Go | yes | yes |
| TChannel unknown method | `tchannel.rs:329` returns "not implemented yet" for anything outside the list above (expected) | | |

## 4. Prioritized gaps

### P0 (SDK compat / mixed gate)
1. GetReplicationMessages returns Internal: `R/handler/handler_impl.rs:245`. Breaks cross-cluster replication pull (fetcher on the other cluster) and any gate with 2 clusters. Needs per-token shard fan-out and response size cap (Go `handler.go` GetReplicationMessages).
2. RemoveTask Internal: `handler_impl.rs:343`. Admin `cadence admin ... remove task` fails; needs shard-context task deletion.
3. RatelimitUpdate Internal: `handler_impl.rs:499`. Frontend global ratelimiter calls History for aggregation; breaks if global ratelimiter keys enabled.
4. gRPC missing CloseShard, DescribeQueue, RemoveTask, ResetQueue: `wiring/server/grpc_g1-3.rs` (only 40/46 RPCs). Breaks Go frontend/admin using gRPC history clients for these.
5. Close-task generation ("writeEventToCache/close tasks not wired"): `execution/mutable_state_methods_completed.rs:40-49,76,123-132`, `..._cancellation.rs:97-106`, `..._timedout.rs:31-40`. Verify the replicate (NDC / standby) path still generates workflow close tasks (transfer, retention timer, visibility); if not wired the passive cluster never closes visibility or deletes history. Needs confirmation by test; may be handled in `ndc_workflow.rs`.
6. Delayed decision task / backoff: `execution/mutable_state_methods_decision.rs:260` (`GenerateDelayedDecisionTasks` not wired, returns Ok(false)). Affects cron/retry backoff start.
7. Cassandra stores: in flight (history-cassandra), not counted here.

### P1 (behavior difference)
1. `mutable_state_methods_activity.rs:134,164`: local activity dispatch and GenerateActivityTransferTasks (in flight: history-decisiongaps2).
2. `mutable_state_methods_signal.rs:57`: insertWorkflowRequest on replicate-signal not wired (dedup of signal request IDs on passive side).
3. `mutable_state_methods_started.rs:237,288`: SetHistoryTree(RunID), cache write, ACSP (search attribute) blob not wired (decisiongaps2 covers search-attr task).
4. `mutable_state_methods_decision.rs:140-158`: DecisionRetryCriticalAttempts metric/warn log; `mutable_state_methods_completed.rs:53` terminate logging/metrics.
5. `shard_context.rs:533`: replication queue state "not supported" in update path.
6. `engine/queue_operations.rs:29`: DescribeQueue state string representation must match Go `ProcessingQueueState.String()`, used by admin CLI.
7. `engine/describe_workflow_execution.rs:41`: execution-time in mutable state (same TODO exists in Go, no action).
8. `launcher.rs:118,175`: OpenFeature dynamic config provider unsupported; `launcher.rs:169` falls back to no-op config if ConfigStore fails (Go aborts?). Verify.
9. `service.rs:47`: no-op membership default (single host); check `wiring/resources/membership*.rs` is used by default for multi-host Ringpop.
10. `engine/deps.rs`, `queue/deps.rs`, `task/deps.rs`, `queue/task_allocator.rs`, `queuev2/deps.rs:533`: TODO(link) trait shims; behavior identical only if `wiring/assembly/*` implements every one. Audit the adapters (queue_deps*, ndc_adapters, shard_adapters) for stubs.
11. `queue/processor_base.rs:349`: `panic!` on unsupported metric scope (Go also panics; fine).
12. Queue WRR scheduler, reinjector, timer standby behaviors: in flight (history-queuegaps).
13. Parent close policy and cron-close errors: in flight (history-parentclose).
14. NDC inter-host TLS: in flight (history-ndctests2).
15. `wrappers/ratelimited.rs`: only ratelimited wrapper ported; Go `wrappers/thrift` and `grpc` error mapping is in `wiring/server/errors.rs` (39 lines); confirm each Go error type maps (ShardOwnershipLost, ServiceBusy, LimitExceeded, etc.). Thrift mapping is present in `thrift_g*.rs`.

### P2 (tests, metrics only)
1. Tests: Go ~1,300 test fns vs ~200 Rust. Biggest holes: engine (185 vs 0), task (208 vs 7), queue (117 vs 0), queuev2 (149 vs 1), execution (349 vs 35), replication (84 vs 0), shard (87 vs 0), decision (56 vs 0), taskdlq (41 vs 0), failover (16 vs 0), events (9 vs 0), query (7 vs 0), workflowcache (6 vs 0), handler (48 vs 1).
2. Go `simulation/`, `testing/`, `templates/`, engine `testdata/`: no Rust equivalent.
3. `workflowcache` metrics, `replication/metrics_emitter.rs` coverage.
4. Rust test support mocks with `unimplemented!()`: `ndc/test_support.rs`, `ndc/workflow_resetter.rs:217-235` (test only, no action).
5. Soak FAIL for Frontend (query "init"/tail latency) is tracked in memory (fe-soak-fix-deferred), not a History gap.

## 5. Work in flight (do not double count)
- history-cassandra: Cassandra execution/history/queue stores (P0 item 7).
- history-decisiongaps2: local activity dispatch, search-attribute task, client version (P1 items 1, 3 partially; `decision/checker.rs`).
- history-ndctests2: NDC sub-manager tests, inter-host TLS (P2 test rows for ndc; P1 item 14).
- history-queuegaps: WRR scheduler, reinjector, timer standby (P1 item 12; queue tests).
- history-parentclose: parent close policy, cron close errors (P1 item 13).

## 6. Suggested next work (not in flight)
1. Handler gaps P0 1-4 (GetReplicationMessages, RemoveTask, RatelimitUpdate, gRPC admin RPCs).
2. Close-task generation on replicate path (P0 5) and delayed decision tasks (P0 6): write a mixed-gate test first to see whether real.
3. Port Go tests in bulk for engine/task/queue/replication/shard (P2) after behaviors settle.
4. Audit adapters in `wiring/assembly` for stub trait impls (P1 10).

Caveat: statuses come from file presence and marker greps; no behavior was executed. Items marked "verify" need a test or a closer read.
