# History port audit 2026-11-05

Scope: Go `go/service/history` non-test files (189 files, 61627 LOC) vs `rust/crates/cadence-history/src` (166 files, 33151 LOC). Static audit only, no cargo. Status is a heuristic from Rust/Go line ratio (real >=0.6, partial 0.25-0.6, stub <0.25) plus grep for TODO(link)/stub/unimplemented!/Noop. Several Rust files are densely formatted (e.g. events/mod.rs), so ratios are rough and some mapped files are shared (one Rust file standing in for several Go files).

## Headline findings

1. **The service is not runnable.** `handler/` is 57 lines (trait only) versus Go `handler/handler.go` 2289 LOC. No gRPC/Thrift server impl, no `wrappers/grpc`, `wrappers/thrift`, no `service.go`/`resource` wiring. `main.rs` just calls `cadence_matching::launcher::run("cadence-history", ...)` (launcher has no history service body).
2. **Dependency facades remain.** `engine/deps.rs`, `engine/types.rs`, `task/deps.rs`, `task/deps_exec.rs`, `queue/deps.rs`, `queuev2/deps.rs`, `replication/deps.rs`, `queue/task_allocator.rs` define local trait/stub copies of Go packages with `TODO(link)` markers (about 30 markers). They are not yet bound to real persistence/domain cache/client/metrics. `task/deps_exec.rs:114` has `unimplemented!("TODO(link): quotas::Collection")`.
3. **Engine is mostly missing**: only 12 of ~30 engineimpl operations exist (see table). `start_workflow_execution` (1050 LOC), `describe_workflow_execution`, `get_replication_messages`, `reapply_events`, `refresh_workflow_tasks`, `notify_tasks`, `register_domain_failover_callback`, `record_decision_task_started`, `respond_decision_task_*` (Go ones are 33-line wrappers) have no Rust file.
4. **Timer queue processors missing entirely** (Go `queue/timer_queue_*`: ~1800 LOC) plus `queue/factory.go`.
5. **Mutable state is a fraction**: Go mutable_state_builder.go+methods ~6200 LOC vs Rust mutable_state.rs+mutable_state_activity.rs+execution/* small files (~1.6k).
6. `reset/resetter.go` 732 LOC vs reset/mod.rs 48 + history.rs 150.

## Per-file table

| Go file | Go LOC | Rust file (LOC) | Status |
|---|---|---|---|
| common/errors.go | 46 | - | missing |
| common/type.go | 47 | - | missing |
| config/config.go | 730 | config.rs (614) | real (0.84) |
| constants/constants.go | 47 | constants.rs (81) | real (1.72) |
| constants/test_constants.go | 200 | - | n/a (test helper) |
| decision/checker.go | 958 | decision/checker.rs (125) | stub/facade (0.13) |
| decision/handler.go | 948 | decision/handler.rs (271) | partial (0.29) |
| decision/task_handler.go | 1241 | decision/task_handler.rs (313) | partial (0.25) |
| engine/engineimpl/describe_mutable_state.go | 87 | engine/mutable_state_admin.rs (33) | partial (0.38) |
| engine/engineimpl/describe_queues.go | 74 | engine/queue_operations.rs (33) | partial (0.45) |
| engine/engineimpl/describe_workflow_execution.go | 295 | - | missing |
| engine/engineimpl/dlq_operations.go | 102 | - | missing |
| engine/engineimpl/get_replication_messages.go | 153 | - | missing |
| engine/engineimpl/history_engine.go | 481 | engine/runtime.rs (157) | partial (0.33) |
| engine/engineimpl/notify_tasks.go | 131 | - | missing |
| engine/engineimpl/poll_mutable_state.go | 320 | - | missing |
| engine/engineimpl/query_workflow.go | 359 | - | missing |
| engine/engineimpl/reapply_events.go | 184 | - | missing |
| engine/engineimpl/record_activity_task_started.go | 170 | - | missing |
| engine/engineimpl/record_child_execution_completed.go | 120 | - | missing |
| engine/engineimpl/record_decision_task_started.go | 33 | - | missing |
| engine/engineimpl/refresh_workflow_tasks.go | 72 | - | missing |
| engine/engineimpl/register_domain_failover_callback.go | 282 | - | missing |
| engine/engineimpl/remove_signal_mutable_state.go | 59 | engine/workflow_mutations.rs (114) | real (1.93) |
| engine/engineimpl/request_cancel_workflow_execution.go | 111 | engine/workflow_mutations.rs (114) | real (1.03) |
| engine/engineimpl/reset_queues.go | 54 | engine/queue_operations.rs (33) | real (0.61) |
| engine/engineimpl/reset_sticky_tasklist.go | 64 | engine/workflow_mutations.rs (114) | real (1.78) |
| engine/engineimpl/reset_workflow_execution.go | 296 | - | missing |
| engine/engineimpl/respond_activity_task_canceled.go | 123 | engine/activity_responses.rs (105) | real (0.85) |
| engine/engineimpl/respond_activity_task_completed.go | 126 | engine/activity_responses.rs (105) | real (0.83) |
| engine/engineimpl/respond_activity_task_failed.go | 143 | engine/activity_responses.rs (105) | real (0.73) |
| engine/engineimpl/respond_activity_task_heartbeat.go | 123 | engine/activity_responses.rs (105) | real (0.85) |
| engine/engineimpl/respond_decision_task_completed.go | 33 | - | missing |
| engine/engineimpl/respond_decision_task_failed.go | 33 | - | missing |
| engine/engineimpl/signal_workflow_execution.go | 130 | - | missing |
| engine/engineimpl/start_workflow_execution.go | 1050 | - | missing |
| engine/engineimpl/terminate_workflow_execution.go | 103 | engine/workflow_mutations.rs (114) | real (1.11) |
| engine/interface.go | 83 | engine/mod.rs (18) | stub/facade (0.22) |
| engine/testdata/engine_for_tests.go | 164 | - | n/a (test helper) |
| events/blob.go | 53 | events/mod.rs (92) | real (1.74) |
| events/cache.go | 307 | events/mod.rs (92) | partial (0.30) |
| events/notifier.go | 271 | events/mod.rs (92) | partial (0.34) |
| execution/cache.go | 397 | execution/cache.rs (97) | stub/facade (0.24) |
| execution/checksum.go | 115 | execution/checksum.rs (103) | real (0.90) |
| execution/context.go | 1503 | execution/context.rs (348) | stub/facade (0.23) |
| execution/context_util.go | 226 | execution/context_util.rs (155) | real (0.69) |
| execution/history_builder.go | 950 | execution/history_builder.rs (186) | stub/facade (0.20) |
| execution/integrity.go | 175 | execution/integrity.rs (52) | partial (0.30) |
| execution/mutable_state_builder.go | 2479 | mutable_state.rs (888) | partial (0.36) |
| execution/mutable_state_builder_methods_activity.go | 746 | mutable_state_activity.rs (266) | partial (0.36) |
| execution/mutable_state_builder_methods_cancellation.go | 240 | execution/cancellation.rs (57) | stub/facade (0.24) |
| execution/mutable_state_builder_methods_child_workflow.go | 535 | execution/child_workflow.rs (153) | partial (0.29) |
| execution/mutable_state_builder_methods_completed.go | 158 | execution/workflow_close.rs (53) | partial (0.34) |
| execution/mutable_state_builder_methods_decision.go | 354 | execution/mutable_state_decision_task_manager.rs (177) | partial (0.50) |
| execution/mutable_state_builder_methods_semaphore.go | 193 | execution/semaphore.rs (80) | partial (0.41) |
| execution/mutable_state_builder_methods_signal.go | 255 | execution/signal.rs (82) | partial (0.32) |
| execution/mutable_state_builder_methods_started.go | 301 | execution/workflow_started.rs (23) | stub/facade (0.08) |
| execution/mutable_state_builder_methods_timedout.go | 63 | execution/markers.rs (28) | partial (0.44) |
| execution/mutable_state_builder_methods_timer.go | 284 | execution/timer.rs (170) | partial (0.60) |
| execution/mutable_state_decision_task_manager.go | 884 | execution/mutable_state_decision_task_manager.rs (177) | stub/facade (0.20) |
| execution/mutable_state.go | 255 | mutable_state.rs (888) | real (3.48) |
| execution/mutable_state_task_generator.go | 714 | execution/mutable_state_task_generator.rs (185) | partial (0.26) |
| execution/mutable_state_task_refresher.go | 505 | execution/mutable_state_task_refresher.rs (94) | stub/facade (0.19) |
| execution/mutable_state_util.go | 199 | execution/mutable_state_util.rs (48) | stub/facade (0.24) |
| execution/mutable_state_util_test_helpers.go | 309 | - | n/a (test helper) |
| execution/retry.go | 109 | execution/retry.rs (66) | real (0.61) |
| execution/state_builder.go | 581 | execution/state_builder.rs (73) | stub/facade (0.13) |
| execution/state_rebuilder.go | 281 | execution/state_rebuilder.rs (61) | stub/facade (0.22) |
| execution/timer_sequence.go | 511 | execution/timer_sequence.rs (114) | stub/facade (0.22) |
| execution/workflow_execution_util.go | 51 | execution/workflow_execution_util.rs (28) | partial (0.55) |
| execution/workflow.go | 344 | execution/workflow.rs (93) | partial (0.27) |
| execution/workflow_repairer.go | 450 | execution/workflow_repairer.rs (84) | stub/facade (0.19) |
| failover/coordinator.go | 453 | failover/coordinator.rs (351) | real (0.77) |
| failover/marker_notifier.go | 122 | failover/marker_notifier.rs (89) | real (0.73) |
| handler/handler.go | 2289 | handler/interface.rs (54) | stub/facade (0.02) |
| handler/interface.go | 90 | handler/interface.rs (54) | real (0.60) |
| lookup/lookup.go | 33 | - | missing |
| ndc/activity_replicator.go | 322 | ndc/activity_replicator.rs (185) | partial (0.57) |
| ndc/branch_manager.go | 278 | ndc/branch_manager.rs (165) | partial (0.59) |
| ndc/conflict_resolver.go | 196 | ndc/conflict_resolver.rs (115) | partial (0.59) |
| ndc/events_reapplier.go | 111 | ndc/events_reapplier.rs (64) | partial (0.58) |
| ndc/existing_workflow_transaction_manager.go | 566 | ndc/existing_workflow_transaction_manager.rs (335) | partial (0.59) |
| ndc/history_replicator.go | 903 | ndc/history_replicator.rs (508) | partial (0.56) |
| ndc/new_workflow_transaction_manager.go | 393 | ndc/new_workflow_transaction_manager.rs (244) | real (0.62) |
| ndc/replication_task.go | 403 | ndc/replication_task.rs (240) | partial (0.60) |
| ndc/transaction_manager.go | 543 | ndc/transaction_manager.rs (255) | partial (0.47) |
| ndc/workflow_resetter.go | 237 | ndc/workflow_resetter.rs (194) | real (0.82) |
| query/query.go | 152 | query/mod.rs (218) | real (1.43) |
| query/registry.go | 220 | query/mod.rs (218) | real (0.99) |
| queue/action.go | 122 | queue/action.rs (82) | real (0.67) |
| queue/constants.go | 31 | queue/constants.rs (5) | stub/facade (0.16) |
| queue/domain_filter.go | 136 | queue/domain_filter.rs (79) | partial (0.58) |
| queue/factory.go | 113 | - | missing |
| queue/interface.go | 97 | queue/interface.rs (24) | stub/facade (0.25) |
| queue/processing_queue_collection.go | 238 | queue/processing_queue_collection.rs (194) | real (0.82) |
| queue/processing_queue.go | 362 | queue/processing_queue.rs (297) | real (0.82) |
| queue/processing_queue_state.go | 104 | queue/processing_queue_state.rs (47) | partial (0.45) |
| queue/processor_base.go | 471 | queue/processor_base.rs (444) | real (0.94) |
| queue/processor_options.go | 60 | queue/processor_options.rs (84) | real (1.40) |
| queue/queue_processor_util.go | 125 | queue/util.rs (201) | real (1.61) |
| queue/split_policy.go | 433 | queue/split_policy.rs (311) | real (0.72) |
| queue/task_allocator.go | 233 | queue/task_allocator.rs (199) | real (0.85) |
| queue/timer_queue_active_processor.go | 95 | - | missing |
| queue/timer_queue_failover_processor.go | 125 | - | missing |
| queue/timer_queue_processor_base.go | 734 | - | missing |
| queue/timer_queue_processor.go | 566 | - | missing |
| queue/timer_queue_standby_processor.go | 115 | - | missing |
| queue/transfer_queue_processor_base.go | 631 | queue/transfer_queue_processor_base.rs (557) | real (0.88) |
| queue/transfer_queue_processor.go | 750 | queue/transfer_queue_processor.rs (890) | real (1.19) |
| queue/transfer_queue_validator.go | 190 | queue/transfer_queue_validator.rs (164) | real (0.86) |
| queuev2/alert.go | 21 | queuev2/alert.rs (19) | real (0.90) |
| queuev2/convert.go | 121 | queuev2/convert.rs (108) | real (0.89) |
| queuev2/interface.go | 45 | queuev2/interface.rs (28) | real (0.62) |
| queuev2/mitigator.go | 240 | queuev2/mitigator.rs (273) | real (1.14) |
| queuev2/monitor.go | 125 | queuev2/monitor.rs (132) | real (1.06) |
| queuev2/pause_controller.go | 156 | queuev2/pause_controller.rs (117) | real (0.75) |
| queuev2/pending_task_tracker.go | 122 | queuev2/pending_task_tracker.rs (110) | real (0.90) |
| queuev2/predicate.go | 116 | queuev2/predicate.rs (59) | partial (0.51) |
| queuev2/predicate_operation.go | 183 | queuev2/predicate_operation.rs (123) | real (0.67) |
| queuev2/queue_base.go | 365 | queuev2/queue_base.rs (481) | real (1.32) |
| queuev2/queue_cached.go | 75 | queuev2/queue_cached.rs (76) | real (1.01) |
| queuev2/queue_immediate.go | 212 | queuev2/queue_immediate.rs (214) | real (1.01) |
| queuev2/queue_mem.go | 235 | queuev2/queue_mem.rs (146) | real (0.62) |
| queuev2/queue_reader_cached_base.go | 749 | queuev2/queue_reader_cached_base.rs (568) | real (0.76) |
| queuev2/queue_reader_cached_immediate.go | 178 | queuev2/queue_reader_cached_immediate.rs (199) | real (1.12) |
| queuev2/queue_reader_cached_scheduled.go | 574 | queuev2/queue_reader_cached_scheduled.rs (519) | real (0.90) |
| queuev2/queue_reader.go | 150 | queuev2/queue_reader.rs (135) | real (0.90) |
| queuev2/queue_scheduled.go | 239 | queuev2/queue_scheduled.rs (251) | real (1.05) |
| queuev2/queue_state.go | 70 | queuev2/queue_state.rs (77) | real (1.10) |
| queuev2/range.go | 52 | queuev2/range.rs (41) | real (0.79) |
| queuev2/timer_queue_factory.go | 185 | queuev2/timer_queue_factory.rs (140) | real (0.76) |
| queuev2/transfer_queue_factory.go | 194 | queuev2/transfer_queue_factory.rs (166) | real (0.86) |
| queuev2/virtual_queue.go | 480 | queuev2/virtual_queue.rs (505) | real (1.05) |
| queuev2/virtual_queue_manager.go | 253 | queuev2/virtual_queue_manager.rs (292) | real (1.15) |
| queuev2/virtual_slice.go | 612 | queuev2/virtual_slice.rs (588) | real (0.96) |
| replication/dlq_handler.go | 398 | replication/dlq_handler.rs (323) | real (0.81) |
| replication/dynamic_task_batch_sizer.go | 156 | replication/dynamic_task_batch_sizer.rs (180) | real (1.15) |
| replication/metrics_emitter.go | 182 | replication/metrics_emitter.rs (157) | real (0.86) |
| replication/task_ack_manager.go | 298 | replication/task_ack_manager.rs (267) | real (0.90) |
| replication/task_executor.go | 369 | replication/task_executor.rs (353) | real (0.96) |
| replication/task_fetcher.go | 349 | replication/task_fetcher.rs (330) | real (0.95) |
| replication/task_hydrator.go | 330 | replication/task_hydrator.rs (371) | real (1.12) |
| replication/task_processor.go | 782 | replication/task_processor.rs (751) | real (0.96) |
| replication/task_reader.go | 67 | replication/task_reader.rs (33) | partial (0.49) |
| replication/task_store.go | 265 | replication/task_store.rs (326) | real (1.23) |
| reset/resetter.go | 732 | reset/mod.rs (48) | stub/facade (0.07) |
| resource/resource.go | 191 | - | missing |
| resource/resource_test_utils.go | 71 | - | n/a (test helper) |
| service.go | 165 | - | missing |
| shard/context.go | 2032 | shard_context.rs (924) | partial (0.45) |
| shard/context_test_utils.go | 118 | - | n/a (test helper) |
| shard/controller.go | 605 | shard.rs (316) | partial (0.52) |
| shard/notifying_execution_manager.go | 161 | shard.rs (316) | real (1.96) |
| shard/task_notifier.go | 241 | shard.rs (316) | real (1.31) |
| simulation/event.go | 52 | - | missing |
| task/constants.go | 36 | task/constants.rs (14) | partial (0.39) |
| taskdlq/constants.go | 8 | taskdlq/constants.rs (6) | real (0.75) |
| taskdlq/host_limiter.go | 42 | taskdlq/host_limiter.rs (41) | real (0.98) |
| taskdlq/interface.go | 35 | taskdlq/interface.rs (18) | partial (0.51) |
| taskdlq/processor.go | 610 | taskdlq/processor.rs (628) | real (1.03) |
| task/event_logger.go | 111 | task/event_logger.rs (95) | real (0.86) |
| task/executor_wrapper.go | 111 | task/executor_wrapper.rs (79) | real (0.71) |
| task/interface.go | 140 | task/interface.rs (137) | real (0.98) |
| task/priority_assigner.go | 161 | task/priority_assigner.rs (144) | real (0.89) |
| task/processor.go | 260 | task/processor.rs (54) | stub/facade (0.21) |
| task/rate_limited_processor.go | 83 | task/rate_limited_processor.rs (72) | real (0.87) |
| task/redispatcher.go | 318 | task/redispatcher.rs (322) | real (1.01) |
| task/rescheduler.go | 292 | task/rescheduler.rs (86) | partial (0.29) |
| task/standby_task_util.go | 327 | task/standby_task_util.rs (305) | real (0.93) |
| task/task.go | 552 | task/history_task.rs (626) | real (1.13) |
| task/task_rate_limiter.go | 113 | task/task_rate_limiter.rs (98) | real (0.87) |
| task/task_util.go | 402 | task/task_util.rs (281) | real (0.70) |
| task/timer_active_task_executor.go | 937 | task/timer_active_task_executor.rs (184) | stub/facade (0.20) |
| task/timer_standby_task_executor.go | 599 | task/timer_standby_task_executor.rs (113) | stub/facade (0.19) |
| task/timer_task_executor_base.go | 407 | task/timer_task_executor_base.rs (103) | partial (0.25) |
| task/transfer_active_task_executor.go | 2045 | task/transfer_active_task_executor.rs (967) | partial (0.47) |
| task/transfer_standby_task_executor.go | 767 | task/transfer_standby_task_executor.rs (116) | stub/facade (0.15) |
| task/transfer_task_executor_base.go | 657 | task/transfer_task_executor_base.rs (127) | stub/facade (0.19) |
| workflowcache/cache.go | 242 | workflowcache/mod.rs (172) | real (0.71) |
| workflowcache/metrics.go | 78 | workflowcache/mod.rs (172) | real (2.21) |
| workflow/context.go | 88 | - | missing |
| workflow/errors.go | 62 | - | missing |
| workflow/util.go | 291 | - | missing |
| wrappers/grpc/grpc_handler_generated.go | 241 | - | missing |
| wrappers/grpc/grpc_handler.go | 43 | - | missing |
| wrappers/ratelimited/handler_generated.go | 329 | wrappers/ratelimited.rs (163) | partial (0.50) |
| wrappers/ratelimited/ratelimit.go | 27 | wrappers/ratelimited.rs (163) | real (6.04) |
| wrappers/thrift/thrift_handler_generated.go | 229 | - | missing |
| wrappers/thrift/thrift_handler.go | 53 | - | missing |

## Rust files with no direct Go counterpart (infrastructure/dependency shims)

config_collection.rs, decision/mod.rs, engine/common_util.rs, engine/decision_replication.rs, engine/deps.rs, engine/deps_ms.rs, engine/dlq_operations.rs, engine/poll_mutable_state.rs, engine/query_workflow.rs, engine/record_activity_task_started.rs, engine/record_child_execution_completed.rs, engine/reset_workflow_execution.rs, engine/signal_workflow_execution.rs, engine/types.rs, execution/mod.rs, failover/mod.rs, handler/mod.rs, lib.rs, main.rs, ndc/mod.rs, queue/config.rs, queue/deps.rs, queue/mod.rs, queue/processor_options_factory.rs, queue/shard_deps.rs, queuev2/deps.rs, queuev2/mod.rs, replication/deps.rs, replication/mod.rs, reset/history.rs, task/deps.rs, task/deps_exec.rs, task/mod.rs, taskdlq/mod.rs, wrappers/mod.rs

## Prioritized gap list

### P0 - needed to run the service at all
- `handler.go`: `NewHandler`, `Start`, `Stop`, `PrepareToStop`, `Health`, and all RPC methods (StartWorkflowExecution, RecordDecisionTaskStarted, RespondDecisionTaskCompleted/Failed, RecordActivityTaskStarted, RespondActivityTask*, RequestCancelWorkflowExecution, SignalWorkflowExecution, SignalWithStartWorkflowExecution, RemoveSignalMutableState, TerminateWorkflowExecution, ResetWorkflowExecution, ScheduleDecisionTask, RecordChildExecutionCompleted, DescribeWorkflowExecution, GetMutableState, PollMutableState, ResetStickyTaskList, QueryWorkflow, ReplicateEventsV2, SyncShardStatus, SyncActivity, GetReplicationMessages, GetDLQReplicationMessages, ReapplyEvents, ReadDLQMessages, PurgeDLQMessages, MergeDLQMessages, RefreshWorkflowTasks, CountDLQMessages, NotifyFailoverMarkers, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, GetFailoverInfo, RatelimitUpdate, CloseShard, DescribeHistoryHost, DescribeQueue, ResetQueues, DescribeMutableState, RemoveTask, GetCrossClusterTasks).
- `wrappers/grpc/grpc_handler_generated.go`, `wrappers/thrift/thrift_handler_generated.go` (+ `grpc_handler.go`, `thrift_handler.go`): transport adapters, missing; `wrappers/ratelimited` only partially covers.
- `service.go`: `NewService`, `Start`, `Stop`; `resource/resource.go`: `New`, resource wiring (persistence, domain cache, membership, clients, archiver, dynamic config).
- `shard/controller.go`: check `NewController`, `acquireShards`, `acquireShard`, `removeEngineForShard`, `shardManagementPump` against shard.rs (316 vs 605+161+241 LOC); `shard/context.go` `NewContext`, `GetEngine`, `UpdateWorkflowExecution`, `CreateWorkflowExecution`, `AllocateTransferIDs`, `renewRangeLocked`, `ReplicateFailoverMarkers`, `UpdateTimerMaxReadLevel` (924 vs 2032 LOC).
- `engine/engineimpl/history_engine.go`: `NewEngineWithShardContext`, `Start`, `Stop`, `registerDomainFailoverCallback`, `createMutableState`, `getActiveDomainByID`; plus `start_workflow_execution.go`: `StartWorkflowExecution`, `SignalWithStartWorkflowExecution`, `startWorkflowHelper`, `createNewWorkflow`, `handleCreateWorkflowExecutionFailure`, `getWorkflowAlreadyStartedError`.
- Bind every `TODO(link)` dep shim to real crates (persistence managers, domain cache, history client, matching client, metrics, quotas).
### P1 - core workflow logic
- Mutable state builder (`mutable_state_builder.go` ~2479 LOC and 10 `_methods_*.go`): `AddWorkflowExecutionStartedEvent`, `ReplicateWorkflowExecutionStartedEvent`, `AddDecisionTaskScheduledEvent*`, `AddActivityTask*`, `AddStartChildWorkflowExecutionInitiatedEvent`, `AddSignalRequested`, `CloseTransactionAsMutation`, `CloseTransactionAsSnapshot`, `StartTransaction`, `UpdateCurrentVersion`, `AddContinueAsNewEvent`, `AddCompletedWorkflowEvent`, `AddFailWorkflowEvent`, `AddTimeoutWorkflowEvent`, `AddWorkflowExecutionTerminatedEvent`, `GetEffectiveVersion`.
- `execution/context.go`: `CreateWorkflowExecution`, `UpdateWorkflowExecutionAsActive/AsPassive`, `UpdateWorkflowExecutionWithNew`, `ConflictResolveWorkflowExecution`, `PersistStartWorkflowBatchEvents`, `PersistNonStartWorkflowBatchEvents`, `ReapplyEvents`, `LoadWorkflowExecution`, `emitSessionUpdateStats`, `mergeContinueAsNewReplicationTasks`.
- `execution/history_builder.go` (950 vs 186), `state_builder.go` `ApplyEvents` (581 vs 73), `state_rebuilder.go` `Rebuild` (281 vs 61), `mutable_state_task_generator.go` (714 vs 185), `mutable_state_task_refresher.go` (505 vs 94), `mutable_state_decision_task_manager.go` (884 vs 177), `timer_sequence.go`, `workflow_repairer.go`, `integrity.go`, `cache.go` (397 vs 97).
- Decision: `checker.go` (`attrValidator.validate*Attributes`, 958 vs 125), `handler.go` (`handleDecisionStartTimer`, `handleDecisionScheduleActivity`, `handleDecisionCompleteWorkflow`, `handleDecisionContinueAsNewWorkflow`, `handleDecisionStartChildWorkflow`, `handleDecisionSignalExternalWorkflow`, `handleDecisionUpsertWorkflowSearchAttributes`, 948 vs 271), `task_handler.go` (`handleDecisions`, 1241 vs 313).
- Missing engine ops: `DescribeWorkflowExecution`, `GetReplicationMessages`, `ReapplyEvents`, `RefreshWorkflowTasks`, `NotifyNewTransferTasks/TimerTasks`, `RecordDecisionTaskStarted`, `RespondDecisionTaskCompleted/Failed`, `DescribeQueues`, `ResetQueues` (partial).
- `task/timer_active_task_executor.go` (937 vs 184), `timer_standby_task_executor.go` (599 vs 113), `transfer_standby_task_executor.go` (767 vs 116), `timer_task_executor_base.go` (407 vs 103), `transfer_task_executor_base.go` (657 vs 127), `task/transfer_active_task_executor.go` (2045 vs 967, partial), `task/task.go` (552 vs 626 OK).
- `queue/timer_queue_processor.go`, `timer_queue_processor_base.go`, `timer_queue_active_processor.go`, `timer_queue_standby_processor.go`, `timer_queue_failover_processor.go`, `queue/factory.go`: no Rust file at all.
### P2 - replication / NDC / ancillary
- `reset/resetter.go`: `NewWorkflowResetter`, `ResetWorkflow`, `replayResetWorkflow`, `reapplyContinueAsNewWorkflowEvents`, `reapplyEvents`.
- `ndc/history_replicator.go` (903 vs 508), `transaction_manager.go`, `new/existing_workflow_transaction_manager.go`, `conflict_resolver.go`, `branch_manager.go`, `replication_task.go`, `workflow_resetter.go`.
- `failover/coordinator.go`, `marker_notifier.go`, `engine .registerDomainFailoverCallback`.
- `queuev2/*`: ratios are mostly 0.6-1.0 (real) but depend on `queuev2/deps.rs` stubs; `queue_mem.go`, `virtual_slice.go` verify.
- `lookup/lookup.go` (`HistoryServiceResolver`/shard hashing), `workflow/context.go`, `workflow/errors.go`, `workflow/util.go`, `simulation/event.go`, `common/errors.go`, `common/type.go`: no Rust counterpart.
### Outside go/service/history
- `go/client/history` (client.go, peer_resolver.go): Rust only has `cadence-frontend/src/history_client.rs` (480 LOC, frontend-side subset) and matching peer resolver; no shared history client crate with full method set/retry/redirect.
- `go/common/persistence`: history_manager.go, execution_manager.go, history_task_dlq_manager.go, versionHistory.go - Rust persistence crate has history_* files and executions.rs; verify DLQ manager, execution manager (`UpdateWorkflowExecution`, `ConflictResolveWorkflowExecution`, `CreateFailoverMarkerTasks`, `GetReplicationTasks`, `RangeCompleteHistoryTask`) and `versionHistory` completeness.
- `go/common/xdc` does not exist in this tree (no gap). Replication helpers used: `common/ndc` (verify), `common/reconciliation`, `common/task` (priority scheduler, `common/task/*`), `common/cache` (domain cache, history cache), `common/domain` failover watcher, `common/activecluster`, `common/archiver`, `common/membership`, `common/quotas`, `common/clock`, `common/future`, `common/dynamicconfig` - check each is bound in Rust (history deps currently carry local stand-ins).
