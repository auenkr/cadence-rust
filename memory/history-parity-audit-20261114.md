# History parity audit (2026-11-14), branch history-parityaudit

Method and limits: static analysis only (no cargo/go/docker). The per-file table below is a **name-based mapping** of every non-test Go file in `go/service/history/**` to Rust files under `rust/crates/cadence-history/src/` (snake_case filename match, plus a manual map for renamed or merged files such as `engineimpl/*`, `mutable_state_builder_methods_*`, wrappers). It proves a counterpart exists, not line-level equivalence. Behaviour evidence comes from the existing notes (`history-parity-audit-20261111/12/12-v2/13-final.md`, `history-p2rest-20261112.md`, `history-uncovered-20261113.md`, `history-crossreview-*-20261113.md`, `history-soakfix-20261113.md`) and the CI gate runs they record. Status: PORTED = counterpart exists and is covered by unit tests and/or `history-mixed-gate` (12 combos green on master 68620ea11 and later, see `history-uncovered-20261113.md`, `history-admingate-20261113.md`); PARTIAL = counterpart exists with a known open gap; MISSING = no counterpart; N-A = Go test/simulation helper.

## 1. Package summary
| Go package | Rust counterpart | Status | Evidence / open item |
|---|---|---|---|
| `.` (service.go) | `service.rs`, `launcher.rs`, `main.rs` | PORTED | gate bring-up, `history-runtime-20261109.md` |
| `common`, `constants`, `lookup` | `common.rs`, `constants.rs`, `lookup.rs` | PORTED | unit tests |
| `config` | `config.rs`, `config_collection.rs`, `wiring/resources/dynconfig.rs`, `openfeature_dynamic.rs` | PORTED | see section 3 |
| `decision` | `decision/{handler,task_handler,checker}.rs` | PORTED | `history-decisiongaps*`, `history-decfailover-20261112.md` |
| `engine`, `engine/engineimpl` | `engine/*`, `wiring/assembly/engine_*.rs` | PARTIAL | repair-by-rebuild on checksum mismatch not on engine path (P2); reviews found fixed items in `history-review2fix`/`xrfix` |
| `events` | `events/mod.rs`, `engine/notify_tasks.rs` | PORTED | |
| `execution`, `workflow` | `execution/*`, `mutable_state*.rs` | PORTED | `history-mutstate-unification-plan`, `history-mstxn`, `history-msoverlay` |
| `failover` | `failover/{coordinator,marker_notifier}.rs` | PARTIAL | `history-uncovered-20261113.md` lists coordinator as "not audited"; later `history-failcoord-20261113.md` covers it (re-verify) |
| `handler`, `wrappers/*` | `handler/*`, `wrappers/`, `wiring/server/*` | PORTED | `history-admingate-20261113.md` (admin/CLI API gate rows) |
| `ndc` | `ndc/*`, `wiring/assembly/ndc_*.rs` | PARTIAL | notifier classification of NDC write_failed/conflict-resolve/CreateHistoryTasks (P2); xcluster review findings, see section 4 |
| `query` | `query/mod.rs` | PORTED | |
| `queue` | `queue/*` | PORTED | |
| `queuev2` | `queuev2/*` | PORTED | not the default path in gates unless enabled (see gap P2-6) |
| `replication` | `replication/*` | PORTED | `history-activerepl`, `history-standbyrepl`, `history-replgen` |
| `reset` | `reset/{history,mod}.rs`, `ndc/workflow_resetter.rs` | PORTED | `history-retryreset-20261112.md` |
| `resource` | `wiring/resources/*` | PORTED | |
| `shard` | `shard.rs`, `shard_context.rs`, `wiring/assembly/controller.rs` | PARTIAL | review3 flagged strong-ref cycle per published shard (verify fixed) |
| `task` | `task/*` | PORTED | timer/transfer active+standby executors with tests |
| `taskdlq` | `taskdlq/*` | PORTED | |
| `workflowcache` | `workflowcache/mod.rs` | PORTED | |
| `archival` (in History) | `wiring/assembly/archiver_*.rs`, `cadence-common/src/archiver*` | PORTED | `history-archstream`, `history-s3gate` (real S3/GCS clients need credentials: not exercised) |
| `simulation`, `testing`, `templates` | n/a | N-A | |

## 2. Per-file mapping (Go file -> Rust)
| Go file (go/service/history/) | Rust (rust/crates/cadence-history/src/) | Status |
|---|---|---|
| `common/errors.go` | `wiring/server/errors.rs` | PORTED |
| `common/type.go` | `common.rs` | PORTED |
| `config/config.go` | `config.rs`, `queue/config.rs` | PORTED |
| `constants/constants.go` | `constants.rs`, `queue/constants.rs` | PORTED |
| `constants/test_constants.go` | (test/simulation helper) | N-A |
| `decision/checker.go` | `decision/checker.rs` | PORTED |
| `decision/handler.go` | `decision/handler.rs` | PORTED |
| `decision/task_handler.go` | `decision/task_handler.rs` | PORTED |
| `engine/engineimpl/describe_mutable_state.go` | `engine/mutable_state_admin.rs` | PORTED |
| `engine/engineimpl/describe_queues.go` | `engine/queue_operations.rs` | PORTED |
| `engine/engineimpl/describe_workflow_execution.go` | `engine/describe_workflow_execution.rs` | PORTED |
| `engine/engineimpl/dlq_operations.go` | `engine/dlq_operations.rs` | PORTED |
| `engine/engineimpl/get_replication_messages.go` | `engine/dlq_operations.rs`, `replication/task_ack_manager.rs` | PORTED |
| `engine/engineimpl/history_engine.go` | `engine/mod.rs`, `engine/lifecycle.rs`, `wiring/assembly/engine_factory.rs` | PORTED |
| `engine/engineimpl/notify_tasks.go` | `engine/notify_tasks.rs` | PORTED |
| `engine/engineimpl/poll_mutable_state.go` | `engine/poll_mutable_state.rs` | PORTED |
| `engine/engineimpl/query_workflow.go` | `engine/query_workflow.rs` | PORTED |
| `engine/engineimpl/reapply_events.go` | `engine/reapply_events.rs` | PORTED |
| `engine/engineimpl/record_activity_task_started.go` | `engine/record_activity_task_started.rs` | PORTED |
| `engine/engineimpl/record_child_execution_completed.go` | `engine/record_child_execution_completed.rs` | PORTED |
| `engine/engineimpl/record_decision_task_started.go` | `decision/handler.rs`, `engine/decision_replication.rs` | PORTED |
| `engine/engineimpl/refresh_workflow_tasks.go` | `engine/interface.rs`, `wiring/assembly/engine_refresher.rs` | PORTED |
| `engine/engineimpl/register_domain_failover_callback.go` | `engine/register_domain_failover_callback.rs` | PORTED |
| `engine/engineimpl/remove_signal_mutable_state.go` | `engine/interface.rs`, `engine/workflow_mutations.rs` | PORTED |
| `engine/engineimpl/request_cancel_workflow_execution.go` | `engine/workflow_mutations.rs` | PORTED |
| `engine/engineimpl/reset_queues.go` | `engine/queue_operations.rs` | PORTED |
| `engine/engineimpl/reset_sticky_tasklist.go` | `engine/workflow_mutations.rs` | PORTED |
| `engine/engineimpl/reset_workflow_execution.go` | `engine/reset_workflow_execution.rs` | PORTED |
| `engine/engineimpl/respond_activity_task_canceled.go` | `engine/activity_responses.rs` | PORTED |
| `engine/engineimpl/respond_activity_task_completed.go` | `engine/activity_responses.rs` | PORTED |
| `engine/engineimpl/respond_activity_task_failed.go` | `engine/activity_responses.rs` | PORTED |
| `engine/engineimpl/respond_activity_task_heartbeat.go` | `engine/activity_responses.rs` | PORTED |
| `engine/engineimpl/respond_decision_task_completed.go` | `engine/decision_replication.rs`, `wiring/assembly/engine_decision*.rs` | PORTED |
| `engine/engineimpl/respond_decision_task_failed.go` | `wiring/assembly/engine_decision_ops.rs` | PORTED |
| `engine/engineimpl/signal_workflow_execution.go` | `engine/signal_workflow_execution.rs` | PORTED |
| `engine/engineimpl/start_workflow_execution.go` | `engine/start_workflow_execution.rs` | PORTED |
| `engine/engineimpl/terminate_workflow_execution.go` | `engine/workflow_mutations.rs` | PORTED |
| `engine/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `engine/testdata/engine_for_tests.go` | (test/simulation helper) | N-A |
| `events/blob.go` | `events/mod.rs` | PORTED |
| `events/cache.go` | `execution/cache.rs` | PORTED |
| `events/notifier.go` | `engine/notify_tasks.rs`, `engine/poll_mutable_state.rs` | PORTED |
| `execution/cache.go` | `execution/cache.rs` | PORTED |
| `execution/checksum.go` | `execution/checksum.rs` | PORTED |
| `execution/context.go` | `execution/context.rs` | PORTED |
| `execution/context_util.go` | `execution/context_util.rs` | PORTED |
| `execution/history_builder.go` | `execution/history_builder.rs` | PORTED |
| `execution/integrity.go` | `execution/integrity.rs` | PORTED |
| `execution/mutable_state_builder.go` | `execution/mutable_state_builder.rs` | PORTED |
| `execution/mutable_state_builder_methods_activity.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_cancellation.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_child_workflow.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_completed.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_decision.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_semaphore.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_signal.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_started.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_timedout.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_builder_methods_timer.go` | `execution/mutable_state_methods_*.rs` | PORTED |
| `execution/mutable_state_decision_task_manager.go` | `execution/mutable_state_decision_task_manager.rs` | PORTED |
| `execution/mutable_state.go` | `mutable_state.rs` | PORTED |
| `execution/mutable_state_task_generator.go` | `execution/mutable_state_task_generator.rs` | PORTED |
| `execution/mutable_state_task_refresher.go` | `execution/mutable_state_task_refresher.rs` | PORTED |
| `execution/mutable_state_util.go` | `execution/mutable_state_util.rs` | PORTED |
| `execution/mutable_state_util_test_helpers.go` | (test/simulation helper) | N-A |
| `execution/retry.go` | `execution/retry.rs` | PORTED |
| `execution/state_builder.go` | `execution/state_builder.rs` | PORTED |
| `execution/state_rebuilder.go` | `execution/state_rebuilder.rs` | PORTED |
| `execution/timer_sequence.go` | `execution/timer_sequence.rs` | PORTED |
| `execution/workflow_execution_util.go` | `execution/workflow_execution_util.rs` | PORTED |
| `execution/workflow.go` | `execution/workflow.rs` | PORTED |
| `execution/workflow_repairer.go` | `execution/workflow_repairer.rs` | PORTED |
| `failover/coordinator.go` | `failover/coordinator.rs` | PORTED |
| `failover/marker_notifier.go` | `failover/marker_notifier.rs` | PORTED |
| `handler/handler.go` | `decision/handler.rs` | PORTED |
| `handler/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `lookup/lookup.go` | `lookup.rs` | PORTED |
| `ndc/activity_replicator.go` | `ndc/activity_replicator.rs` | PORTED |
| `ndc/branch_manager.go` | `ndc/branch_manager.rs` | PORTED |
| `ndc/conflict_resolver.go` | `ndc/conflict_resolver.rs` | PORTED |
| `ndc/events_reapplier.go` | `ndc/events_reapplier.rs` | PORTED |
| `ndc/existing_workflow_transaction_manager.go` | `ndc/existing_workflow_transaction_manager.rs` | PORTED |
| `ndc/history_replicator.go` | `ndc/history_replicator.rs` | PORTED |
| `ndc/new_workflow_transaction_manager.go` | `ndc/new_workflow_transaction_manager.rs` | PORTED |
| `ndc/replication_task.go` | `ndc/replication_task.rs` | PORTED |
| `ndc/transaction_manager.go` | `ndc/transaction_manager.rs` | PORTED |
| `ndc/workflow_resetter.go` | `ndc/workflow_resetter.rs` | PORTED |
| `query/query.go` | `query/mod.rs` | PORTED |
| `query/registry.go` | `query/mod.rs`, `engine/deps_ms.rs` | PORTED |
| `queue/action.go` | `queue/action.rs` | PORTED |
| `queue/constants.go` | `constants.rs`, `queue/constants.rs` | PORTED |
| `queue/domain_filter.go` | `queue/domain_filter.rs` | PORTED |
| `queue/factory.go` | `queue/factory.rs` | PORTED |
| `queue/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `queue/processing_queue_collection.go` | `queue/processing_queue_collection.rs` | PORTED |
| `queue/processing_queue.go` | `queue/processing_queue.rs` | PORTED |
| `queue/processing_queue_state.go` | `queue/processing_queue_state.rs` | PORTED |
| `queue/processor_base.go` | `queue/processor_base.rs` | PORTED |
| `queue/processor_options.go` | `queue/processor_options.rs` | PORTED |
| `queue/queue_processor_util.go` | `queue/util.rs` | PORTED |
| `queue/split_policy.go` | `queue/split_policy.rs` | PORTED |
| `queue/task_allocator.go` | `queue/task_allocator.rs` | PORTED |
| `queue/timer_queue_active_processor.go` | `queue/timer_queue_processor.rs`, `wiring/assembly/queue_deps_timer_active.rs` | PORTED |
| `queue/timer_queue_failover_processor.go` | `queue/timer_queue_processor.rs (failover_domain_async)` | PORTED |
| `queue/timer_queue_processor_base.go` | `queue/timer_queue_processor_base.rs` | PORTED |
| `queue/timer_queue_processor.go` | `queue/timer_queue_processor.rs` | PORTED |
| `queue/timer_queue_standby_processor.go` | `queue/timer_queue_processor.rs`, `wiring/assembly/queue_deps_standby.rs` | PORTED |
| `queue/transfer_queue_processor_base.go` | `queue/transfer_queue_processor_base.rs` | PORTED |
| `queue/transfer_queue_processor.go` | `queue/transfer_queue_processor.rs` | PORTED |
| `queue/transfer_queue_validator.go` | `queue/transfer_queue_validator.rs` | PORTED |
| `queuev2/alert.go` | `queuev2/alert.rs` | PORTED |
| `queuev2/convert.go` | `queuev2/convert.rs` | PORTED |
| `queuev2/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `queuev2/mitigator.go` | `queuev2/mitigator.rs` | PORTED |
| `queuev2/monitor.go` | `queuev2/monitor.rs` | PORTED |
| `queuev2/pause_controller.go` | `queuev2/pause_controller.rs` | PORTED |
| `queuev2/pending_task_tracker.go` | `queuev2/pending_task_tracker.rs` | PORTED |
| `queuev2/predicate.go` | `queuev2/predicate.rs` | PORTED |
| `queuev2/predicate_operation.go` | `queuev2/predicate_operation.rs` | PORTED |
| `queuev2/queue_base.go` | `queuev2/queue_base.rs` | PORTED |
| `queuev2/queue_cached.go` | `queuev2/queue_cached.rs` | PORTED |
| `queuev2/queue_immediate.go` | `queuev2/queue_immediate.rs` | PORTED |
| `queuev2/queue_mem.go` | `queuev2/queue_mem.rs` | PORTED |
| `queuev2/queue_reader_cached_base.go` | `queuev2/queue_reader_cached_base.rs` | PORTED |
| `queuev2/queue_reader_cached_immediate.go` | `queuev2/queue_reader_cached_immediate.rs` | PORTED |
| `queuev2/queue_reader_cached_scheduled.go` | `queuev2/queue_reader_cached_scheduled.rs` | PORTED |
| `queuev2/queue_reader.go` | `queuev2/queue_reader.rs` | PORTED |
| `queuev2/queue_scheduled.go` | `queuev2/queue_scheduled.rs` | PORTED |
| `queuev2/queue_state.go` | `queuev2/queue_state.rs` | PORTED |
| `queuev2/range.go` | `queuev2/range.rs` | PORTED |
| `queuev2/timer_queue_factory.go` | `queuev2/timer_queue_factory.rs` | PORTED |
| `queuev2/transfer_queue_factory.go` | `queuev2/transfer_queue_factory.rs` | PORTED |
| `queuev2/virtual_queue.go` | `queuev2/virtual_queue.rs` | PORTED |
| `queuev2/virtual_queue_manager.go` | `queuev2/virtual_queue_manager.rs` | PORTED |
| `queuev2/virtual_slice.go` | `queuev2/virtual_slice.rs` | PORTED |
| `replication/dlq_handler.go` | `replication/dlq_handler.rs` | PORTED |
| `replication/dynamic_task_batch_sizer.go` | `replication/dynamic_task_batch_sizer.rs` | PORTED |
| `replication/metrics_emitter.go` | `replication/metrics_emitter.rs` | PORTED |
| `replication/task_ack_manager.go` | `replication/task_ack_manager.rs` | PORTED |
| `replication/task_executor.go` | `replication/task_executor.rs` | PORTED |
| `replication/task_fetcher.go` | `replication/task_fetcher.rs` | PORTED |
| `replication/task_hydrator.go` | `replication/task_hydrator.rs` | PORTED |
| `replication/task_processor.go` | `replication/task_processor.rs` | PORTED |
| `replication/task_reader.go` | `replication/task_reader.rs` | PORTED |
| `replication/task_store.go` | `replication/task_store.rs` | PORTED |
| `reset/resetter.go` | `reset/mod.rs`, `ndc/workflow_resetter.rs`, `wiring/assembly/engine_resetter.rs` | PORTED |
| `resource/resource.go` | `wiring/resources/`, `service.rs` | PORTED |
| `resource/resource_test_utils.go` | (test/simulation helper) | N-A |
| `service.go` | `service.rs` | PORTED |
| `shard/context.go` | `execution/context.rs` | PORTED |
| `shard/context_test_utils.go` | (test/simulation helper) | N-A |
| `shard/controller.go` | `wiring/assembly/controller.rs` | PORTED |
| `shard/notifying_execution_manager.go` | `wiring/persistence/shard_events.rs`, `engine/notify_tasks.rs` | PORTED |
| `shard/task_notifier.go` | `engine/notify_tasks.rs`, `shard_context.rs` | PORTED |
| `simulation/event.go` | (test/simulation helper) | N-A |
| `task/constants.go` | `constants.rs`, `queue/constants.rs` | PORTED |
| `taskdlq/constants.go` | `constants.rs`, `queue/constants.rs` | PORTED |
| `taskdlq/host_limiter.go` | `taskdlq/host_limiter.rs` | PORTED |
| `taskdlq/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `taskdlq/processor.go` | `replication/processor_tests.rs`, `task/processor.rs` | PORTED |
| `task/event_logger.go` | `task/event_logger.rs` | PORTED |
| `task/executor_wrapper.go` | `task/executor_wrapper.rs` | PORTED |
| `task/interface.go` | `engine/interface.rs`, `handler/interface.rs` | PORTED |
| `task/priority_assigner.go` | `task/priority_assigner.rs` | PORTED |
| `task/processor.go` | `replication/processor_tests.rs`, `task/processor.rs` | PORTED |
| `task/rate_limited_processor.go` | `task/rate_limited_processor.rs` | PORTED |
| `task/redispatcher.go` | `task/redispatcher.rs` | PORTED |
| `task/rescheduler.go` | `task/rescheduler.rs` | PORTED |
| `task/standby_task_util.go` | `task/standby_task_util.rs` | PORTED |
| `task/task.go` | `task/history_task.rs`, `task/interface.rs` | PORTED |
| `task/task_rate_limiter.go` | `task/task_rate_limiter.rs` | PORTED |
| `task/task_util.go` | `task/task_util.rs` | PORTED |
| `task/timer_active_task_executor.go` | `task/timer_active_task_executor.rs`, `task/timer_active_task_executor_tests.rs` | PORTED |
| `task/timer_standby_task_executor.go` | `task/timer_standby_task_executor.rs`, `task/timer_standby_task_executor_tests.rs` | PORTED |
| `task/timer_task_executor_base.go` | `task/timer_task_executor_base.rs` | PORTED |
| `task/transfer_active_task_executor.go` | `task/transfer_active_task_executor.rs`, `task/transfer_active_task_executor_tests.rs` | PORTED |
| `task/transfer_standby_task_executor.go` | `task/transfer_standby_task_executor.rs`, `task/transfer_standby_task_executor_tests.rs` | PORTED |
| `task/transfer_task_executor_base.go` | `task/transfer_task_executor_base.rs` | PORTED |
| `workflowcache/cache.go` | `execution/cache.rs` | PORTED |
| `workflowcache/metrics.go` | `wiring/resources/metrics.rs` | PORTED |
| `workflow/context.go` | `execution/context.rs` | PORTED |
| `workflow/errors.go` | `wiring/server/errors.rs` | PORTED |
| `workflow/util.go` | `queue/util.rs` | PORTED |
| `wrappers/grpc/grpc_handler_generated.go` | `wiring/server/grpc*.rs` | PORTED |
| `wrappers/grpc/grpc_handler.go` | `wiring/server/grpc*.rs` | PORTED |
| `wrappers/ratelimited/handler_generated.go` | `wrappers/ratelimited.rs`, `wiring/server/limiter.rs` | PORTED |
| `wrappers/ratelimited/ratelimit.go` | `wrappers/ratelimited.rs`, `wiring/server/limiter.rs` | PORTED |
| `wrappers/thrift/thrift_handler_generated.go` | `wiring/server/thrift*.rs`, `wiring/server/tchannel.rs` | PORTED |
| `wrappers/thrift/thrift_handler.go` | `wiring/server/thrift*.rs`, `wiring/server/tchannel.rs` | PORTED |

Files that map only through the manual table (renamed/merged) are the riskiest for hidden omissions; they are the first candidates for a line-level re-read (`engineimpl/respond_*`, `execution/mutable_state_builder_methods_*`).

## 3. History dynamic config keys
Source: every `dynamicproperties.<Key>` referenced in `go/service/history/config/config.go` (269 distinct identifiers). Rust reads were checked by searching for the Go key string (e.g. `history.acquireShardInterval`) in `rust/crates/cadence-history`, `cadence-service-core`, `cadence-common` and `cadence-persistence`.
- **242 keys: Rust references the exact key string.** The full list is derivable with `grep -oE 'dynamicproperties\.[A-Za-z0-9]+' go/service/history/config/config.go`.
- **27 identifiers have no key string, 19 of which are not keys but property-function type names** (`IntPropertyFn`, `BoolPropertyFnWithDomainFilter`, `DurationPropertyFnWithShardIDFilter`, `MapPropertyFn`, ...). N-A.
- **8 real keys not found in the Rust history/common crates by key string** (all `frontend.*`, used by History's `config.go` for search-attribute validation): `frontend.enableQueryAttributeValidation`, `frontend.globalRatelimiterUpdateInterval`, `frontend.searchAttributesHiddenValueKeys`, `frontend.searchAttributesNumberOfKeysLimit`, `frontend.searchAttributesSizeOfValueLimit`, `frontend.searchAttributesTotalSizeLimit`, `frontend.validSearchAttributes`. Status: Frontend reads them in `cadence-frontend` (see `memory/frontend-validator-config-20261103.md`); whether History's own copy is read is unverified (P3).
- Caveat: a key string present in Rust is not proof that the key is wired to the same default or filter (domain/shard/task-list). Default/filter equivalence was verified per-subsystem in earlier notes, not exhaustively here. Static check also cannot catch keys built by concatenation.

## 4. Prioritized gap list
### P1 (breaks SDK/Go compatibility or load behaviour)
1. **History soak FAIL** (`history-soak-20261113.md`, `history-soakfix-20261113.md`): under sustained load (~10.8k workflows) only ~5-12 complete; the rest stay in query state `init` because decision tasks mostly never reach the worker. Two History-side root causes fixed (pending-decision guard, EntityNotExists classification), a stall remains (suspects: transfer queue to Matching dispatch). Deferred by user decision until after History migration; it must be closed before History completion is claimed. Note the memory file `fe-soak-fix-deferred` says the same for Frontend.
2. **Cross-cluster (NDC) replication from Go cluster to Rust cluster**: `history-crossreview-codex-20261113.md` reported that the second replicated batch could fail with "load with incoming task version needs StartTransaction decision failover", and that cross-workflow events lose progress on failover. `history-xrfix-20261113.md` records fixes; re-verify against a mixed Go-active/Rust-standby gate (the note `history-lastp2` says a gate assertion for global-domain replication rows is NOT done / infeasible, so replication is unit-tested only).
3. **Kafka TLS/SASL for the visibility producer** (`history-crossreview-codex-20261113.md`): Rust exits when the existing Kafka config has `tls.enabled: true`; SASL mechanism selection missing. Breaks drop-in replacement in secured ES/OpenSearch deployments. Verify whether fixed after 2026-11-13.
4. **Archival JSON format**: reviewers asked for a complete Go-JSON archive serialization layer (enums, nested attributes); verify against Go archive readers.

### P2 (behavior divergence)
1. Checksum mismatch: detect/log/metric only, no repair-by-rebuild on engine path.
2. NDC task-notifier classification for `write_failed`, conflict-resolve, `CreateHistoryTasks`.
3. Shard context strong-reference cycle (review3 P1) - confirm fix landed.
4. Failover coordinator parity - confirm `history-failcoord-20261113.md` closes the "not audited" row.
5. Known stubs: `MissingExecutorsProvider`, `UnwiredReplicator`, timer standby retry paths with `TODO(link)` (`history-auditp1-20261111.md`); `WorkflowRequest` has no type/version so strong-idempotency signal records have empty data.
6. `CadenceError` lacks CurrentBranchChanged, InternalDataInconsistency, RemoteSyncMatched, StickyWorkerUnavailable, ReadOnlyPartition, FeatureNotEnabled; ShardOwnershipLost/RetryTaskV2 details not attached; tchannel `fail` emits only BadRequest/Internal/AccessDenied (confirm still true).
7. queuev2 is ported but not shown as exercised by a gate.
8. Real S3/GCS network clients and Kafka authenticated variants have no CI evidence.

### P3 (cosmetic)
- Non-blob JSON tag mismatches (HTTP/JSON surfaces).
- Test volume gaps in engine/events/query/queue; stale `TODO(link)` comments.
- Domain id used in place of domain name for some metric tags.
- The 7 `frontend.*` keys in History `config.go` (section 3).

## 5. Not done in this audit
No line-by-line comparison; no execution of anything. Mapping is by filename. Recommended next step: line-level re-read of the manually mapped files in section 2 by workers, and a Go-active/Rust-standby replication gate.
