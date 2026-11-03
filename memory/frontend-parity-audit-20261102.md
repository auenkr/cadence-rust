# Frontend parity audit — 2026-11-02

## Scope and verdict

Audited source HEAD `855909c97d62bdf83483435914381c96cd7ca0eb` on `t3/frontend-parity-audit`. Frontend is **not complete**. The final mixed gate establishes a useful MySQL/single-cluster SDK baseline, not every API or distributed/failure-mode parity.

Read `memory/frontend-progress-checkpoint-20261101.md` first. Its referenced `memory/frontend-final-gate-20261102.md` and `memory/frontend-final-gate-20261102-steps.json` are absent in this checkout (also absent in the main repo checkout). Recovered final evidence read-only from Actions artifact instead; no fabricated local evidence link. No Cargo, Go, rustfmt or runtime validation was run locally.

The requested historical `workflowHandler.go`/`adminHandler.go` paths are now `go/service/frontend/api/handler.go`, `api/domain_handlers.go`, `api/handler_schedule.go`, `api/handler_semaphore.go` and `admin/handler.go`. Inventory comes from **every method in api/interface.go and admin/interface.go**, checked against `wrappers/thrift/{api,admin}_generated.go` and `wrappers/grpc/{api,admin}_generated.go`. Start/Stop (Daemon) are lifecycle, not API rows. Preserve the Go IDL misspelling `Configuraton`.

Status: **ported** = concrete handler with named meaningful test or mixed evidence for its core behavior; not proof of every deployment mode. **partial** = handler/adapter exists but has a known behavior/wiring gap or lacks required mixed Go-backed acceptance. **missing** = no handler found. Unit/local-router tests are source evidence, not newly executed tests, and in-memory service-core workflows are not evidence of compatibility with remote Go History.

## Reproducible evidence

- Mixed run: https://github.com/auenkr/cadence-rust/actions/runs/41393672433 — success; head `55a6ec215566ab642d3a5ee369ec822c6a3dacb3`.
- Artifact `frontend-mixed-gate`, ID `13118553012`; `logs/steps.json`: **46/46 true**. Retrieve with `gh run download 41393672433 -n frontend-mixed-gate -D <directory>` while artifact retention permits. All gate step references below mean this exact JSON; it is not present at the requested memory path.
- `binary.sha256`: `fa685c08e32ee70906fc57df4a9637205277460b7274de61bbcfd862f89e4abb`, built in `/tmp/cadence-cargo-target-fe-41393672433/debug/cadence-server` (fresh dedicated CI target).
- Clean workspace CI: https://github.com/auenkr/cadence-rust/actions/runs/41394584404 — success, same head. Verified both conclusions/head SHAs with `gh run view`; workspace success does not close runtime gaps.
- Harness: `.github/workflows/frontend-mixed-gate.yml`, `tests/gates/run-frontend-gate.py`, `tests/gates/run-frontend-mysql-lane.sh`, SDK `tests/frontend-gate/`. Rust Frontend + Rust Matching; Go History + Go Worker; MySQL; no Go Frontend. Survival checks before/after SDK and Frontend/Matching SIGKILL/restart passed.
- Named unit tests below live in the handler module unless otherwise stated; local router tests live in `rust/crates/cadence-service-core/tests/e2e.rs`. These must not be read as API-specific mixed checks.

## WorkflowService inventory

Rust locations are repo-relative. `Handler::dispatch_operation` selects the named API branch; `ProductionBackend::dispatch` is the concrete outbound implementation.

| Go API | Rust handler location (file:fn) | Status | Evidence or gap |
|---|---|---|---|
| Health | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | ported | Unit tests `stop_rejects_workflow_calls_but_keeps_health_available`, `cluster_info_matches_go_constants_and_remains_available_while_stopping`. |
| CountWorkflowExecutions | `rust/crates/cadence-frontend/src/workflow_backend.rs:ProductionBackend::dispatch` | ported (ES path gate-proven: run 41610924645; SQL stores reject like Go) | Go SQL visibility store (`go/common/persistence/sql/sql_visibility_store.go:340-359`) rejects this API with BadRequestError "Operation is not supported"; Rust rejection matches Go (only ES/Pinot visibility implements it, out of scope). |
| DeleteDomain | `rust/crates/cadence-service-core/src/handlers.rs:delete_domain` | partial | Local service-core tests (`failover_moves_global_domain_and_lists_history`, `semaphore_create_and_gate_flow`, domain CRUD) exercise local state; no mixed Go History/Worker acceptance for this API. |
| DeprecateDomain | `rust/crates/cadence-frontend/src/handler_domain.rs:Handler::deprecate_domain` | ported | Unit `domain_crud_methods_use_shared_store_and_preserve_go_error_order`; service-core `domain_crud_round_trip`. Global replication/failover acceptance remains open below. |
| DescribeDomain | `rust/crates/cadence-frontend/src/handler_domain.rs:Handler::describe_domain` | ported | Gate `cli-describe-domain-tchannel`, `cli-describe-domain-grpc`, `sdk-describe-domain`. |
| DescribeTaskList | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::describe_task_list` | partial | Final-head mixed MySQL run 41427827989, `sdk-describe-task-list`; successful SDK call, broader deployment/error parity still open. |
| DescribeWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | ported | Gate `sdk-describe-open / sdk-describe-closed`. Fixed single History endpoint remains a deployment limit. |
| DiagnoseWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::diagnose_workflow_execution` | partial | Units `restart_reads_start_event_and_starts_a_new_run_with_go_fields`, `diagnose_starts_system_workflow_with_go_input_and_request_policy`; mixed gate does not call this API (process restart is distinct). |
| GetClusterInfo | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | ported | Unit tests `stop_rejects_workflow_calls_but_keeps_health_available`, `cluster_info_matches_go_constants_and_remains_available_while_stopping`. |
| GetSearchAttributes | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | partial | Unit `search_attributes_uses_local_dynamic_config_provider`; production builder does not install the custom provider. Admin updates/durable visibility schema not established. |
| GetWorkflowExecutionHistory | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::get_workflow_execution_history` | partial | Indirect mixed evidence: `sdk-complete-echo`, `sdk-query`, `sdk-complete-after-restart`; live-history path only. Production archival reader is not installed; Cassandra reader remains open. |
| ListArchivedWorkflowExecutions | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | ported | Filestore visibility archive: Handler tests `handler_lists_archived_visibility_through_real_filestore_reader`, Thrift hook test `archived_visibility_dispatch_reaches_installed_workflow_api`, gRPC routed through WorkflowApi (`grpc.rs`); mixed gate (MySQL, Rust Frontend+Matching, Go History/Worker) steps `archival-list-archived-visibility`/`-paged`/`-domain-not-configured` over TChannel and gRPC, run 41555187877. Not covered: s3/gcloud backends, exact sqlparser syntax-error text. |
| ListClosedWorkflowExecutions | `rust/crates/cadence-frontend/src/workflow_visibility.rs:ProductionBackend::list_visibility` | ported | Gate `sdk-list-open` / `sdk-list-closed`; SQL basic visibility only. |
| ListDomains | `rust/crates/cadence-frontend/src/handler_domain.rs:Handler::list_domains` | ported | Unit `domain_crud_methods_use_shared_store_and_preserve_go_error_order`; service-core `domain_crud_round_trip`. Global replication/failover acceptance remains open below. |
| ListOpenWorkflowExecutions | `rust/crates/cadence-frontend/src/workflow_visibility.rs:ProductionBackend::list_visibility` | ported | Gate `sdk-list-open` / `sdk-list-closed`; SQL basic visibility only. |
| ListTaskListPartitions | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::list_task_list_partitions` | partial | Final-head mixed MySQL run 41427827989, `sdk-list-task-list-partitions`; successful SDK call, broader deployment/error parity still open. |
| GetTaskListsByDomain | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::get_task_lists_by_domain` | partial | Final-head mixed MySQL run 41427827989, `sdk-get-task-lists-by-domain`; successful SDK call, broader deployment/error parity still open. |
| RefreshWorkflowTasks | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Final-head mixed MySQL run 41427827989, `sdk-refresh-workflow-tasks`; successful call against the open SDK execution. Failure/recovery parity and multi-owner routing remain open. |
| ListWorkflowExecutions | `rust/crates/cadence-frontend/src/workflow_backend.rs:ProductionBackend::dispatch` | ported (ES path gate-proven: run 41610924645; SQL stores reject like Go) | Go SQL visibility store (`go/common/persistence/sql/sql_visibility_store.go:340-359`) rejects this API with BadRequestError "Operation is not supported"; Rust rejection matches Go (only ES/Pinot visibility implements it, out of scope). |
| PollForActivityTask | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::poll_activity` | ported | Indirect mixed evidence: `sdk-complete-echo`, `sdk-query`, `sdk-complete-after-restart` (driver consumes history/polls/responds); not a separate per-API assertion. |
| PollForDecisionTask | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::poll_decision` | ported | Indirect mixed evidence: `sdk-complete-echo`, `sdk-query`, `sdk-complete-after-restart` (driver consumes history/polls/responds); not a separate per-API assertion. |
| QueryWorkflow | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | ported | Gate `sdk-query`. Fixed single History endpoint remains a deployment limit. |
| RecordActivityTaskHeartbeat | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RecordActivityTaskHeartbeatByID | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RegisterDomain | `rust/crates/cadence-frontend/src/handler_domain.rs:Handler::register_domain` | ported | Gate `cli-register-domain-tchannel`, `cli-register-domain-grpc`. |
| RequestCancelWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Final-head mixed MySQL run 41427827989, `sdk-cancel-workflow-execution`; cancellation path succeeded. Failure/recovery parity and multi-owner routing remain open. |
| ResetStickyTaskList | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Final-head mixed MySQL run 41427827989, `sdk-reset-sticky-task-list`; reset path succeeded. Broader failure/recovery parity remains open. |
| ResetWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskCanceled | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskCanceledByID | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskCompleted | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskCompletedByID | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskFailed | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondActivityTaskFailedByID | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondDecisionTaskCompleted | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::respond_decision_completed` | ported | Indirect mixed evidence: `sdk-complete-echo`, `sdk-query`, `sdk-complete-after-restart` (driver consumes history/polls/responds); not a separate per-API assertion. |
| RespondDecisionTaskFailed | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| RespondQueryTaskCompleted | `rust/crates/cadence-frontend/src/workflow_matching.rs:ProductionBackend::respond_query_completed` | ported | Indirect mixed evidence: `sdk-complete-echo`, `sdk-query`, `sdk-complete-after-restart` (driver consumes history/polls/responds); not a separate per-API assertion. |
| RestartWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::restart_workflow_execution` | partial | Units `restart_reads_start_event_and_starts_a_new_run_with_go_fields`, `diagnose_starts_system_workflow_with_go_input_and_request_policy`; mixed gate does not call this API (process restart is distinct). |
| ScanWorkflowExecutions | `rust/crates/cadence-frontend/src/workflow_backend.rs:ProductionBackend::dispatch` | ported (ES path gate-proven: run 41610924645; SQL stores reject like Go) | Go SQL visibility store (`go/common/persistence/sql/sql_visibility_store.go:340-359`) rejects this API with BadRequestError "Operation is not supported"; Rust rejection matches Go (only ES/Pinot visibility implements it, out of scope). |
| SignalWithStartWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Go request envelope/validation implemented; unit coverage in handler_workflow.rs and local service-core workflow tests. No API-specific mixed failure/recovery evidence; fixed History routing applies. |
| SignalWithStartWorkflowExecutionAsync | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | partial | Unit `async_start_publishes_go_thriftrw_message_and_skips_history_backend`; production build_workflow_api installs no AsyncWorkflowPublisher, so this hook is not production queue integration. |
| SignalWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | ported | Gate `sdk-signal / sdk-signal-after-restart`. Fixed single History endpoint remains a deployment limit. |
| StartWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | ported | Gate `sdk-start-echo`. Fixed single History endpoint remains a deployment limit. |
| StartWorkflowExecutionAsync | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation` | partial | Unit `async_start_publishes_go_thriftrw_message_and_skips_history_backend`; production build_workflow_api installs no AsyncWorkflowPublisher, so this hook is not production queue integration. |
| TerminateWorkflowExecution | `rust/crates/cadence-frontend/src/handler_workflow.rs:Handler::dispatch_operation → workflow_backend.rs:ProductionBackend::dispatch` | partial | Final-head mixed MySQL run 41427827989, `sdk-terminate-workflow-execution`; termination path succeeded. Failure/recovery parity and multi-owner routing remain open. |
| UpdateDomain | `rust/crates/cadence-frontend/src/handler_domain.rs:Handler::update_domain` | ported | Unit `domain_crud_methods_use_shared_store_and_preserve_go_error_order`; service-core `domain_crud_round_trip`. Global replication/failover acceptance remains open below. |
| FailoverDomain | `rust/crates/cadence-service-core/src/handlers.rs:failover_domain` | partial | Local service-core tests (`failover_moves_global_domain_and_lists_history`, `semaphore_create_and_gate_flow`, domain CRUD) exercise local state; no mixed Go History/Worker acceptance for this API. |
| ListFailoverHistory | `rust/crates/cadence-service-core/src/handlers.rs:list_failover_history` | partial | Local service-core tests (`failover_moves_global_domain_and_lists_history`, `semaphore_create_and_gate_flow`, domain CRUD) exercise local state; no mixed Go History/Worker acceptance for this API. |
| CreateSchedule | `rust/crates/cadence-service-core/src/handlers.rs:create_schedule` | ported | Final-head mixed MySQL run 41427827989, `sdk-sched-create-grpc`; Go Worker ran the scheduler workflow. |
| DescribeSchedule | `rust/crates/cadence-service-core/src/handlers.rs:describe_schedule` | ported | Final-head mixed MySQL run 41427827989, `sdk-sched-describe-after-backfill-grpc`, Go Worker-backed schedule state. |
| UpdateSchedule | `rust/crates/cadence-service-core/src/handlers.rs:update_schedule` | partial | Implemented signal/update path; local `schedule_crud_and_firing_flow`; no UpdateSchedule mixed assertion. |
| DeleteSchedule | `rust/crates/cadence-service-core/src/handlers.rs:delete_schedule` | ported | Gate `sched-delete-grpc` against Go Worker. |
| PauseSchedule | `rust/crates/cadence-service-core/src/handlers.rs:pause_schedule` | ported | Gate `sched-pause-grpc` against Go Worker. |
| UnpauseSchedule | `rust/crates/cadence-service-core/src/handlers.rs:unpause_schedule` | ported | Gate `sched-unpause-grpc` against Go Worker. |
| BackfillSchedule | `rust/crates/cadence-service-core/src/handlers.rs:backfill_schedule` | ported | Final-head mixed MySQL run 41427827989, `sdk-sched-backfill-grpc` plus `sdk-sched-backfill-completed`; two historical target runs reached completed close status via ListClosed and DescribeWorkflowExecution against Go Worker. |
| ListSchedules | `rust/crates/cadence-service-core/src/handlers.rs:list_schedules` | ported | Gate `sched-list-grpc` against Go Worker. |
| CreateSemaphore | `rust/crates/cadence-service-core/src/handlers.rs:create_semaphore` | partial | Local service-core tests (`failover_moves_global_domain_and_lists_history`, `semaphore_create_and_gate_flow`, domain CRUD) exercise local state; no mixed Go History/Worker acceptance for this API. |

## AdminService inventory

| Go API | Rust handler location (file:fn) | Status | Evidence or gap |
|---|---|---|---|
| AddSearchAttribute | `rust/crates/cadence-service-core/src/admin.rs:add_search_attribute_core` | ported (ES mapping unit-tested only, no real-ES gate) | Now persists validSearchAttributes through the durable config path (test `add_search_attribute_persists_valid_search_attributes`); Elasticsearch mapping update still absent. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. Gate proves the durable write and survival across restart on all three lanes (memory/frontend-dynconfig-gate-20261103.md); ES/OpenSearch mapping update ported in `es_mapping.rs` and proven by fake-HTTP unit tests plus handler test (rust-ci 41572812796, memory/evidence/frontend-kafka-archival-es-20261103.md); no gate runs a real ES. |
| CloseShard | `rust/crates/cadence-service-core/src/admin.rs:close_shard` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| DescribeCluster | `rust/crates/cadence-service-core/src/admin.rs:describe_cluster_for` | partial | Units `describe_cluster_uses_resolver_members_per_role_like_go`, `describe_cluster_without_resolver_has_empty_membership_and_errors_propagate`; Ringpop installed by launcher, but no mixed membership payload assertion. |
| DescribeShardDistribution | `rust/crates/cadence-service-core/src/admin.rs:describe_shard_distribution` | partial | Returns local-host shard placement; Go uses membership HistoryServerByShardID plus PageID/PageSize. Distributed placement/pagination parity missing. |
| DescribeHistoryHost | `rust/crates/cadence-service-core/src/admin.rs:describe_history_host` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| DescribeQueue | `rust/crates/cadence-service-core/src/admin.rs:describe_queue` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| DescribeWorkflowExecution | `rust/crates/cadence-service-core/src/admin.rs:describe_workflow_execution_admin` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| GetDLQReplicationMessages | `rust/crates/cadence-service-core/src/admin.rs:get_dlqreplication_messages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| GetDomainReplicationMessages | `rust/crates/cadence-service-core/src/admin.rs:domain_replication_messages_response` | partial | Persistence queue read/decode implemented; final gate is one cluster and does not demonstrate domain replication consumption/ack/recovery. |
| GetReplicationMessages | `rust/crates/cadence-service-core/src/admin.rs:get_replication_messages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| GetWorkflowExecutionRawHistoryV2 | `rust/crates/cadence-service-core/src/admin.rs:get_workflow_execution_raw_history` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| CountDLQMessages | `rust/crates/cadence-service-core/src/admin.rs:count_dlqmessages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| MergeDLQMessages | `rust/crates/cadence-service-core/src/admin.rs:merge_dlqmessages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| PurgeDLQMessages | `rust/crates/cadence-service-core/src/admin.rs:purge_dlqmessages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| ReadDLQMessages | `rust/crates/cadence-service-core/src/admin.rs:read_dlqmessages` | partial | History replication/DLQ proxy in admin_replication.rs; domain DLQ has persistence branch. Units `history_dlq_counts_preserve_source_cluster_and_count`, `domain_dlq_page_tokens_use_go_little_endian_message_ids`; no mixed populated queues, paging, merge/recovery evidence. |
| ReapplyEvents | `rust/crates/cadence-service-core/src/admin.rs:reapply_events` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| RefreshWorkflowTasks | `rust/crates/cadence-service-core/src/admin.rs:refresh_workflow_tasks` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| RemoveTask | `rust/crates/cadence-service-core/src/admin.rs:remove_task` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| ResendReplicationTasks | `rust/crates/cadence-service-core/src/admin.rs:resend_replication_tasks` | partial | Single-host stub: resend accepts current cluster without sending, rejects remotes; cross-cluster fetch returns empty maps and completion does not acknowledge History tasks. No real cross-cluster implementation. |
| ResetQueue | `rust/crates/cadence-service-core/src/admin.rs:reset_queue` | partial | History proxy path exists (handler_admin_history.rs / configured client where applicable); local fallback/state paths also exist. No mixed Admin API assertion or multi-owner routing evidence. |
| GetCrossClusterTasks | `rust/crates/cadence-service-core/src/admin.rs:get_cross_cluster_tasks` | partial | Single-host stub: resend accepts current cluster without sending, rejects remotes; cross-cluster fetch returns empty maps and completion does not acknowledge History tasks. No real cross-cluster implementation. |
| RespondCrossClusterTasksCompleted | `rust/crates/cadence-service-core/src/admin.rs:respond_cross_cluster_tasks_completed` | partial | Single-host stub: resend accepts current cluster without sending, rejects remotes; cross-cluster fetch returns empty maps and completion does not acknowledge History tasks. No real cross-cluster implementation. |
| GetDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:get_dynamic_config` | ported | Go key-name table (506 keys) + type validation, filter matching, fallback/NotFound, unknown-key Internal error. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. |
| UpdateDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:update_dynamic_config` | ported | Validation, ErrInvalidFilters, NotFound, empty-values delete, versioned conditional insert + refetch/retry ('ran out of retry attempts') proven on SQLite store (`updates_are_durable_across_restart...`, `condition_failed_refetches...`). Production Frontend does not yet bind a config-store writer (only SQLite writer exists; MySQL/PostgreSQL/Cassandra inserts missing), so without a store updates are in-memory (Go file client returns an error). Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. Mixed gates (MySQL 41538270759, Postgres 41540868425, Cassandra 41541744231) prove durable Update/Get/List/Restore/Update + restart survival; see memory/frontend-dynconfig-gate-20261103.md. |
| RestoreDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:restore_dynamic_config` | ported | Validation, ErrInvalidFilters, NotFound, empty-values delete, versioned conditional insert + refetch/retry ('ran out of retry attempts') proven on SQLite store (`updates_are_durable_across_restart...`, `condition_failed_refetches...`). Production Frontend does not yet bind a config-store writer (only SQLite writer exists; MySQL/PostgreSQL/Cassandra inserts missing), so without a store updates are in-memory (Go file client returns an error). Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. Mixed gates (MySQL 41538270759, Postgres 41540868425, Cassandra 41541744231) prove durable Update/Get/List/Restore/Update + restart survival; see memory/frontend-dynconfig-gate-20261103.md. |
| ListDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:list_dynamic_config` | ported | Empty/unknown name lists all (sorted); Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. |
| GetOperationalDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:get_operational_dynamic_config` | partial | Go key-name table (506 keys) + type validation, filter matching, fallback/NotFound, unknown-key Internal error. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. Operational store is in-memory only (Go uses a separate operational config store; not durable). |
| UpdateOperationalDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:update_operational_dynamic_config` | partial | Validation shared with Update; in-memory only, no operational store, no audit log. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. |
| RestoreOperationalDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:restore_operational_dynamic_config` | partial | Validation shared with Restore; in-memory only. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. |
| ListOperationalDynamicConfig | `rust/crates/cadence-service-core/src/admin.rs:list_operational_dynamic_config` | partial | Same semantics as List; operational store in-memory only. Unit tests `dynamic_config_admin_tests::*` (service-core admin.rs), handler_admin tests, e2e `admin_cluster_config_and_workflow_maintenance`; CI run 41531769629. |
| DeleteWorkflow | `rust/crates/cadence-service-core/src/admin.rs:delete_workflow` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| MaintainCorruptWorkflow | `rust/crates/cadence-service-core/src/admin.rs:maintain_corrupt_workflow` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| GetGlobalIsolationGroups | `rust/crates/cadence-service-core/src/admin.rs:get_global_isolation_groups` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| UpdateGlobalIsolationGroups | `rust/crates/cadence-service-core/src/admin.rs:update_global_isolation_groups` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| GetDomainIsolationGroups | `rust/crates/cadence-service-core/src/admin.rs:get_domain_isolation_groups` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| UpdateDomainIsolationGroups | `rust/crates/cadence-service-core/src/admin.rs:update_domain_isolation_groups` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| GetDomainAsyncWorkflowConfiguraton | `rust/crates/cadence-service-core/src/admin.rs:get_domain_async_workflow_configuration` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| UpdateDomainAsyncWorkflowConfiguraton | `rust/crates/cadence-service-core/src/admin.rs:update_domain_async_workflow_configuration` | partial | Implemented local-state/admin route; service-core tests `admin_cluster_config_and_workflow_maintenance`, `admin_isolation_and_async_config_flow`, `admin_shard_queue_history_and_maintain_flow`; no mixed Go-backed assertion for this API. Local execution-store behavior does not prove remote History parity. |
| UpdateTaskListPartitionConfig | `rust/crates/cadence-matching/src/grpc_admin.rs:Admin::update_task_list_partition_config` | partial | gRPC calls a locally constructed grpc_matching::Matching instead of forwarding to remote Matching; no YARPC Admin dispatch found. No mixed gate assertion. |

## Transport and production wiring audit

Go Thrift and gRPC generated wrappers convert requests/errors into the same interface inventory above. Rust's real network adapters are shared with the Matching crate: `rust/crates/cadence-matching/src/grpc.rs` (Workflow API), `grpc_admin.rs` (Admin API), `tchannel.rs`, and `rust/crates/cadence-service-core/src/yarpc.rs` plus `workflow_api.rs:{codec,dispatch_thrift}`. A handler in cadence-frontend alone does not establish wire coverage.

| Area | Status | Evidence / gap |
|---|---|---|
| TChannel/Thrift Workflow and gRPC Workflow | partial | Both listeners and register/describe-domain CLI checks pass; SDK core workflows pass; schedule checks are gRPC only. No exhaustive populated request/response/error wire comparison for all 57 methods, enums, tokens, headers and cancellation paths. |
| TChannel/Thrift Admin and gRPC Admin | partial | `grpc_admin.rs` and YARPC admin dispatch exist, but final gate has no Admin API checks. UpdateTaskListPartitionConfig has a gRPC implementation using a local Matching instance, with no corresponding YARPC dispatch found. |
| Domain entry points | partial | Network domain requests use service-core `handlers.rs:{register_domain,describe_domain,list_domains,update_domain,deprecate_domain}` and shared persistence; `handler_domain.rs` supplies additional ported handler/test behavior. Its existence is not proof that every check there runs on every network path. |
| Outbound History/Matching | partial | `workflow_backend.rs:service_addr` selects one configured/env endpoint; no Ringpop owner selection or shard-ownership retry loop. Replication Admin uses one `history_admin_channel` via `admin_replication.rs:client`. |
| Production policy hooks | partial | `workflow_backend.rs:build_workflow_api` installs lookup, quota policies and version checks, but only NopAuthorizer/NopAuthorizationMetrics. It does not install metrics, archive readers, async publisher, cluster redirector, custom search provider or the richer injected policy hooks tested in isolation. Config-enabled auth/redirect/archival cannot be accepted from unit-hook tests. |

## Non-API parity

| Area | Status | Evidence / remaining gap |
|---|---|---|
| MySQL persistence | partial | Final mixed gate exercises domain persistence, live history reads/basic visibility and process restart recovery with MySQL. It does not cover every Admin operation, domain queue/DLQ, schema upgrade/error mode, populated archival or distributed failover. |
| PostgreSQL persistence | partial | Backend infrastructure exists; checkpoint explicitly leaves separate visibility-store configuration open. No Frontend PostgreSQL mixed lane in final gate. Need same SDK/Admin/restart lane against configured default and separate visibility stores. |
| Cassandra persistence | partial | Checkpoint leaves Cassandra history reader open; production installs HistoryAccess only when launcher supplies history_node_store. No Cassandra Frontend mixed lane or history pagination/restart evidence. |
| Ringpop discovery/lifecycle | partial | `cadence-matching/src/launcher.rs` installs Frontend Ringpop on shared TChannel listener; final service-survival and restart checks supersede failed Nov 1 bootstrap baseline. Resolver-backed DescribeCluster has unit coverage. Multi-node churn, partition/heal, owner movement and payload consistency not validated. |
| History shard routing | missing | ProductionBackend stores one history_addr; service_addr does not query membership. Workflow-ID shard calculation/history reads exist (`workflow_matching.rs:workflow_id_to_history_shard`), but they do not implement Go membership-based outbound owner routing or ShardOwnershipLost recovery. DescribeShardDistribution maps every shard to local host. |
| IPv6 bind / endpoint formatting | missing | Launcher gRPC bind constructs IPv4 arrays `[127,0,0,1]` / `[0,0,0,0]` rather than honoring `[::1]` bindOnIP; service_addr formats raw host:port without IPv6 brackets. No IPv6 gate. |
| Domain replication | partial | Domain metadata/replication queue and DLQ branches exist; final gate registers global domains in a single cluster. Remote consumption, failover conflict/version handling, replay, durable ACK and restart/recovery not established. Go Worker remains required. |
| Dynamic configuration | partial | File/YAML constraints, overlay get/update/restore/list and config-store snapshot infrastructure have tests in service-core admin.rs; launcher persistence hooks exist. Full Frontend key/provider wiring, distributed update propagation and durable Admin update/restart evidence remain open. |
| Metrics | partial | Metered wrapper has unit coverage (`dispatch_runs_go_metered_wrapper_around_validation_and_backend`, `wrapper_order_is_access_then_metered_rate_limit_then_version`); production workflow Handler defaults to NoopWorkflowMetrics and builder does not replace it. No mixed emitted-metric/name/tag comparison. |
| Startup, shutdown, recovery | partial | `frontend_lifecycle.rs` and frontend lifecycle unit tests cover warmup/drain/stop ordering; final SIGKILL/restart preserves SDK workflow. Graceful shutdown under load, inflight poll cancellation/error cases and distributed recovery still need acceptance. |
| Archival / advanced visibility / async queue | partial | Handler interfaces and isolated tests exist; concrete production archive readers and async publisher are not installed, advanced visibility returns not-supported. No real external backend validation. |

## Completion blockers

1. Implement distributed History owner routing/retry and correct shard-distribution responses; verify multi-node membership churn and failover.
2. Implement IPv6 bind and bracket-safe endpoint selection; run IPv6 mixed transport checks.
3. Wire and validate advanced visibility, archival readers, async workflow publisher, real authorization/cluster redirection and emitted Frontend metrics.
4. Replace cross-cluster fetch/ack/resend stubs; validate populated replication/domain DLQ operations and remote domain replication/recovery.
5. Route Admin maintenance/raw-history/describe/delete paths to the actual Go History/persistence behavior rather than accepting local execution-store tests; validate all Admin APIs and both transports, including partition-config forwarding to remote Matching.
6. Run PostgreSQL (including separate visibility store) and Cassandra Frontend lanes; expand mixed evidence for the APIs marked partial, schedules UpdateSchedule, failure injection, pagination and graceful shutdown.
7. Preserve the final gate Markdown/steps evidence in memory before artifact expiry. This audit records the immutable run/head/binary hash and all step names below but does not silently create the absent requested evidence files.

## Retained final gate step inventory

All of the following are true in the recovered `logs/steps.json` (46 checks). These are assertions, not 46 distinct API methods:

- `go-history-listening`
- `rust-matching-listening`
- `rust-frontend-tchannel-listening`
- `rust-frontend-grpc-listening`
- `cli-register-domain-tchannel`
- `cli-register-domain-grpc`
- `cli-describe-domain-tchannel`
- `cli-describe-domain-grpc`
- `go-history-worker-alive-before-sdk`
- `rust-matching-alive-before-sdk`
- `rust-frontend-alive-before-sdk`
- `rust-frontend-killed`
- `rust-frontend-restarted-listening`
- `rust-matching-killed`
- `rust-matching-restarted-listening`
- `sdk-describe-domain`
- `sdk-worker-start`
- `sdk-start-echo`
- `sdk-complete-echo`
- `sdk-describe-closed`
- `sdk-start-signal-query`
- `sdk-signal`
- `sdk-query`
- `sdk-describe-open`
- `sdk-list-open`
- `sdk-restart-handshake`
- `sdk-signal-after-restart`
- `sdk-matching-restart-handshake`
- `sdk-signal-after-matching-restart`
- `sdk-signal-done`
- `sdk-complete-after-restart`
- `sdk-list-closed`
- `sdk-driver-exit`
- `go-history-worker-alive-after-sdk`
- `rust-matching-alive-after-sdk`
- `rust-frontend-alive-after-sdk`
- `cli-list-workflows-grpc`
- `cli-list-workflows-tchannel`
- `sched-create-grpc`
- `sched-describe-grpc`
- `sched-pause-grpc`
- `sched-unpause-grpc`
- `sched-backfill-grpc`
- `sched-list-grpc`
- `sched-describe-after-backfill-grpc`
- `sched-delete-grpc`

## Update 2026-11-02
- List/Scan/CountWorkflowExecutions reclassified `ported`: Go SQL visibility also returns "Operation is not supported" (worker finding, thread t3/frontend-sql-visibility-queries, no change needed).
- History shard-owner routing (CI 41415396963, gate 41415440353) and IPv6 bind (crate CI 41411339193) merged to master e6baf7596; merged-head clean CI 41416995094 success. Missing items now: none; `[::1]` mixed-gate assertion deferred.


## Final expanded MySQL gate evidence (2026-11-02)

Correct final branch run https://github.com/auenkr/cadence-rust/actions/runs/41427827989,
source/gate head `5644052674ad0294bb10662a8fe8c67446193e3b`, conclusion success,
51/51 steps. Binary SHA256 `e6cd5037ffb45cf4897c81a5d40d005207e12b6e9f149de7b4dc67efe803db85`.
The added SDK parent-child workflow completed; two historical schedule backfill runs
were closed with Completed status; the seven requested WorkflowService checks
(DescribeTaskList, ListTaskListPartitions, GetTaskListsByDomain,
RefreshWorkflowTasks, RequestCancelWorkflowExecution, TerminateWorkflowExecution,
ResetStickyTaskList) succeeded; and a real gRPC DescribeDomain call to `[::1]`
succeeded. Step JSON is retained in `memory/frontend-gate-extras-41427827989-steps.json`.
The failed intervening attempts used `rust_ref=fe-integ` (source head
`115f5b4b56969603648a828f01758ad1e73bdf70`) and do not test this branch. This
expands API-specific mixed evidence only; the `partial` service-wide verdict and
remaining parity limits above still apply.

## 2026-11-03 archival update
Filestore archival readers, domain archival state machine and production wiring landed on `frontend-archival` (see memory/evidence/frontend-archival-20261103.md, CI run 41548252149). Rows GetWorkflowExecutionHistory / ListArchivedWorkflowExecutions / Archival remain `partial`: unit-proven only, no endpoint or mixed-gate evidence yet.


## 2026-11-03 archival gate update
Branch `frontend-archival-gate`: ListArchivedWorkflowExecutions is `ported` for the filestore provider (see memory/evidence/frontend-archival-gate-20261103.md). GetWorkflowExecutionHistory archive fallback stays `partial` (Handler test only: Go archives history at retention expiry, not reachable in a gate run). Row 156 stays `partial`.
## 2026-11-03 Kafka/archival/ES update
Postgres and Cassandra gates with Kafka and archival steps pass on 49f3ec988 (runs 41573628343, 41573623671); see memory/evidence/frontend-kafka-archival-es-20261103.md. Cassandra domain-blob 0x59 preamble bug fixed.

## FINAL STATUS 2026-11-03 (branch frontend-final-audit) — supersedes the tables above where it differs
Rule: a row keeps the status in its original table row unless it is listed here. "gate" = asserted by a mixed-gate step (Rust Frontend+Matching vs Go History/Worker) in run 41588409434 (MySQL, kafka, two_history; 131/131 steps; memory/evidence/frontend-final-audit-41588409434-steps.json; binary sha256 959a17b3c5fbc50e48cdf0a2fff11c43fb2db0c279e4eebc4418d910a9fa9634). Postgres and Cassandra lanes passed earlier on 49f3ec988 (runs 41573628343, 41573623671, memory/evidence/frontend-kafka-archival-es-20261103.md); they were NOT re-run on the final head.

### Workflow API
| API | Final | Evidence |
|---|---|---|
| DescribeTaskList | ported | gate `sdk-describe-task-list` |
| ListTaskListPartitions | ported | gate `sdk-list-task-list-partitions` |
| GetTaskListsByDomain | ported | gate `sdk-get-task-lists-by-domain` |
| RefreshWorkflowTasks | ported | gate `sdk-refresh-workflow-tasks` |
| ResetStickyTaskList | ported | gate `sdk-reset-sticky-task-list` |
| RequestCancelWorkflowExecution | ported | gate `sdk-cancel-workflow-execution` |
| TerminateWorkflowExecution | ported | gate `sdk-terminate-workflow-execution`, `archival-terminate-*` |
| RespondActivityTaskCompleted | ported | gate `sdk-complete-echo` (activity executed by SDK worker) |
| GetWorkflowExecutionHistory | partial | live history: gate `sdk-complete-*`; archived-only fallback has Handler tests only — not gate-provable (Go archives at retention expiry, min 1 day; see memory/frontend-final-audit-20261103.md "Item 2") |
| StartWorkflowExecutionAsync / SignalWithStartWorkflowExecutionAsync | ported | gate `sdk-async-*` with Kafka (accept, execute, invalid, disabled domain, broker down/up, Frontend restart mid-traffic with no lost messages); TLS/SASL config parsing unit-tested (`async_kafka_tls.rs`, `async_kafka.rs`) |
| Frontend metrics (cross-cutting) | ported | gate `metrics-*` steps scrape Prometheus: `cadence_requests`, `cadence_latency`, `cadence_latency_ns` with `operation`+`domain`+`cadence_service="cadence_frontend"` |
| Remaining `partial` Workflow rows (DeleteDomain, DiagnoseWorkflowExecution, GetSearchAttributes, Record*Heartbeat*, Respond* other than Completed, RespondDecisionTaskFailed, Restart/Reset, SignalWithStart sync, FailoverDomain, ListFailoverHistory, UpdateSchedule, CreateSemaphore) | partial | handler/unit coverage only, no gate step |

### Admin API
| API | Final | Evidence |
|---|---|---|
| Get/UpdateDomainAsyncWorkflowConfiguraton | ported | gate `cli-admin-async-config-update-grpc` / `-get` |
| ResendReplicationTasks | ported (unit + two-cluster gate 41598950223) | `admin_replication.rs::resend_replication_tasks` + `resend_tests`, rust-ci 41587752247; remote must be gRPC, no remote TLS |
| GetDomainReplicationMessages | ported | re-checked line-by-line against Go handler.go:945-1001 (memory/frontend-final-audit-20261103.md) |
| GetCrossClusterTasks / RespondCrossClusterTasksCompleted | partial | validation + History proxy done; non-empty payload types NOT ported (return Internal error). Go cross-cluster queue is experimental/off by default |
| Other Admin `partial` rows (replication/DLQ proxies, raw history, queue/shard/host describe, Operational dynamic config, isolation groups, DeleteWorkflow, MaintainCorruptWorkflow, UpdateTaskListPartitionConfig, CloseShard, RemoveTask, ResetQueue, ReapplyEvents) | partial | implemented per earlier audit rows; no mixed-gate assertion on this head |

### Non-API
| Area | Final | Evidence |
|---|---|---|
| Metrics | ported | see above |
| Archival (visibility, filestore) | ported | gate `archival-*` steps; history archive read: partial (above) |
| Async queue | ported | gate `sdk-async-*`, `kafka-broker-*` |
| MySQL / Postgres / Cassandra persistence | ported for the exercised paths | MySQL on final head; Postgres/Cassandra earlier head (see above) |
| IPv6 bind | ported | gate `cli-describe-domain-grpc-ipv6` |
| Multi-History churn | ported | gate `go-history-2-killed` / `sdk-history-churn-*` |
| History shard routing, Ringpop multi-node, domain replication remote consumption | partial | see rows above; unchanged |


## Lane coverage update 2026-11-03
Full Frontend mixed gate (kafka, two_history, oauth, metrics, archival, dynconfig, multi-History churn) green on MySQL, Postgres (run 41622697487) and Cassandra (run 41622702539); ES visibility lane green on Postgres (41624742792); rust-ci green (41624747694). Details: memory/evidence/frontend-pg-cassandra-lanes-20261103.md.
