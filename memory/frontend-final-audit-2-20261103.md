# Frontend final parity audit 2 (2026-11-03)

Branch `t3/frontend-final-audit-2` from master `cef54c866`. READ-ONLY audit: no Cargo/Go/Docker run, no Rust change. Evidence is read from source and from recorded run evidence in `memory/`; nothing here is newly executed.

## Verdict
Rust Frontend is **not yet a "change only the image name" drop-in**. Core SDK paths (start/signal/query/poll/respond/list/describe, schedules, async queue, archival visibility, ES visibility, OAuth, dynamic-config admin, IPv6, multi-History churn, two-cluster redirection) have mixed-gate evidence on MySQL, Postgres and Cassandra. The remaining blockers are (a) hard-coded validation limits and unbound dynamic config (41 of 79 Go frontend dynamic keys are never read), (b) a missing Thrift route for admin `ResendReplicationTasks`, (c) missing static-config sections, and (d) ~22 workflow and ~31 admin APIs with no mixed-gate assertion.

Counts: WorkflowService 57 APIs: **35 done, 22 partial, 0 missing**. AdminService 39 APIs: **8 done, 31 partial, 0 missing** (all have a handler; one lacks a Thrift route, see gap 2). Frontend dynamic keys (79 in `go/service/frontend/config/config.go` NewConfig): **29 wired, 8 validator-only (overlay not bound in production), 1 matching-only, 41 not read**.

## Verification of earlier audits (what I re-checked)
- `memory/frontend-parity-audit-20261102.md` FINAL STATUS and `frontend-final-audit-20261103.md` are consistent with the gate step lists: latest recorded step set is `memory/evidence/frontend-pg-lane-41622697487-steps.json` (155 steps, 0 failed, Postgres), `-cass-lane-41622702539-steps.json` (Cassandra), `-pg-es-lane-41624742792-steps.json`; MySQL baseline `frontend-final-audit-41588409434-steps.json` (131 steps).
- Newer than the audit tables: cross-cluster task payload types ported (`frontend-cross-cluster-tasks-20261103.md`, 38ddf4ce6, rust-ci 41622883106), gRPC remote forwarding for all redirected APIs (`frontend-grpc-remote-full-20261103.md`, two-cluster gate 41646432287), OAuth authorizer + gate (`frontend-oauth-authorizer-20261103.md`), ES lanes, dynconfig durability. The older audit's "Production policy hooks: only Nop authorizer/no metrics/no redirector" is now stale: `workflow_backend.rs` installs metrics sink, rate limit, access control (nop or OAuth), version check, redirector.
- Still wrong or stale in the older audit: it lists `CountDLQMessages` and `UpdateTaskListPartitionConfig` as Thrift-callable; Go's Thrift admin wrapper has neither (`go/service/frontend/wrappers/thrift/admin_generated.go`), they are gRPC-only. It lists 59 workflow rows; `api/interface.go` has 57 methods (Health..CreateSemaphore).
- NEW finding not in any earlier audit: production `Handler::new` uses `RequestValidator::default()` (`handler_workflow.rs:543`); `with_validator` and `RequestValidator::with_dynamic_config` have no non-test caller (`grep -rn with_validator rust/crates`), so `ValidationConfig` defaults apply and the dynamic overlay is never consulted by the validator.

## 1. API matrix

Gate step names come from `tests/gates/run-frontend-gate.py` / `tests/frontend-gate/main.go` and the recorded step JSON above. Thrift and gRPC listeners both route workflow APIs through one `WorkflowApi` (`rust/crates/cadence-service-core/src/workflow_api.rs`, `yarpc.rs`, `rust/crates/cadence-matching/src/grpc.rs`); `grpc_admin.rs` implements all 39 admin RPCs. A step proves the call over the transport the gate used (SDK = TChannel/Thrift for core calls; schedules, async-config, dyncfg via gRPC CLI); the other transport is not separately asserted except domain, list-workflows and OAuth steps (both).

### WorkflowService (Go `api/interface.go`)
| # | Go API | Status | Validation parity | Gate coverage | Rust pointer |
|---|---|---|---|---|---|
| 1 | Health | done | n/a | none (listening steps only) | `rust/crates/cadence-frontend/src/handler_workflow.rs` |
| 2 | CountWorkflowExecutions | done | request_validator.rs:266 | ES lane (memory/evidence/frontend-es-lanes-20261103.md); SQL rejects like Go | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 3 | DeleteDomain | partial | request_validator.rs:458 | none | `rust/crates/cadence-service-core/src/handlers.rs:delete_domain` |
| 4 | DeprecateDomain | partial | request_validator.rs:467 | none (unit) | `rust/crates/cadence-frontend/src/handler_domain.rs:deprecate_domain` |
| 5 | DescribeDomain | done | request_validator.rs:418 | cli-describe-domain-tchannel/-grpc, sdk-describe-domain, cli-describe-domain-grpc-ipv6 | `handler_domain.rs:describe_domain` |
| 6 | DescribeTaskList | done | request_validator.rs:228 | sdk-describe-task-list | `rust/crates/cadence-frontend/src/workflow_matching.rs:describe_task_list` |
| 7 | DescribeWorkflowExecution | done | execution check handler_workflow.rs:1800 | sdk-describe-open, sdk-describe-closed | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 8 | DiagnoseWorkflowExecution | partial | Go-order unit tests | none | `rust/crates/cadence-frontend/src/handler_workflow.rs:diagnose_workflow_execution` |
| 9 | GetClusterInfo | partial | n/a | none (unit) | `rust/crates/cadence-frontend/src/handler_workflow.rs` |
| 10 | GetSearchAttributes | partial | n/a | none (unit; admin add-search-attr gated: dyncfg-list-search-attr-after-restart) | `rust/crates/cadence-frontend/src/handler_workflow.rs` |
| 11 | GetWorkflowExecutionHistory | partial | handler_workflow.rs:1736 (token, run id, page size) | sdk-complete-*, sdk-query (live only); archived-only fallback unit-tested only | `rust/crates/cadence-frontend/src/workflow_matching.rs:get_workflow_execution_history` |
| 12 | ListArchivedWorkflowExecutions | done | request_validator.rs:304 | archival-list-archived-visibility, -paged, -domain-not-configured | `workflow_visibility.rs` |
| 13 | ListClosedWorkflowExecutions | done | request_validator.rs:327 | sdk-list-closed, cli-list-workflows-* | `workflow_visibility.rs:list_visibility` |
| 14 | ListDomains | partial | n/a | none | `handler_domain.rs:list_domains` |
| 15 | ListOpenWorkflowExecutions | done | request_validator.rs:286 | sdk-list-open | `workflow_visibility.rs:list_visibility` |
| 16 | ListTaskListPartitions | done | request_validator.rs:240 | sdk-list-task-list-partitions | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 17 | GetTaskListsByDomain | done | request_validator.rs:249 | sdk-get-task-lists-by-domain | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 18 | RefreshWorkflowTasks | done | request_validator.rs:220 | sdk-refresh-workflow-tasks | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 19 | ListWorkflowExecutions | done on ES; SQL rejects like Go | request_validator.rs:274 | ES lane steps (frontend-es-lanes-20261103.md) | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 20 | PollForActivityTask | done | long-poll deadline handler_workflow.rs:2461 | sdk-complete-echo (activity executed) | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 21 | PollForDecisionTask | done | same | sdk-complete-echo, sdk-query, metrics-requests-poll-decision | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 22 | QueryWorkflow | done | request_validator.rs query validate; disallowQuery NOT wired | sdk-query | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 23 | RecordActivityTaskHeartbeat | partial | envelope only | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 24 | RecordActivityTaskHeartbeatByID | partial | envelope only | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 25 | RegisterDomain | done | request_validator.rs:348 | cli-register-domain-tchannel/-grpc/-async/-noasync | `handler_domain.rs:register_domain` |
| 26 | RequestCancelWorkflowExecution | done | execution check | sdk-cancel-workflow-execution | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 27 | ResetStickyTaskList | done | request_validator.rs:257 | sdk-reset-sticky-task-list | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 28 | ResetWorkflowExecution | partial | envelope only | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 29 | RespondActivityTaskCanceled | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 30 | RespondActivityTaskCanceledByID | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 31 | RespondActivityTaskCompleted | done | envelope | sdk-complete-echo | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 32 | RespondActivityTaskCompletedByID | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 33 | RespondActivityTaskFailed | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 34 | RespondActivityTaskFailedByID | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 35 | RespondDecisionTaskCompleted | done | blob size policy NOT wired | sdk-complete-echo, sdk-query | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 36 | RespondDecisionTaskFailed | partial | envelope | none | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 37 | RespondQueryTaskCompleted | done | envelope | sdk-query | `rust/crates/cadence-frontend/src/workflow_matching.rs` |
| 38 | RestartWorkflowExecution | partial | unit tests | none | `rust/crates/cadence-frontend/src/handler_workflow.rs:restart_workflow_execution` |
| 39 | ScanWorkflowExecutions | done on ES; SQL rejects like Go | request_validator.rs:274 | ES lane steps | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 40 | SignalWithStartWorkflowExecution | partial | start validation limits wrong (see gaps) | none (SDK signal-with-start not asserted sync; async variant gated) | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 41 | SignalWithStartWorkflowExecutionAsync | done | same + signalName | sdk-async-signal-with-start-* | `handler_workflow_async.rs` |
| 42 | SignalWorkflowExecution | done | execution check; signalName/identity length limits NOT enforced | sdk-signal, sdk-signal-after-restart | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 43 | StartWorkflowExecution | done (limits differ) | cadence-api validation.rs:164 hardcodes 256 vs Go 1000 | sdk-start-echo | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 44 | StartWorkflowExecutionAsync | done | same | sdk-async-start-*, sdk-async-frontend-restart-no-lost-messages | `handler_workflow_async.rs` |
| 45 | TerminateWorkflowExecution | done | execution check | sdk-terminate-workflow-execution, archival-terminate-* | `rust/crates/cadence-frontend/src/workflow_backend.rs` |
| 46 | UpdateDomain | done | request_validator.rs:431 | archival-update-*, oauth-domain-update-groups | `handler_domain.rs:update_domain` |
| 47 | FailoverDomain | partial | request_validator.rs:476 | two-cluster gate failover steps use UpdateDomain, not FailoverDomain | `rust/crates/cadence-service-core/src/handlers.rs:failover_domain` |
| 48 | ListFailoverHistory | partial | n/a | none | `rust/crates/cadence-service-core/src/handlers.rs:list_failover_history` |
| 49 | CreateSchedule | done | handler_schedule.rs | sdk-sched-create-grpc | `rust/crates/cadence-service-core/src/handlers.rs:create_schedule` |
| 50 | DescribeSchedule | done |  | sdk-sched-describe-after-backfill-grpc | `rust/crates/cadence-service-core/src/handlers.rs` |
| 51 | UpdateSchedule | partial |  | none | `rust/crates/cadence-service-core/src/handlers.rs:update_schedule` |
| 52 | DeleteSchedule | done |  | sdk-sched-delete-grpc | `rust/crates/cadence-service-core/src/handlers.rs` |
| 53 | PauseSchedule | done |  | (sched-pause-grpc in older gate; not in latest step list) | `rust/crates/cadence-service-core/src/handlers.rs` |
| 54 | UnpauseSchedule | done |  | (sched-unpause-grpc older) | `rust/crates/cadence-service-core/src/handlers.rs` |
| 55 | BackfillSchedule | done |  | sdk-sched-backfill-grpc, sdk-sched-backfill-completed | `rust/crates/cadence-service-core/src/handlers.rs` |
| 56 | ListSchedules | done |  | (sched-list-grpc older) | `rust/crates/cadence-service-core/src/handlers.rs` |
| 57 | CreateSemaphore | partial | limit.semaphoreNameLength not read | none | `rust/crates/cadence-service-core/src/handlers.rs:create_semaphore` |

### AdminService (Go `admin/interface.go`)
| # | Go API | Status | Gate coverage | Rust pointer | Notes |
|---|---|---|---|---|---|
| 1 | AddSearchAttribute | done | dyncfg-add-search-attr, -persisted, dynconfig-restart-search-attr | `rust/crates/cadence-service-core/src/admin.rs:add_search_attribute_core` | ES mapping update unit-tested only; gate has no real ES mapping step |
| 2 | CloseShard | partial | none | `rust/crates/cadence-service-core/src/admin.rs:close_shard` | History proxy; no gate |
| 3 | DescribeCluster | partial | none (oauth admin-api steps hit admin auth only) | `rust/crates/cadence-service-core/src/admin.rs:describe_cluster_for` | membership payload not compared to Go |
| 4 | DescribeShardDistribution | partial | none | `rust/crates/cadence-service-core/src/admin.rs:describe_shard_distribution` | Go uses membership HistoryServerByShardID + paging; Rust local-host placement |
| 5 | DescribeHistoryHost | partial | none | `rust/crates/cadence-service-core/src/admin.rs:describe_history_host` |  |
| 6 | DescribeQueue | partial | none | `rust/crates/cadence-service-core/src/admin.rs:describe_queue` |  |
| 7 | DescribeWorkflowExecution | partial | none | `rust/crates/cadence-service-core/src/admin.rs:describe_workflow_execution_admin` |  |
| 8 | GetDLQReplicationMessages | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 9 | GetDomainReplicationMessages | done (unit, line-checked vs Go) | two-cluster gate (domain replicates A<-B) | `rust/crates/cadence-service-core/src/admin.rs:domain_replication_messages_response` |  |
| 10 | GetReplicationMessages | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 11 | GetWorkflowExecutionRawHistoryV2 | partial | used indirectly by ResendReplicationTasks in two-cluster gate | `rust/crates/cadence-service-core/src/admin.rs:get_workflow_execution_raw_history` |  |
| 12 | CountDLQMessages | partial (gRPC only; absent from Go Thrift admin wrapper) | none | `rust/crates/cadence-service-core/src/admin_replication.rs:291` |  |
| 13 | MergeDLQMessages | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 14 | PurgeDLQMessages | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 15 | ReadDLQMessages | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 16 | ReapplyEvents | partial | none | `rust/crates/cadence-service-core/src/admin.rs:reapply_events` |  |
| 17 | RefreshWorkflowTasks | partial | none | `rust/crates/cadence-service-core/src/admin.rs:refresh_workflow_tasks` |  |
| 18 | RemoveTask | partial | none | `rust/crates/cadence-service-core/src/admin.rs:remove_task` |  |
| 19 | ResendReplicationTasks | partial | two-cluster gate (gRPC remote) run 41598950223 | `rust/crates/cadence-service-core/src/admin_replication.rs:70` | NO Thrift/YARPC dispatch arm in yarpc.rs (only MethodDef at cadence-thrift/src/service.rs:1069 and REST route handlers.rs:1471) -> "unknown method" over TChannel/Thrift |
| 20 | ResetQueue | partial | none | `rust/crates/cadence-service-core/src/admin.rs:reset_queue` |  |
| 21 | GetCrossClusterTasks | partial (payload types ported, 38ddf4ce6) | none (History returns empty in gate) | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 22 | RespondCrossClusterTasksCompleted | partial | none | `rust/crates/cadence-service-core/src/admin_replication.rs` |  |
| 23 | GetDynamicConfig | done | dyncfg-get, dyncfg-get-after-restart | `rust/crates/cadence-service-core/src/admin.rs` |  |
| 24 | UpdateDynamicConfig | done | dyncfg-update, dyncfg-update-again, dyncfg-db-row-written, dyncfg-seeded-go-row | `rust/crates/cadence-service-core/src/admin.rs` |  |
| 25 | RestoreDynamicConfig | done | dyncfg-restore, dyncfg-get-after-restore | `rust/crates/cadence-service-core/src/admin.rs` |  |
| 26 | ListDynamicConfig | done | dyncfg-list, dyncfg-list-after-restore | `rust/crates/cadence-service-core/src/admin.rs` |  |
| 27 | GetOperationalDynamicConfig | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | in-memory only, no operational store |
| 28 | UpdateOperationalDynamicConfig | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | in-memory only |
| 29 | RestoreOperationalDynamicConfig | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | in-memory only |
| 30 | ListOperationalDynamicConfig | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | in-memory only |
| 31 | DeleteWorkflow | partial | none | `rust/crates/cadence-service-core/src/admin.rs:delete_workflow` |  |
| 32 | MaintainCorruptWorkflow | partial | none | `rust/crates/cadence-service-core/src/admin.rs:maintain_corrupt_workflow` |  |
| 33 | GetGlobalIsolationGroups | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | local state |
| 34 | UpdateGlobalIsolationGroups | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | local state |
| 35 | GetDomainIsolationGroups | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | local state |
| 36 | UpdateDomainIsolationGroups | partial | none | `rust/crates/cadence-service-core/src/admin.rs` | local state; oauth-domain-update-groups is UpdateDomain |
| 37 | GetDomainAsyncWorkflowConfiguraton | done | cli-admin-async-config-get | `rust/crates/cadence-service-core/src/admin.rs` | Go IDL misspelling preserved |
| 38 | UpdateDomainAsyncWorkflowConfiguraton | done | cli-admin-async-config-update-grpc | `rust/crates/cadence-service-core/src/admin.rs` |  |
| 39 | UpdateTaskListPartitionConfig | partial | none | `rust/crates/cadence-matching/src/grpc_admin.rs:553` | gRPC only, forwards to Matching |
Admin transport note: gRPC admin (`rust/crates/cadence-matching/src/grpc_admin.rs`) implements all 39 RPCs (verified by fn names). Thrift admin dispatch lives in `cadence-service-core/src/yarpc.rs:2640-2830`; every Go-Thrift admin method has an arm except `ResendReplicationTasks` (no match in the file).

## 2. Configuration

### Static YAML (`go/common/config/config.go` -> Rust `cadence-service-core/src/config.rs`, `cadence-frontend/src/config.rs`)
| Go key | Rust | Status |
|---|---|---|
| `persistence` (+ numHistoryShards, advancedVisibilityStore, elasticsearch) | `PersistenceConfig` | read |
| `log`, `services.frontend.rpc` (port, grpcPort, bindOnLocalHost, bindOnIP, grpcMaxMsgSize, tls) | `ServiceConfig`/`RpcConfig` config.rs:995-1018 | read; IPv6 gated (`cli-describe-domain-grpc-ipv6`) |
| `services.frontend.rpc.http` | not found | MISSING (Go HTTP inbound) |
| `services.frontend.metrics` (m3/statsd/prometheus), `histograms`, `gauge-migration`, `counter-migration` | `MetricsConfig`, `matching_metrics_*` | read; gated `metrics-*` |
| `services.frontend.pprof` | `PProfConfig` | parsed (launcher wiring seen for Matching only) |
| `clusterGroupMetadata`, `clusterMetadata`, `dcRedirectionPolicy` | `ClusterMetadata` | read; redirection gated by two-cluster gate |
| `dynamicconfig`, `dynamicConfigClient` (filebased, configstore, openfeature stanzas) | `DynamicConfigConfig` | filebased + configstore gated (`dynconfig-restart-*`); openfeature parsed, runtime rejects non-filebased/configstore per config.rs comment |
| `authorization` (oauthAuthorizer, noopAuthorizer) | `AuthorizationConfig` | read; gated `oauth-*` (JWKS fetched once like Go) |
| `asyncWorkflowQueues` (kafka) | `AsyncWorkflowQueueProviderConfig` | read; gated `sdk-async-*`, TLS/SASL unit-only |
| `archival`, `domainDefaults` | raw YAML | read; filestore gated `archival-*`; s3/gcloud NOT validated |
| `ringpop` | `RingpopConfig` | read (launcher); multi-node churn not gated for Frontend ring |
| `shardDistributorClient`, `shard-distributor-matching` | parsed | n/a for Frontend |
| `kafka` (top-level) | no field | MISSING (only needed for legacy replication/visibility producers) |
| `publicClient` | no field | MISSING (Go frontend uses it for the worker client / hostPort) |
| `blobstore` | no field | MISSING (legacy filestore) |
| `headerForwardingRules` | no field | MISSING (Go forwards configured headers on outbound calls) |
| Pinot visibility store | none | MISSING (explicitly out of scope per earlier audit) |

### Dynamic config (79 Go keys from `NewConfig`; Rust names in `cadence-frontend/src/config.rs:62-146` match)
The registry in `config.rs` only lists key names; "read" below means a non-test Rust read site exists outside `config.rs` and the admin key table. `validator-only` = read only inside `RequestValidator`, which production never binds to the overlay (see Verification), so the Go default is not even approximated. Counts: 29 wired, 8 validator-only, 1 matching-only, 41 not read.

| Key | Go default | Rust read? | Rust files |
|---|---|---|---|
| `history.adminOperationToken` | "CadenceTeamONLY" | wired | handler_admin.rs, request_validator.rs |
| `limit.blobSize.error` | 2 * 1024 * 1024 | not read | - |
| `limit.blobSize.warn` | 256 * 1024 | not read | - |
| `frontend.disableListVisibilityByFilter` | false | not read | - |
| `system.disallowQuery` | false | not read | - |
| `frontend.domainFailoverRefreshInterval` | time.Second * 10 | not read | - |
| `frontend.domainFailoverRefreshTimerJitterCoefficient` | 0.1 | not read | - |
| `limit.domainNameLength` | 1000 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `frontend.enableActiveClusterSelectionPolicyInStartWorkflow` | false | not read | - |
| `history.enableAdminProtection` | false | wired | handler_admin.rs, request_validator.rs |
| `frontend.enableClientVersionCheck` | false | wired | workflow_backend.rs |
| `system.enableDistributedSemaphore` | false | wired | e2e.rs, grpc_matching.rs, matching_engine_go_tests_d.rs, matching_launch_native.rs, matching_rpc_tests.rs |
| `system.enableDomainAuditLogging` | false | not read | - |
| `system.enableDomainNotActiveAutoForwarding` | true | wired | workflow_backend.rs |
| `system.enableGracefulFailover` | true | not read | - |
| `system.enableLogCustomerQueryParameter` | false | not read | - |
| `frontend.enableQueryAttributeValidation` | true | wired | workflow_backend.rs |
| `system.enableReadFromClosedExecutionV2` | false | wired | launcher.rs |
| `worker.enableScheduler` | false | wired | e2e.rs, handler_admin.rs, handlers.rs |
| `system.enableTasklistIsolation` | false | matching-only | grpc_matching.rs, matching_concurrent_go_parity.rs, matching_delivery.rs, matching_rpc_tests.rs, matching_runtime_native.rs |
| `system.enableVisibilitySampling` | false | not read | - |
| `frontend.asyncrps` | 10000 | wired | workflow_backend.rs |
| `frontend.decisionResultCountLimit` | 0 | not read | - |
| `frontend.esIndexMaxResultWindow` | 10000 | wired | request_validator.rs, workflow_backend.rs |
| `frontend.esVisibilityListMaxQPS` | 30 | not read | - |
| `frontend.emitSignalNameMetricsTag` | false | not read | - |
| `frontend.failoverCoolDown` | time.Minute | wired | e2e.rs, handlers.rs |
| `frontend.failoverHistoryMaxSize` | 5 | wired | handlers.rs |
| `frontend.globalDomainAsyncrps` | 100000 | not read | - |
| `frontend.globalDomainrps` | UnlimitedRPS | not read | - |
| `frontend.globalDomainVisibilityrps` | UnlimitedRPS | not read | - |
| `frontend.globalDomainWorkerrps` | UnlimitedRPS | not read | - |
| `frontend.globalRatelimiterMode` | "disabled" | not read | - |
| `frontend.globalTaskListAsyncrps` | 100000 | not read | - |
| `frontend.globalTaskListrps` | UnlimitedRPS | not read | - |
| `frontend.globalTaskListWorkerrps` | UnlimitedRPS | not read | - |
| `frontend.historyMaxPageSize` | 1000 | not read | - |
| `frontend.maxBadBinaries` | 10 | wired | handlers.rs |
| `frontend.domainasyncrps` | 10000 | wired | workflow_backend.rs |
| `frontend.domainrps` | 1200 | wired | workflow_backend.rs |
| `frontend.domainvisibilityrps` | UnlimitedRPS | wired | workflow_backend.rs |
| `frontend.domainworkerrps` | UnlimitedRPS | wired | workflow_backend.rs |
| `frontend.maxFailoverTimeoutInSeconds` | 300 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `frontend.tasklistasyncrps` | 10000 | not read | - |
| `frontend.tasklistrps` | 1200 | not read | - |
| `frontend.tasklistworkerrps` | UnlimitedRPS | not read | - |
| `frontend.maxWorkerPollDelay` | 0 | wired | workflow_backend.rs |
| `frontend.persistenceGlobalMaxQPS` | 0 | not read | - |
| `frontend.persistenceMaxQPS` | 2000 | not read | - |
| `frontend.shutdownDrainDuration` | 0 | wired | launcher.rs |
| `frontend.throttledLogRPS` | 20 | not read | - |
| `frontend.rps` | 1200 | wired | workflow_backend.rs |
| `frontend.visibilityListMaxQPS` | 10 | not read | - |
| `frontend.visibilityMaxPageSize` | 1000 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `frontend.visibilityrps` | UnlimitedRPS | wired | workflow_backend.rs |
| `frontend.warmupDuration` | 30 * time.Second | wired | launcher.rs |
| `frontend.workerrps` | UnlimitedRPS | wired | workflow_backend.rs |
| `frontend.globalRatelimiterUpdateInterval` | 3 * time.Second | not read | - |
| `limit.identityLength` | 1000 | not read | - |
| `system.Lockdown` | false | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `limit.maxIDWarnLength` | 128 | not read | - |
| `system.maxRetentionDays` | 30 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `system.minRetentionDays` | 1 | wired | handlers.rs |
| `frontend.pinotOptimizedQueryColumns` | map[string]interface{ | not read | - |
| `system.rateLimiterBypassCallerTypes` | []interface{ | not read | - |
| `system.readVisibilityStoreName` | "es" | wired | request_validator.rs, workflow_backend.rs |
| `limit.requestIDLength` | 1000 | not read | - |
| `system.requiredDomainDataKeys` | nil | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `frontend.searchAttributesNumberOfKeysLimit` | 100 | wired | workflow_backend.rs |
| `frontend.searchAttributesSizeOfValueLimit` | 2048 | wired | workflow_backend.rs |
| `frontend.searchAttributesTotalSizeLimit` | 40 * 1024 | wired | workflow_backend.rs |
| `limit.semaphoreNameLength` | 1000 | not read | - |
| `frontend.sendRawWorkflowHistory` | false | not read | - |
| `limit.signalNameLength` | 1000 | not read | - |
| `limit.taskListNameLength` | 1000 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `frontend.validSearchAttributes` | definition.GetDefaultIndexedKeys() | wired | handler_admin.rs |
| `frontend.visibilityArchivalQueryMaxPageSize` | 10000 | validator-only (overlay not bound in prod Handler) | request_validator.rs |
| `limit.workflowIDLength` | 1000 | not read | - |
| `limit.workflowTypeLength` | 1000 | not read | - |
Defaults compared: wired RPS keys use Go values (`workflow_backend.rs` policy(): rps 1200/domainrps 1200, async 10000, unlimited others). Divergent hard-coded fallbacks (all in code paths that run in production):
| Place | Rust | Go |
|---|---|---|
| `cadence-api/src/validation.rs:162` domain/workflowID/workflowType/taskList/requestID max length | 256 | 1000 (`limit.*Length`, dynamic per domain) |
| `request_validator.rs:37-47` max_retention_days | 3650 | 30 (`system.maxRetentionDays`) |
| same, max_failover_timeout_seconds | 60 | 300 |
| same, max_domain_name_length / max_task_list_name_length | 255 | 1000 |
| same, visibility_archival_query_max_page_size | 1000 | 10000 |
| same, `dynamic_string_domain("system.readVisibilityStoreName", .., "db")` at :146 | "db" | "es" (backend path uses "es" correctly) |

## 3. Wrappers (`rust/crates/cadence-frontend/src/wrappers/`)
| Go wrapper | Rust | Parity notes |
|---|---|---|
| accesscontrolled | `accesscontrolled.rs`, `oauth_authorizer.rs`, `SelectedAuthorizer` in `workflow_backend.rs:1274` | Nop unless oauth enabled; gated for both transports and both services (`oauth-*` incl. admin-api). Authorization-provider outbound middleware not ported (grpc-remote-full doc). |
| metered | `metered.rs`, `metrics_sink.rs` | `cadence_requests`/`cadence_latency`/`cadence_latency_ns` with operation+domain tags gated (`metrics-*`). Emitted metric NAMES for errors/warn counters (blob size, id length warnings, `EmitSignalNameMetricsTag`, per-API sub-metrics) are not compared with Go. Blob size policy (`limit.blobSize.*`) not installed in production. |
| ratelimited | `ratelimited.rs`, `quota_policy.rs` | Per-domain (user/worker/visibility/async) policies wired. NOT wired: per-tasklist limits (`frontend.tasklistrps`, `...workerrps`, `...asyncrps`), global ratelimiter (`frontend.global*`, `globalRatelimiterMode`), `system.rateLimiterBypassCallerTypes` (installed `NoBypass`). No gate step triggers a throttle (`ServiceBusy`) at all. |
| versioncheck | `versioncheck.rs` | Reads `frontend.enableClientVersionCheck`; exempt-ops list ported; no gate (unit only). |
| clusterredirection | `clusterredirection.rs`, `redirection_runtime.rs`, `frontend_remote*.rs` | noop/selected-apis/all-domain-apis policies, Thrift + gRPC remotes, active-active attribute resolution gated by two-cluster gate 41646432287. Not done: active-active per-run policy lookup (needs execution-table reader), strong-consistency header, schedule APIs routed through resolver, remote worker ops only unit-tested. |
| thrift / grpc wrappers | `wrappers/thrift.rs`, `wrappers/grpc.rs`, `cadence-thrift`, `cadence-proto` | Field/error mapping covered by unit tests and SDK traffic; no exhaustive per-API populated request/response/error wire comparison against Go. Known latent bug: HistoryEvent thrift ToThrift ms*1e6 vs FromThrift raw ns (`cadence-proto convert.rs ~6136`, flagged in cross-cluster doc, unfixed). |

## 4. Remaining gaps that break a drop-in swap (ordered by user impact)
1. **Hard-coded validation limits (silent wrong rejections).** `cadence-api/src/validation.rs:162` caps domain/workflowID/workflowType/taskList/requestID at 256; Go allows 1000 per domain (dynamic). A workflow ID or task-list name of 257-1000 chars works on Go and fails on Rust (`StartWorkflowExecution`, `SignalWithStart*`, task-list checks 255 at `request_validator.rs:37`). Fix: build `Limits` from the overlay (`workflow_backend.rs` has overlay at ~934) and pass to `validate_start_workflow_execution_request` (`handler_workflow.rs:1695-1698`); also enforce Go's SignalName/Identity/RequestID length (`limit.signalNameLength`, `identityLength`, `requestIDLength`), `validate/errors.go:66-72`, plus warn limit `limit.maxIDWarnLength`.
2. **`RequestValidator` never bound to the dynamic overlay.** `handler_workflow.rs:543` default validator; add `Handler::with_validator(RequestValidator::default().with_dynamic_config(overlay))` in `build_workflow_api` (`workflow_backend.rs` ~line 1037) and make `ValidationConfig::default()` equal Go defaults (table above). Affects `frontend.visibilityMaxPageSize`, `visibilityArchivalQueryMaxPageSize`, `system.Lockdown`, `taskListNameLength`, retention/failover limits, `requiredDomainDataKeys`.
3. **Thrift admin `ResendReplicationTasks` unrouted.** No arm in `yarpc.rs` (compare `"ResetQueue"` arm ~2689); Go CLI/tools using TChannel get `unknown method`. Fix: add arm calling `admin::resend_replication_tasks` (REST route exists, `handlers.rs:1471`; `cadence-thrift/src/service.rs:1069`). Add a Thrift gate step to `run-frontend-two-cluster-gate.py`.
4. **41 of 79 frontend dynamic keys ignored.** Operators who tune them (e.g. `frontend.historyMaxPageSize`, `system.disallowQuery`, `frontend.sendRawWorkflowHistory`, `frontend.decisionResultCountLimit`, `limit.blobSize.error/warn`, `system.enableVisibilitySampling`, `frontend.persistenceMaxQPS`, `frontend.visibilityListMaxQPS`, `system.enableTasklistIsolation` for frontend poll routing, `frontend.disableListVisibilityByFilter`, `frontend.enableActiveClusterSelectionPolicyInStartWorkflow`) see no effect; blob-size limit is the most dangerous (Go rejects oversized payloads at 2MB error / 256KB warn; Rust accepts and History/persistence rejects later). Fix: read via `cadence_persistence::dynamic_config::*` helper like `workflow_backend.rs:880-985`; `WorkflowBlobSizePolicy` seam exists (`handler_workflow.rs:442`, `with_blob_size_policy`) but is uninstalled.
5. **Per-tasklist and global rate limits missing.** `frontend.tasklistrps` (default 1200) and global ratelimiter keys unwired; a hot task list is not throttled as in Go. Fix: add task-list-scoped policy in `wrappers/quota_policy.rs`; `ratelimited.rs:142-172` already extracts the task-list name.
6. **22 workflow and 31 admin APIs have no mixed-gate assertion.** Highest-risk gaps for SDK users: RecordActivityTaskHeartbeat(+ByID), Respond*Failed/Canceled(+ByID), RespondDecisionTaskFailed, ResetWorkflowExecution, SignalWithStartWorkflowExecution (sync), RestartWorkflowExecution, DiagnoseWorkflowExecution, ListDomains/DeprecateDomain/DeleteDomain/FailoverDomain/ListFailoverHistory, UpdateSchedule. Fix: add steps to `tests/frontend-gate/main.go` (Go SDK client can call all via `client.Client`/`workflowserviceclient`).
7. **Admin parity not gate-proven.** DescribeCluster membership, DescribeShardDistribution (local placement vs Go membership + PageSize), shard/queue/DLQ/replication admin, raw history, DeleteWorkflow, MaintainCorruptWorkflow, isolation groups: History-proxied or local-state only. Operational dynamic config is in-memory only (no operational store, no audit log); `GetDLQReplicationMessages`/DLQ ops lack populated-queue tests. Fix: gate steps using the Go CLI `cadence admin ...` against the mixed cluster.
8. **Static config gaps.** `publicClient`, `headerForwardingRules`, `kafka`, `blobstore`, `services.frontend.rpc.http` not parsed; operators with these in YAML get silently ignored behaviour (Go would honor them). Fix: add fields in `cadence-service-core/src/config.rs` and either implement or fail loudly at startup.
9. **Archived-only `GetWorkflowExecutionHistory` and s3/gcloud archival unverified.** Filestore only; fallback unit-tested (Go archives only at retention expiry, not reproducible in a gate). Fix: pre-seeded archive gate is of limited value; at least unit-test against real Go-written archive file produced by a Go archiver in `archival-go-visibility-wrote-archive` style for history.
10. **Redirection and metric-name residuals.** Active-active per-run policy, strong-consistency forwarding header, schedule APIs not routed through resolver; metered error counter names/EmitSignalNameMetricsTag/warn counters not compared; HistoryEvent thrift timestamp asymmetry (`cadence-proto convert.rs ~6136`) unfixed.

Also unverified (not gaps proven): Ringpop multi-node Frontend churn, graceful shutdown under load/in-flight long polls, TLS on inbound/outbound listeners, Kafka TLS/SASL against a real broker, domain replication consumption on a full two-cluster failover with History, Postgres/Cassandra lanes on final head (earlier head runs 41622697487 / 41622702539 are the latest; ES visibility lane 41624742792 Postgres only).
