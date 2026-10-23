# Matching Go/Rust parity audit v4 (2026-10-23)

Base: `origin/master` @ `92668eb2f`. Read-only source audit. No cargo, no build, no gate was run for this document; no code was edited.
Go: `go/service/matching/**` (non-test) plus Matching-related `go/common/**`. Rust: `rust/crates/cadence-matching/src`, `rust/crates/cadence-service-core/src`, `rust/crates/cadence-persistence/src`, `rust/crates/cadence-common/src`.

Labels: `matches` (code read and a named test or gate exit backs it), `partial`, `different`, `missing`, `N/A` (interface, mock, or test helper). A row is never `matches` without a named test, a named gate exit file, or a stated code-only basis. Rows with code-only basis say so.

## 0. Method and limits

- Read in full: `memory/matching-go-parity-audit-v3-20261023.md`, `memory/evidence/matching-gate-run3-20261023/EVIDENCE.md`, `memory/evidence/matching-final-verify-20261023/*`, `memory/matching-go-test-inventory-port2-20261023.md` (gap section).
- Read in part: `memory/matching-metrics-matrix-20261023.md` (first 158 of 358 lines; the status counts in §4 come from a full awk pass over all rows).
- Skimmed verdict lines only: `matching-current-parity-audit-20261020.md`, `matching-config-property-audit-20261021.md`, `matching-dynamicconfig-audit-20261020.md`, `matching-membership-audit-20261023.md`, `matching-persistence-compat-audit-20261023.md`, `matching-cassandra-protocol-parity-20261021.md`, `matching-shard-distributor-audit-20261019.md`, `matching-go-parity-audit-v2-20261022.md`, `matching-go-test-inventory-20261023.md` (status counts only).
- Every Rust `file:line` below was opened or matched by grep in this worktree. Line numbers in v3 were partly stale (see §1.2), so v3 line references were not reused without re-checking.
- The Rust proto `TaskListKind` numbering could not be checked: `rust/crates/cadence-proto` is generated from a submodule that is not initialised in this worktree.

## 1. Evidence binding and corrections

### 1.1 Which evidence applies to which code

| Evidence | Exit file (exact value) | Code it covers | Applies to `92668eb2f`? |
|---|---|---|---|
| Final verify: workspace clippy `-D warnings` | `memory/evidence/matching-final-verify-20261023/clippy.exit` = 0 | head `caf262336` (`head.txt`) | **Yes.** `git diff caf262336 origin/master -- . ':!memory'` is empty; the only later commit is the evidence commit itself. |
| Final verify: workspace `cargo test` | `memory/evidence/matching-final-verify-20261023/test.exit` = 0 | same | **Yes**, same reason. Every `test result:` line in `test.log` is `ok`, with 0 failed. |
| Gate run 3: Cassandra lane | `memory/evidence/matching-gate-run3-20261023/exit-codes.txt` line `cass.exit 0` | binary sha `7dc23ec4…` (`binary-shas.txt`) | **Not bound.** The build commit is not recorded. Since the gate base `67f95cb78`, `git log 67f95cb78..caf262336` has 33 commits and 11 files under `rust/crates/cadence-matching/src` changed (+917/-25). |
| Gate run 3: Thrift/MySQL lane | `exit-codes.txt` `thrift.exit 0` | same | Not bound (as above) |
| Gate run 3: shard-distributor fixture / real | `exit-codes.txt` `sdfix.exit 0`, `sdreal.exit 0` | same | Not bound (as above) |
| MySQL mixed run 5 | `memory/evidence/matching-mysql-mixed-20261023/run5.exit` = 0 | `a34060ddf` (older) | Not bound |
| MySQL gate (frozen binaries) | `memory/evidence/matching-mixed-gates-20261023/mysql-gate.exit` = 0 | `~47774da67` | Not bound |
| PostgreSQL mixed run 2 | `memory/evidence/matching-pg-mixed-20261023/run2.exit` = 0 | `47774da67` | Not bound |
| Two-node MySQL, run b | `memory/evidence/matching-mixed-gates-20261023/mysql-2node-b.exit` = **1** | older | **Failing, never re-run** |
| Two-node MySQL, run c | `…/mysql-2node-c.exit` = **143** (killed) | older | **Failing, never re-run** |
| Two-node unit (shutdown fix) | `memory/evidence/matching-twonode-20261023/unit-test.exit` = 0 | unit only | Unit only |
| Query parity check/clippy/test | `memory/evidence/matching-query-parity-20261023/test.exit` = 0 | pre-split | Not bound |

**Consequence:** the only exit files that cover current master code are the two final-verify exits. Every runtime gate lane must be re-run on a recorded binary sha before any lane can be called evidence for `92668eb2f`.

### 1.2 Corrections to earlier audits

1. **`matching.errorInjectionRate` (v3 §6 "missing").** Go does not have this key. Go's key is `system.persistenceErrorInjectionRate` (`go/common/dynamicconfig/dynamicproperties/constants.go:5404`, read at `go/common/persistence/config.go:66`). Rust reads the correct key at `rust/crates/cadence-matching/src/launcher.rs:654`, and also reads a Rust-only alias `matching.errorInjectionRate` at `launcher.rs:651`. The alias is a behavior difference (see P1-9). The "missing" label is wrong.
2. **v3 line numbers have drifted.** `Meta::health` is now `grpc_matching.rs:3556-3557` (v3 said 3533-3536) and `thrift_matching.rs:723` (v3 said 697). `mark_started` is `launcher.rs:1397` (v3 said 1321). `describe_task_list` is at `grpc_matching.rs:3384`.
3. **"24 MISSING" (v3 §8).** The metrics matrix (`matching-metrics-matrix-20261023.md`) has 142 metric rows. 6 rows are explicitly labelled MISSING (`PersistenceErrBadRequestCounter` and its PerDomain variant, `PersistenceErrDBUnavailableCounter` and its PerDomain variant, `PersistenceResponseRowSize`, `PersistenceResponsePayloadSize`). 15 rows have no Rust name but are N/A by design (the matrix says they are unreachable from Matching). 9 rows are Go-catalog-only (no Go emission). The figure 24 does not match the matrix text.
4. **Backlog-forward tests (v3 §4, `final/test.exit` = 143).** Superseded: `matching-final-verify-20261023/test.exit` = 0 on the same code.
5. **`matching-config-property-audit-20261021.md`** lists `UserRPS`, `WorkerRPS`, `PersistenceMaxQPS`, `ThrottledLogRPS` as "Missing". Superseded: they are implemented in `cadence-service-core/src/matching_quota.rs`, `cadence-matching/src/matching_persistence_limiter.rs`, `cadence-matching/src/matching_throttled_logger.rs`.
6. **Test-port count.** The branch merge commit `ff6d4b9f1` says "51 matcher + 8 forwarder tests still missing". The port-2 inventory says `matcher_test.go`: 15 of 54 ported after pass 2. The port-2 figure is the later one and is used in this document.

## 2. Dynamic config: Go Matching keys vs Rust

Go defaults are from the default map in `go/common/dynamicconfig/dynamicproperties/constants.go`. Go reads are from `go/service/matching/config/config.go:190-255` unless stated. Rust reads are by key string. "Rust default" is the default value present at the read site.

| Key (KeyName) | Go default | Go read | Rust read (file:line) | Rust default | Status |
|---|---|---|---|---|---|
| `matching.rps` | 1200 | config.go:193 | `cadence-service-core/src/matching_quota.rs:143` | 1200 (`matching_quota.rs:12`) | matches (test: `user_and_worker_buckets_are_independent_and_refresh_live_values`) |
| `matching.workerrps` | `UnlimitedRPS` = MaxInt32 (constants.go:330) | :194 | `matching_quota.rs:144` | `i32::MAX` (`matching_quota.rs:13`) | matches |
| `matching.domainrps` | 0 | :195 | `matching_quota.rs:149` | 0 falls back to host (test: `zero_domain_rate_falls_back_to_host_and_count_only_calls_are_admitted`) | matches |
| `matching.domainworkerrps` | UnlimitedRPS | :196 | `matching_quota.rs:150` | host fallback (same test) | matches |
| `matching.persistenceMaxQPS` | 3000 | :190 | `cadence-matching/src/matching_persistence_limiter.rs:423-427` | 3000 (`HOST_QPS_DEFAULT`, :13) | matches (test: `zero_global_qps_falls_back_to_go_host_default`) |
| `matching.persistenceGlobalMaxQPS` | 0 | :191 | `matching_persistence_limiter.rs:430-434` | 0 (`GLOBAL_QPS_DEFAULT`, :14) | matches |
| `matching.throttledLogRPS` | 20 | :208 | `cadence-matching/src/matching_throttled_logger.rs:77,110` | 20 (test: `default_rate_starts_with_twenty_tokens_and_is_shared_by_both_polls`) | matches |
| `matching.enableSyncMatch` | true | :192 | none | — | matches (no consumer on either side; Go copies it to `tasklist.Config` only, `tasklist/task_list_manager.go:1059-1060`, and nothing reads it) |
| `matching.activityTaskSyncMatchWaitTime` | 50 ms | :219 | `cadence-matching/src/matching_config.rs:72` (helper, no caller) | 50 ms | matches (no consumer on either side; Go copies it at `task_list_manager.go:1040` only) |
| `matching.maxTasklistIdleTime` | 5 m | :202 | `matching_config.rs:55` (helper, no caller) | 5 m | matches (no consumer on either side; Go copies it at `task_list_manager.go:1050-1051`) |
| `matching.getTasksBatchSize` | 1000 | :199 | `grpc_matching.rs:593`; `matching_reader.rs:15` | 1000 | matches |
| `matching.readRangeSize` | 50000 | :198 | `grpc_matching.rs:597`; `matching_reader.rs:17` | 50000 | matches |
| `matching.updateAckInterval` | 1 m | :200 | `grpc_matching.rs:612, 662`; `matching_reader.rs:24` | 60 s | matches |
| `matching.idleTasklistCheckInterval` | 5 m | :201 | `grpc_matching.rs:670-683` | 300 s | matches (idle sweep: `matching_registry.rs:2126-2144`; gate lanes "idle eviction and recovery" passed, see §4) |
| `matching.longPollExpirationInterval` | 1 m | :203 | `grpc_matching.rs:1248` | 60 s (`matching_registry.rs:355`) | matches |
| `matching.minTaskThrottlingBurstSize` | 1 | :204 | `grpc_matching.rs:642-655` | 1 (`matching_limiter.rs:25`) | matches |
| `matching.maxTaskDeleteBatchSize` | 100 | :205 | `grpc_matching.rs:54-56` | 100 | matches |
| `matching.maxTaskBatchSize` | 100 | :207 | `grpc_matching.rs:73-77` | 100 | matches |
| `matching.outstandingTaskAppendsThreshold` | 250 | :206 | `grpc_matching.rs:87-92` | 250 | matches |
| `matching.numTasklistWritePartitions` | 1 | :209 | `grpc_matching.rs:409-416, 1176` | 1 | matches |
| `matching.numTasklistReadPartitions` | 1 | :210 | `grpc_matching.rs:427-431, 1168` | 1 | matches |
| `matching.forwarderMaxOutstandingPolls` | 1 | :211 | `grpc_matching.rs:521-525` | 1 | matches |
| `matching.forwarderMaxOutstandingTasks` | 1 | :212 | `grpc_matching.rs:535-539` | 1 | matches |
| `matching.forwarderMaxRatePerSecond` | 10 | :213 | `grpc_matching.rs:549-553` | 10 | matches |
| `matching.forwarderMaxChildrenPerNode` | 20 (Go clamps to ≥1) | :214 | `grpc_matching.rs:505-512` | 20, `.max(1)` | matches |
| `matching.enableGetNumberOfPartitionsFromCache` | false | :215 | `grpc_matching.rs:469-474, 725` | false | matches |
| `matching.shutdownDrainDuration` | 0 | :216 | `cadence-matching/src/matching_runtime.rs:669-676` | not re-read in this audit | partial (key read; default unverified) |
| `matching.enableTaskInfoLogByDomainID` | false | :218 | `matching_task_info_log.rs:37` | false (test: `info_requires_debug_mode_and_domain_id_flag_otherwise_dropped`) | matches |
| `system.enableDebugMode` | false | :217 | `launcher.rs:656` (read once at start) | false | matches (Go reads once at config build, `handler/engine.go:1593`) |
| `system.enableTasklistIsolation` | false | :220 | `grpc_matching.rs:784, 807, 1184` | false | matches |
| `system.enableDistributedSemaphore` | false | :221 | `grpc_matching.rs:2963` | false | matches (test: `matching_rpc_semaphore_acquire_grants_and_reports_no_slot`) |
| `matching.semaphoreIdleTime` | 5 m | :222 | `grpc_matching.rs:2967`; `matching_config.rs:77-78` | 5 m (`DEFAULT_IDLE_TIME_NANOS`, :19) | matches |
| `matching.appendTaskTimeout` | 5 s | :223 | `grpc_matching.rs:80-84` | 5 s | matches |
| `matching.asyncTaskDispatchTimeout` | 3 s | :224 | `grpc_matching.rs:601`; `matching_reader.rs:19` | 3 s | matches |
| `matching.localPollWaitTime` | 10 ms | :225 | `grpc_matching.rs:455-459` | 10 ms | matches |
| `matching.localTaskWaitTime` | 10 ms | :226 | `grpc_matching.rs:441-445` | 10 ms | matches |
| `matching.partitionUpscaleRPS` | 200 | :227 | `grpc_matching.rs:730` | 200 | matches |
| `matching.partitionDownscaleFactor` | 0.75 | :228 | `grpc_matching.rs:731-737` | 0.75 | matches |
| `matching.partitionUpscaleSustainedDuration` | 1 m | :229 | `grpc_matching.rs:738-741` | 60 s | matches |
| `matching.partitionDownscaleSustainedDuration` | 2 m | :230 | `grpc_matching.rs:742-745` | 120 s | matches |
| `matching.adaptiveScalerUpdateInterval` | 15 s | :231 | `grpc_matching.rs:726-729` | 15 s | matches |
| `matching.enableAdaptiveScaler` | false | :232 | `grpc_matching.rs:721` | false | matches |
| `matching.enablePartitionEmptyCheck` | false | :233 | `grpc_matching.rs:751` | false | matches |
| `matching.qpsTrackerInterval` | 10 s | :234 | `grpc_matching.rs:479-498`; `matching_registry.rs:271` | 10 s | matches |
| `matching.overrideTaskListRps` | 0 | :235 | `grpc_matching.rs:1080` | 0.0 | matches |
| `matching.enablePartitionIsolationGroupAssignment` | false | :236 | `grpc_matching.rs:747-750` | false | matches |
| `matching.isolationGroupUpscaleSustainedDuration` | 1 m | :237 | `grpc_matching.rs:752-755` | 60 s | matches |
| `matching.isolationGroupDownscaleSustainedDuration` | 2 m | :238 | `grpc_matching.rs:756-759` | 120 s | matches |
| `matching.isolationGroupHasPollersSustainedDuration` | 1 m | :239 | `grpc_matching.rs:760-763` | 60 s | matches |
| `matching.isolationGroupNoPollersSustainedDuration` | 1 m | :240 | `grpc_matching.rs:764-767` | 60 s | matches |
| `matching.isolationGroupsPerPartition` | 2 | :241 | `grpc_matching.rs:768` | 2 | matches |
| `matching.taskIsolationDuration` | 200 ms | :242 | `grpc_matching.rs:832` | 200 ms | matches |
| `matching.taskIsolationPollerWindow` | 2 s | :243 | `grpc_matching.rs:770-772, 830` | 2 s | matches |
| `system.allIsolationGroups` | list | :... (isolation) | `grpc_matching.rs:774, 790` | empty list | matches (read; default as Go's empty list) |
| `matching.enableStandbyTaskCompletion` | true | :250 | `grpc_matching.rs:875-879` | true | matches |
| `matching.enableClientAutoConfig` | false | :251 | `grpc_matching.rs:1220-1224` | false | matches |
| `matching.matchingReturnAllTaskListKinds` | false | :252 | `grpc_matching.rs:2782` | false | matches |
| `matching.excludeShortLivedTaskListsFromShardManager` | true | :253 | `launcher.rs` (`exclude_short_lived`, ~:1296) | true | matches |
| `matching.recordTaskStartedTimeout` | 1 s | :254 | `grpc_matching.rs:633`; `matching_history.rs:154` | 1 s | matches |
| `matching.taskListMinimumWritePartitions` | 1 | :255 | `grpc_matching.rs:746` | 1 | matches |
| `matching.percentageOnboardedToShardManager` | 0 (constants.go:4080) | not read in `go/service/matching` | `launcher.rs` (~:1287) | 0 | partial (Go consumer not located in `go/service/matching`) |
| `system.persistenceErrorInjectionRate` | 0 | `go/common/persistence/config.go:66` | `launcher.rs:654` | 0.0 | matches (fault-injection tests: `fake_error_selection_and_forwarding_follow_go`) |
| `matching.errorInjectionRate` | **no Go key** | — | `launcher.rs:651` | 0.0 | **different** (Rust-only alias; see P1-9) |

Summary: 63 rows (every key Go Matching reads or copies, plus the Rust-only alias). 60 matches, of which 3 (`enableSyncMatch`, `activityTaskSyncMatchWaitTime`, `maxTasklistIdleTime`) have no consumer on either side; 2 partial (`shutdownDrainDuration` default not re-read; `percentageOnboardedToShardManager` Go consumer not located in `go/service/matching`); 1 different (the Rust-only alias `matching.errorInjectionRate`).

## 3. Go file → Rust mapping

Go non-test files under `go/service/matching/`. Generated mocks and interface-only files are marked `N/A`.

| Go file or feature | Rust location (file:fn) | Status | Evidence (test or gate exit) | Notes |
|---|---|---|---|---|
| `service.go` NewService/Start/Stop (`:100-155`) | `cadence-matching/src/launcher.rs:479 run`; `matching_runtime.rs` drain (`drain_service`, :87-108; shutdown :538-560); `matching_membership_lifecycle.rs:142-151` (self-evict) | matches | tests `shutdown_drain_runs_membership_evictor_before_live_drain_read`, `drain_flag_flips_after_evict_and_sleep_and_evict_errors_are_ignored` (`matching_runtime.rs`); gate-run3 `cass.exit`=0 and `thrift.exit`=0 (SIGTERM drain with durable rows) | Binary not bound to head (§1.1) |
| `config/config.go` NewConfig (`:190-255`) | `matching_config.rs` (helpers); `grpc_matching.rs:50-900` (dynamic reads); `launcher.rs` (static) | partial | `matching_config.rs` tests `go_defaults_when_unset`, `configured_values_override_defaults`; §2 of this doc | Static config is a Go-shaped subset (v3 §7). `RangeSize`, `TaskDispatchRPS`, `TaskDispatchRPSTTL`, `MaxTimeBetweenTaskDeletes` (`config.go:197, 246-248`) not traced in Rust here (P2-5) |
| `event/logger.go` (`MATCHING_LOG_EVENTS`) | none | missing | grep: `MATCHING_LOG_EVENTS` appears only in `go/service/matching/event/logger.go:36` | Debug/simulation-only event log (P2-1) |
| `handler/context.go` newHandlerContext, handleErr, startProfiling | per-RPC context in `grpc_matching.rs`; error mapping `grpc_convert.rs` | partial | inventory `context_test.go:129` `TestHandleErrUncategorizedError` = MISSING | startProfiling not located (P2-9) |
| `handler/engine.go` NewEngine/Start/Stop/setupExecutor/shardDistributorOnboarded | `matching_runtime.rs`; `matching_shard_distributor.rs` (1547 lines); `launcher.rs:1282-1300` | partial | tests `pinned_executor_heartbeat_assignment_and_drain_contract`, `spectator_waits_for_initial_snapshot_and_returns_cached_owner`; `tests/shard_distributor_go_interop.rs`; gate `sdfix.exit`=0, `sdreal.exit`=0 | Gate binary not bound (§1.1) |
| `engine.go` getOrCreateTaskListManager / unloadTaskList | `matching_registry.rs` get_or_create (~1015-1081), unload | matches | tests `task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls`, `shutdown_tolerates_lost_task_list_ownership`; gate idle eviction + SIGKILL restart lanes | Gate not bound (§1.1) |
| `engine.go` AcquireSemaphore / getOrCreateSemaphoreManager / unloadSemaphoreManager | `grpc_matching.rs:2914 acquire_semaphore`; `matching_semaphore.rs:457-470 get_or_create_with_idle_ttl`, `Registry::unregister` | partial | `matching_rpc_tests.rs::matching_rpc_semaphore_acquire_grants_and_reports_no_slot`; `matching_rpc_sql_semaphore_matches_go_unsupported_backend_error`; `cadence-persistence/src/semaphores.rs` `sql_backend_matches_go_unsupported_error_for_metadata_and_token_operations`; gate Cassandra "semaphore owner/token rows" (EVIDENCE) | Grant retry/deadline/failure paths untested (P1-5) |
| `engine.go` AddDecisionTask (`:497`) | `grpc_matching.rs:2058`; `matching_dispatch.rs`; `matching_sync.rs` | matches | `matching_dispatch.rs::matching_remote_forward_reaches_go_yarpc_peer`; `matching_forward_decision_task_fields_match_go_forwarder` (port 2, passes); gate SDK lanes (`go-sdk.log` passed) | Gate not bound |
| `engine.go` AddActivityTask (`:595-670`) | `grpc_matching.rs:2273`; `matching_dispatch.rs:375-385` (forward) | **different** | Activity forward field test is `#[ignore]`d: `matching_dispatch.rs:2253-2255` | Forwarded activity sends list domain as source domain (P0-1) |
| `engine.go` PollForDecisionTask (`:671`) / PollForActivityTask (`:888`) | `grpc_matching.rs:1622, 1855`; `matching_dispatch.rs` | partial | `matching_dispatch_empty_long_poll_expires_without_delivery`; `matching_long_polls_reject_missing_transport_deadlines_with_bad_request`; inventory `TestPollForActivityTasksEmptyResultWithShortContext` = PARTIAL | Short-deadline empty poll only partly covered (P1-12) |
| `engine.go` QueryWorkflow / waitForQueryResult / deliverQueryResult (`:1014`) | `grpc_matching.rs:2482`; `matching_query.rs` (`:256`) | matches | `matching_query.rs` tests `cancelled_query_poll_removes_waiter_and_cannot_accept_an_offer`, `matching_query_forward_slow_down_only_for_service_busy`; gate "query dispatch passed" in Cassandra, Thrift and MySQL lanes (EVIDENCE) | Gate not bound |
| `engine.go` RespondQueryTaskCompleted (`:1092`) | `grpc_matching.rs:3269` | matches | as above (query dispatch lane) | Gate not bound |
| `engine.go` CancelOutstandingPoll (`:1109`) | `grpc_matching.rs:3212` (owner check, then cancel); `matching_registry.rs` | matches | gate "isolation-group dispatch with CancelOutstandingPoll passed" (EVIDENCE, all three lanes); `matching_dispatch.rs` cancelled-poll tests | Gate not bound |
| `engine.go` DescribeTaskList (`:1133`) | `grpc_matching.rs:3384` (pollers `:3432`) | matches | gate "native partition config passed" (Thrift lane reads partition config via gRPC, per EVIDENCE bug 6) | Thrift `DescribeTaskListResponse` has no `partitionConfig`; Go's Thrift mapper also drops it (EVIDENCE) |
| `engine.go` ListTaskListPartitions (`:1158`, `listTaskListPartitions`) | `grpc_matching.rs:2848` | partial | none against Go | Ring-order and cross-node response never compared to a Go node (P1-8) |
| `engine.go` GetTaskListsByDomain (`:1222`, `getTaskListsByDomainAndKind`) | `grpc_matching.rs:2733`; `matching_config`/`grpc_matching.rs:2782` (`matchingReturnAllTaskListKinds`) | partial | none located for the durable path in this audit | Legacy in-memory fallback still enumerates frontend state (P1-10) |
| `engine.go` UpdateTaskListPartitionConfig (`:1239`) | `grpc_matching.rs:2991`; kind check `:3056` (`matches!(kind, 0 \| 1)`) | **different** (probable) | `matching_partition.rs::matching_native_partition_updates_match_original_go_fixtures` (fixtures do not cover INVALID kind) | Go rejects kind ≠ Normal (`engine.go:1257-1259`); see P0-2 |
| `engine.go` RefreshTaskListPartitionConfig (`:1281`) | `grpc_matching.rs:3129`; kind check `:3182` | **different** (probable) | as above | Go `engine.go:1292-1293`; P0-2 |
| `engine.go` recordDecisionTaskStarted / recordActivityTaskStarted / refreshWorkflowTasks | `matching_history.rs:744` (RecordDecisionTaskStarted), `:579` (refresh) | matches | tests `history_metrics_record_one_outer_call_with_go_names_and_tags`, `history_metrics_record_typed_error_as_failure` (`matching_history.rs`); gate SDK lanes | Gate not bound |
| `engine.go` emitForwardedFromStats / emitTaskIsolationMetrics / emitInfoOrDebugLog | `matching_metrics.rs` (`ForwardedPerTaskList`, `IsolationTaskMatchPerTaskList` wired); `matching_task_info_log.rs` | partial | `info_requires_debug_mode_and_domain_id_flag_otherwise_dropped`; metrics matrix rows "emission/tag audit required" (92 rows) | No runtime metric comparison (P1-3) |
| `engine.go` getHostInfo / getAllPartitions | `matching_membership.rs`; `matching_registry.rs` | partial | no direct test located in this audit | Not individually traced (P2-9 area) |
| `engine.go` errIfShardOwnershipLost / errIfSemaphoreOwnershipLost (`:1600-1662` ring check) | `matching_membership.rs:216-232` (task list), `:259` (semaphore bucket) | matches | test `empty_ring_ownership_errors_wrap_lookup_failure_like_go_engine`; gate: "not owned by this host" ABORTED, matches Go's ring fallback (EVIDENCE) | Gate not bound |
| `handler/handler.go` (`:146-565`: RPC handlers, Start/Stop/Health `:106`) | `grpc_matching.rs:3556-3557 health` (waits for `mark_started`, `:916-919`); `launcher.rs:1397` | matches | gate startup in all lanes (EVIDENCE); Thrift health `thrift_matching.rs:723` | Health readiness gate; v3 line refs were stale |
| `handler/membership.go` runMembershipChangeLoop, shutDownNonOwnedTasklists | `grpc_matching.rs:1024-1050 maintain_membership_until`; `matching_registry.rs:807 unload_non_owned` | matches | `matching_registry.rs` tests calling `unload_non_owned` (`:2483, 2533, 2539, 2660`); gate Cassandra "owner SIGKILL takeover" (EVIDENCE) | Membership tests for panic and lookup failure MISSING (P2-2) |
| `handler/membership.go` shutDownNonOwnedSemaphoreManagers | `grpc_matching.rs:1044-1047` (`membership.owns` → `manager.stop()`, `unregister`) | matches | as above; semaphore-specific failure cases untested | Lookup failure path only logs (`eprintln!`) |
| `handler/interfaces.go`, `semaphore/interfaces.go`, `tasklist/interfaces.go` | trait/concrete types | N/A | — | Interface definitions only |
| `liveness/liveness.go` (NewLiveness/Start/Stop/eventLoop/IsAlive/MarkAlive) | `cadence-common` `Liveness` (used by `matching_semaphore.rs:16, 155`); task-list idle via `matching_registry.rs:2126 sweep_idle` | partial | `matching_semaphore_sweep_unloads_idle_buckets` (`matching_semaphore.rs:644`); gate "idle eviction and recovery" passed (task lists; EVIDENCE) | Rust has no event loop or shutdown callback; inventory `TestEventLoop_Noop`, `TestEventLoop_Shutdown` = MISSING (P1-6) |
| `poller/manager.go` (StartPoll/EndPoll/CancelPoll/HasPollerAfter/HasPollerFromIsolationGroupAfter/GetCount/GetCountByIsolationGroup/ListInfo) | `cadence-common/src/poller.rs:94 has_group_after`; `matching_registry.rs:517 has_poller_after`; `grpc_matching.rs:3432` (poller info); `matching_sync.rs:28` (Poller) | partial | `matching_reader_isolation_enabled_only_with_groups`; gate isolation-group dispatch (EVIDENCE) | `GetCountByIsolationGroup` and per-group `ListInfo` not located (P1-7) |
| `semaphore/semaphore_manager.go` (validateParams, Start/load, Acquire/enqueue/grant, reserve, recordHold, dropStaleHold, freeCount) | `matching_semaphore.rs` `Manager` (`acquire` :257, `grant` :279, reserve :359, load :208) | partial | 13 tests in `matching_semaphore.rs` (`matching_semaphore_grants_until_full_then_reports_no_slot`, `matching_semaphore_load_rebuilds_index_from_persisted_holds`); inventory: ~18 semaphore_manager_test rows MISSING | No `NewManager` validation error path (P2-3); grant retry, max-attempts, deadline, write-failure and confirm-failure paths untested (P1-5) |
| `semaphore/semaphore_registry.go` (GetOrCreate/Unregister/ManagerByIdentifier/AllManagers) | `matching_semaphore.rs` `Registry` (`get_or_create_with_idle_ttl` :461, `sweep_idle` :526, `unregister`) | matches | `matching_semaphore_registry_register_lookup_and_unregister`, `matching_semaphore_unregister_ignores_a_stale_manager`, `matching_semaphore_all_managers_is_a_snapshot`, `matching_semaphore_concurrent_callers_share_one_manager` (all PORTED, inventory) | Test-backed; not a gate |
| `semaphore/identifier.go` (NewIdentifier/ParseRequestOwner/validate/String/LogTags/RingKey) | `matching_semaphore.rs` identifier; `matching_semaphore_identifier_matches_go_validation` | partial | `matching_semaphore_ring_key_separates_buckets`, `matching_semaphore_identifier_is_usable_as_a_map_key` | `LogTags` (zap) has no Rust analog (P2-4) |
| `tasklist/adaptive_scaler.go` (Start/Stop/run/calculateWritePartitionCount/adjust*/collectPartitionMetrics) | `matching_scaler.rs` (`ScalerConfig` :53, `adjust_write_partitions` :123/:251, `adjust_read_partitions` :140, tick :560) | partial | `matching_scaler_sustain_holds_for_the_full_duration`, `matching_scaler_write_count_uses_sustained_thresholds`, `matching_scaler_read_drain_stops_at_first_live_partition` (7 tests); no gate lane enables `enableAdaptiveScaler` (EVIDENCE) | Go `adaptive_scaler_test.go` (2 funcs) not ported; metric semantics approximate (v2 A7, MIN) (P1-4) |
| `tasklist/isolation_balancer.go` (adjustWritePartitions, refreshGroups, calculatePartitionGoal, applyGroupChanges, movePartitions, findMaxWithGroup, findMinWithoutGroup, ensureMinimumGroupsPerPartition) | `matching_scaler.rs` isolation balancer: `partition_goal` :270, `apply_changes` :326, `find_max_with` :384, `find_min_without` :404, `remove_groups` :423, `add_groups` :442, `ensure_minimum_groups` :461, `move_groups` :492 | partial | `matching_scaler_new_group_spreads_across_partitions`, `matching_scaler_group_without_sustained_pollers_gets_no_partitions`, `matching_scaler_single_partition_with_groups_resets_assignments` | Port is present. Go `isolation_balancer_test.go` (1 func) not run against Rust; tie-break nondeterminism (v3 §9) (P1-4) |
| `tasklist/task_list_manager.go` (NewManager/Start/Stop/AddTask/DispatchTask/GetTask/Describe/partition config/gauges) | `matching_registry.rs` (`:1015-1081` owner lifecycle, partition apply, reader/writer wiring; 3155 lines) | partial | 26 tests in `matching_registry.rs` (`scaled_partition_from_response_reads_owner_describe_like_go`, `shutdown_tolerates_lost_task_list_ownership`); inventory `task_list_manager_test.go` (28 funcs) mostly PARTIAL or MISSING | Largest Go file in the subsystem; test coverage not 1:1 (P1-12) |
| `tasklist/matcher.go` (Offer/OfferOrTimeout/OfferQuery/MustOffer/Poll/PollForQuery/pollOrForward/isForwardingAllowed) | `matching_dispatch.rs` (2483 lines), `matching_sync.rs` (705), `matching_query.rs` (594), `matching_reader.rs` (forward) | partial | `matching_dispatch.rs` 25 tests; `matching_sync.rs` 14 tests (`sync_match_preserves_only_the_transient_forwarded_request_signal`); port 2: matcher_test.go 15 of 54 ported, all pass | Known difference P0-1 (forward source domain). 39 Go matcher test functions unported (P1-12) |
| `tasklist/forwarder.go` (ForwardTask/ForwardQueryTask/ForwardPoll, token channels) | `matching_forward.rs` (token generation, rate, classify, 5 tests); `matching_dispatch.rs:90 ForwardState`, `:684 forward_poll` | **different** | decision and poll-for field tests pass (port 2); activity field test `#[ignore]`d at `matching_dispatch.rs:2253-2255` | P0-1 |
| `tasklist/task_reader.go` (Start/Stop/getTasksPump/dispatchBufferedTasks/persistAckLevel) | `matching_reader.rs:316 TaskReader` (1930 lines) | matches | `reader_live_properties_match_go_reads`, `matching_reader_ack_timer_rereads_backlog_without_append_signal`, `matching_reader_reports_go_ack_backlog_gauge`, `matching_reader_counts_unknown_dispatch_errors_but_not_not_started` (23 tests) | Backlog forward-to-parent has unit tests but no runtime lane (P1-13) |
| `tasklist/task_writer.go` (appendTask/allocTaskIDBlock/renewLease/taskWriterLoop) | `matching_writer.rs:123 TaskWriter` (843); `matching_writer_actor.rs` (10 tests) | matches | `matching_writer_original_go_batches`, `matching_writer_rewrite_retry_uses_original_go_outcomes`, `zero_capacity_handoffs_are_fifo_and_never_overwrite_waiters` (10 tests); gate SIGTERM drain with durable task rows (1 before, 0 after recovery) in Thrift and Cassandra (EVIDENCE) | Gate not bound |
| `tasklist/task_completer.go` (CompleteTaskIfStarted, isTaskStarted, isRetryableError) | `matching_standby.rs:1` (Go taskCompleter port), `matching_reader.rs:596` | partial | `matching_standby_original_go_completion_decisions`, `standby_decision_uses_schedule_order_and_started_state`, `standby_activity_requires_started_or_absent_activity` (3 tests); inventory `task_completer_test.go` = 1 func | Standby query-poll forward end-to-end untested (v3 A2); completion-failure counter (A5) (P1-11) |
| `tasklist/task_gc.go` (Run/RunNow/tryDeleteNextBatch/checkPrecond) | `cadence-persistence/src/matching_gc.rs` (`TaskGarbageCollector::run`, `run_with_batch_provider`); wired by `matching_queue.rs:55, 194` | matches (code) | `matching_gc_original_go_fixtures`, `matching_gc_time_boundary_is_strict_and_failure_defers_small_retry`, `matching_queue_live_gc_batch_provider_is_used_for_each_attempt` | PostgreSQL GC/domain SQL checks "not collected" (v3 §5) (P1-14) |
| `tasklist/task.go` (internal task types, Info, Finish) | `matching_dispatch.rs:425 DeliveryTask`; `matching_sync.rs:15 SyncTask`; `cadence-persistence/src/matching_queue.rs:33 PendingTask` | matches | `matching_sync.rs` tests (14); `matching_writer.rs` tests | Equivalent representation (different types, same fields tested) |
| `tasklist/identifier.go` (newTaskListName, IsRoot, Partition, GetRoot, Parent, NewIdentifier, String) | `cadence-common/src/task_list.rs:5 TaskListName`; `grpc_matching.rs:839` (`partition()`) | partial | none located in this audit for `TaskListName` | `TestTaskListIDToString` MISSING in inventory (P2-4) |
| `tasklist/task_list_limiter.go` (Allow/Wait/Reserve/ReportLimit/tryUpdate) | `matching_limiter.rs` (387 lines) | matches | `report_divides_by_live_partition_count`, `report_limit_returns_go_rate_gauge_updates`, `wait_paces_after_terminal_burst_until_tokens_replenish`, `cancelled_wait_restores_reserved_token` (7 tests) | — |
| `tasklist/task_list_registry.go` (Register/Unregister/ManagerByTaskListIdentifier/AllManagers) | `matching_registry.rs` (registry, gauges at `:1015`, `:1581`) | matches | `task_list_manager_gauge_tracks_registry_lifecycle_not_get_calls`; inventory `task_list_registry_test.go` 3 of 3 PORTED or COVERED | — |
| `tasklist/shard_processor.go`, `shard_processor_factory.go` | `matching_shard_processor.rs:22 QueueRegistryShardProcessorFactory`, `:72` | matches | `report_has_strict_ttl_and_recomputes_qps_after_original_deadline`, `shard_processor_factory_rejects_empty_shard_key`, `shard_processor_initial_report_is_ready_with_zero_load` (5 tests); gate-run3 shard-distributor fixture/real (`sdfix.exit`=0, `sdreal.exit`=0) | Gate not bound |
| `tasklist/testing.go` | test helper | N/A | — | Not production code |
| `tasklist/interfaces_mock.go`, `semaphore/interfaces_mock.go`, `handler/interfaces_mock.go` | generated mocks | N/A | — | — |
| `tasklist/db.go` (newTaskListDB, RangeID, CreateTasks, GetTasks, CompleteTasksLessThan, GetTaskListSize, UpdateState, RenewLease, PartitionConfig) | `matching_persistence.rs` (`:175 task_list_size`, `:199 complete_tasks_less_than`, `:421 grant_semaphore_token`); `matching_queue.rs`; `matching_task_store.rs` (664) | matches (SQLite, MySQL, Cassandra); partial (PostgreSQL) | `matching_sqlite.rs` (9), `matching_mysql.rs` (21), `matching_cassandra.rs` (7), `matching_postgres.rs` (8), `matching_queue.rs` (22) tests; gate MySQL/Thrift `thrift.exit`=0, Cassandra `cass.exit`=0 (EVIDENCE); PostgreSQL `run2.exit`=0 (older binary) | MySQL/Cassandra runs not bound to head; PostgreSQL GC/domain checks not collected |
| `wrappers/grpc/grpc_handler.go`, `grpc_handler_generated.go` | `grpc.rs` (1879 lines), `grpc_matching.rs` (service impl) | matches | gate lanes use gRPC for History and Matching (EVIDENCE: `--history-transport grpc`, probes pass) | Gate not bound |
| `wrappers/thrift/thrift_handler.go`, `thrift_handler_generated.go` (10 IDL methods) | `thrift_matching.rs` (1157 lines; dispatch of the 10 IDL methods; `:723` health) | matches | `matching_thrift_exceptions_fill_required_fields_like_go_mapper`, `matching_thrift_uses_each_methods_exception_field_ids`, `matching_inbound_metrics_count_native_and_thrift_dispatch_once_each` (6 tests); gate `thrift.exit`=0 with `matchingTransport: thrift` (EVIDENCE) | Gate not bound |
| `handler/engine.go` (`matchingEngineImpl`) overall | spread across `grpc_matching.rs`, `matching_registry.rs`, `matching_dispatch.rs`, `matching_runtime.rs` | partial | — | Summary row; the per-function rows above carry the evidence |

### 3.1 Go `go/common` pieces used by Matching

| Go feature | Rust location | Status | Evidence | Notes |
|---|---|---|---|---|
| Persistence task store, metered wrappers (`go/common/persistence/wrappers/metered/base.go`) | `matching_persistence_limiter.rs` (9 tests), `matching_persistence.rs`, `matching_task_store.rs` | partial | matrix rows audited; `persistence_empty_response` test `empty_rows_and_host_latency_follow_go_metered_wrapper` | 6 MISSING metric rows (P1-3) |
| Persistence config `ErrorInjectionRate` (`config.go:66`) | `launcher.rs:654`; `matching_fault_injection.rs` (4 tests) | matches (code) | `persistence_errors_map_to_retry_classes`, `client_injection_skips_forwarding_for_busy_and_always_returns_fake`, `fake_error_selection_and_forwarding_follow_go` | Rust-only alias (P1-9) |
| Isolation groups (`go/common/isolationgroup`, `system.allIsolationGroups`, task isolation) | `cadence-common/src/task_isolation.rs` (used by `grpc_matching.rs:846`); `matching_reader.rs` isolation config | matches (code) | `matching_reader_isolation_enabled_only_with_groups`; gate "isolation-group dispatch … passed" (EVIDENCE) | Gate not bound |
| Rate limiting (`go/common/quotas`, `tokenbucket`) | `cadence-common` rate limiter (used by `matching_host_limiter.rs`, 7 tests; `matching_quota.rs`, 6 tests) | matches (code) | `global_burst_is_ceil_rps_then_denies`, `domain_token_is_returned_when_global_denies`, `host_and_domain_reservations_follow_go_order_and_return_domain_token_on_host_reject` | — |
| Membership, hashring, sharddistributor resolver (`go/common/membership/*`) | `matching_membership.rs` (5), `matching_shard_routing.rs` (10), `matching_history_membership.rs` (5), `ringpop_provider.rs` (40) | matches (code) | `go_farmhash_percentage_and_uuid_exclusion_vectors`, `excluded_and_missing_spectator_routes_use_live_ring_snapshot`; gate Cassandra "two-node ring, typed non-owner ABORTED" (EVIDENCE) | Gate not bound |
| Dynamic config providers (`go/common/dynamicconfig/dynamicconfigfx`, `configstore`, `openfeature*`) | `launcher.rs:561-572` — `bail!` for `ConfigStore` and `OpenFeature` providers; file provider only | **missing** (conditional) | none | P0-3 |
| Peer provider (`go/common/peerprovider`) | `matching_peer_provider.rs` (2 tests) | partial | `role_filter_and_named_ports_preserve_legacy_tchannel_fallback`, `malformed_present_tchannel_label_does_not_use_address_fallback` | Limited test coverage |
| Proto task-list kind mapping (`go/common/types/mapper/proto/api.go:4009-4046`) | `grpc_matching.rs:3056, 3182` (raw `kind` check) | partial | none (proto numbering unverified) | P0-2 |

## 4. Gate coverage summary (what runtime evidence exists)

| Lane (gate-run3, exit file) | Exit | Scenarios reported in EVIDENCE.md |
|---|---|---|
| Cassandra (`exit-codes.txt` `cass.exit`) | 0 | probe `passed`, `acceptanceEligible: true`; idle eviction + recovery; SIGTERM drain; SIGKILL restart; query; isolation dispatch with CancelOutstandingPoll; native partition config; Go SDK samples; two-node ring; typed non-owner ABORTED; owner SIGKILL takeover; semaphore rows; domain pagination |
| Thrift/MySQL (`thrift.exit`) | 0 | `preliminary-passed`; `matchingTransport: thrift`; same scenario set except two-node |
| Shard-distributor fixture (`sdfix.exit`) | 0 | `preliminary-passed`; heartbeat before/after SIGKILL and SIGTERM; reassignment; executor drain; SD restart |
| Shard-distributor real/etcd (`sdreal.exit`) | 0 | `preliminary-passed`; `executor-expired-by-ttl` after SIGKILL (6099 ms) |

All four lanes are `Not bound` to `92668eb2f` (§1.1). The two-node MySQL lanes remain failing (§1.1).

## 5. Prioritised gaps

Priority: **P0** = behavior differs or is missing. **P1** = no runtime or unit evidence, or evidence not bound to the head. **P2** = cosmetic, debug-only, or architecture difference.

### P0

- **P0-1. Forwarded activity: source domain is the list domain.**
  Go `go/service/matching/tasklist/forwarder.go:156-157` sends `DomainUUID = fwdr.taskListID.GetDomainID()` and `SourceDomainUUID = task.Event.DomainID` (the workflow's domain).
  Rust `rust/crates/cadence-matching/src/matching_dispatch.rs:378-380` sends the one `domain_id` (the list domain) as both fields.
  Root cause: `rust/crates/cadence-persistence/src/matching_blob.rs:8-15` (`TaskData`) has no source-domain field, so a backlog-forwarded activity row loses the workflow domain.
  Failure scenario: a workflow in domain A schedules an activity onto a tasklist in domain B whose partition forwards to its parent; the parent sees `SourceDomainUUID = B`, not `A`.
  Evidence: the Go-parity test is `#[ignore]`d at `matching_dispatch.rs:2253-2255`; the inventory marks it OPEN (port 2, PG-1). No runtime evidence.
  Fix direction: carry the source domain in `TaskData`; check the persistence format change against the Go blob codec first.

- **P0-2. Partition-config kind acceptance (probable).**
  Go `go/service/matching/handler/engine.go:1257-1259` (UpdateTaskListPartitionConfig) and `:1292-1293` (Refresh) reject any kind other than `TaskListKindNormal`. The Go mapper maps a nil or unknown kind to `TASK_LIST_KIND_INVALID` (`go/common/types/mapper/proto/api.go:4038-4048`).
  Rust `grpc_matching.rs:3056` (update) and `:3182` (refresh) use `matches!(task_list.kind, 0 | 1)`, so a raw kind of 0 is accepted as normal.
  If the proto enum has `INVALID = 0` (the Go mapper names suggest it does), Rust accepts an INVALID-kind request that Go rejects with `BadRequest`.
  Verification needed: read `TaskListKind` in the cadence-proto submodule once it is initialised. No test covers the INVALID kind.

- **P0-3. Dynamic config providers beyond file (conditional).**
  `rust/crates/cadence-matching/src/launcher.rs:561-572` bails for the `ConfigStore` and `OpenFeature` providers. Go supports them through `go/common/dynamicconfig/dynamicconfigfx`.
  This is a behavior difference only if a deployment configures a non-file provider. Confirm the target provider before ranking it higher.

### P1

- **P1-1. Two-node MySQL gate failing, never re-run.** `memory/evidence/matching-mixed-gates-20261023/mysql-2node-b.exit` = 1, `mysql-2node-c.exit` = 143. The Cassandra two-node run passed (gate-run3), but MySQL two-node is unresolved. Go reference: `handler/engine.go:1600-1662` (ring ownership).
- **P1-2. No gate is bound to the current head.** Gate-run3 binaries (`binary-shas.txt`) have no recorded build commit; 33 commits and 11 matching source files changed since its base. Re-run every lane (MySQL, PostgreSQL, Cassandra, Thrift, shard-distributor fixture and real) on a recorded binary sha from `cargo-target-final`, with one gate at a time.
- **P1-3. Metrics.** 6 matrix rows are MISSING: `PersistenceErrBadRequestCounter` and its PerDomain variant (`go/common/persistence/wrappers/metered/base.go:103, 141`), `PersistenceErrDBUnavailableCounter` and its PerDomain variant (`base.go:125, 163`), `PersistenceResponseRowSize` (`base.go:305`), `PersistenceResponsePayloadSize` (`base.go:321`). The remaining rows carry source-audit labels ("implemented name", "audited: emit site present", "fixed (this branch)"). None has an emission-and-tag comparison against a Go run, and there is no metrics test exit file. Rollup/migration config (matrix header) is not modelled.
- **P1-4. Isolation balancer and adaptive scaler test coverage.** Go `tasklist/isolation_balancer_test.go` (1 func) and `tasklist/adaptive_scaler_test.go` (2 funcs) are not run against Rust. Rust `matching_scaler.rs` has 7 tests that do not map one-to-one. No gate lane enables `enableAdaptiveScaler`. Go tie-break nondeterminism in `isolation_balancer.go` is unchanged.
- **P1-5. Semaphore manager failure paths untested.** Go `semaphore/semaphore_manager_test.go` rows MISSING in the inventory: grant retry on slot-taken (`TestGrantRetriesADifferentSlotWhenTheWriteSaysTaken`, :537), max attempts (:562), deadline stops retry (:577), already-held without token (:651), failed write returns slot (:679), unknown outcome (:699), confirm read failure (:772), acquire deadline during start (:389), start not blocking on load (:420), and failed grant surfacing (:496). Rust `matching_semaphore.rs:279 grant` has no test for these branches.
- **P1-6. Liveness mechanism differs.** Go `liveness/liveness.go` runs an event loop with a shutdown callback (`TestEventLoop_Noop`, `TestEventLoop_Shutdown` MISSING). Go task lists stop via `tasklist/task_list_manager.go:217-219`. Rust uses `matching_registry.rs:2126-2144` (sweep at TTL/2). Task-list idle eviction has gate evidence; the semaphore bucket idle path has one unit test only.
- **P1-7. Poller manager counts.** Go `poller/manager.go` `GetCountByIsolationGroup` and per-group `ListInfo` have no Rust counterpart located in this audit. Rust covers `has_group_after` (`cadence-common/src/poller.rs:94`) and `has_poller_after` (`matching_registry.rs:517`).
- **P1-8. ListTaskListPartitions ordering.** Go `handler/engine.go:1158` (`listTaskListPartitions`) vs Rust `grpc_matching.rs:2848`: ring order and cross-node responses were never compared with a Go node.
- **P1-9. Rust-only fault-injection key.** `rust/crates/cadence-matching/src/launcher.rs:651` reads `matching.errorInjectionRate`, which is not a Go key. Setting it injects errors in Rust but not in Go. Remove the alias or document it as Rust-only.
- **P1-10. GetTaskListsByDomain.** Go `handler/engine.go:1222` vs Rust `grpc_matching.rs:2733`: the durable path has no located test, and the legacy in-memory fallback still enumerates frontend state.
- **P1-11. Standby completion.** Go `tasklist/task_completer.go` vs Rust `matching_standby.rs`: the standby query-poll forward has no end-to-end test (v3 A2), and the completion-failure counter (v3 A5) is unverified.
- **P1-12. Matcher and manager unit-test coverage.** Go `tasklist/matcher_test.go` has 54 functions: 15 ported, 39 not (port-2 inventory). `tasklist/task_list_manager_test.go` has 28 functions, mostly PARTIAL or MISSING. The status-token counts in `matching-go-test-inventory-20261023.md` are 23 PORTED, 38 COVERED, 127 PARTIAL, 81 MISSING (raw token counts, not per function). Short-deadline empty poll is PARTIAL (`TestPollForActivityTasksEmptyResultWithShortContext`).
- **P1-13. Backlog forward, runtime.** `matching_reader.rs:299 BacklogForward` and `matching_dispatch.rs:289, 456` pass unit tests in final verify, but no gate lane runs backlog forward with more than one partition.
- **P1-14. PostgreSQL GC and domain checks.** The PostgreSQL gate (`matching-pg-mixed-20261023/run2.exit` = 0) predates the current head and did not collect GC or domain-metadata SQL checks (v3 §5).
- **P1-15. TLS.** gRPC TLS (`cadence-service-core/src/grpc_tls.rs`, 5 tests) and HTTP TLS (`launcher.rs`, 6 tests) are unit-only. TChannel TLS is not examined (v3 §7).
- **P1-16. Domain-failover callbacks.** Go `handler/engine.go:1707-1762` releases blocked polls; Rust releases polls only. The forward leg does not read `released` (v3 §3, §4).

### P2

- **P2-1.** `go/service/matching/event/logger.go:36` (`MATCHING_LOG_EVENTS`): simulation event log has no Rust equivalent. Debug-only.
- **P2-2.** `handler/membership_test.go` panic and lookup-failure cases (`:169, :423, :448`): Rust tasks do not use Go's panic recovery, so these are architecture differences.
- **P2-3.** `semaphore/semaphore_manager.go` `validateParams` / `NewManager` (`TestNewManagerValidatesItsParams`): no Rust validation error path.
- **P2-4.** `tasklist/identifier.go` `String()` (`TestTaskListIDToString`) and `semaphore/identifier.go` `LogTags` (zap): no Rust analog.
- **P2-5.** `config/config.go:197, 246-248` (`RangeSize` 100000, `TaskDispatchRPS` 100000, `TaskDispatchRPSTTL` 1 m, `MaxTimeBetweenTaskDeletes` 1 s): not traced in Rust in this audit.
- **P2-6.** Metrics rollup/migration (`go/common/metrics/defs.go` root-scope rollup): not modelled (matrix header).
- **P2-7.** Stale audits: `memory/matching-config-property-audit-20261021.md` (superseded, see §1.2 item 5).
- **P2-8.** v3 line references drifted (§1.2 item 2) and v3 "24 MISSING" does not match the matrix (§1.2 item 3).
- **P2-9.** `handler/context.go` `startProfiling` and `handler/engine.go` `getHostInfo` / `getAllPartitions`: not located in Rust in this audit.
- **P2-10.** The branch merge commit message count (`ff6d4b9f1`) and the port-2 count disagree (§1.2 item 6).

## 6. Conclusions

- Code-level parity is broad: the RPC surface, the partition/isolation/scaler logic, the registry and semaphore lifecycles, and the persistence backends all have Rust counterparts. Every key Go Matching reads has a Rust reader with the same default, except `matching.shutdownDrainDuration` (default not re-read) and `matching.percentageOnboardedToShardManager` (Go consumer not located); the Rust-only alias `matching.errorInjectionRate` is the one key-level difference (§2).
- Three P0 items remain: P0-1 (forwarded activity source domain, confirmed by an ignored test), P0-2 (partition-config kind acceptance, probable, pending proto numbering), and P0-3 (non-file dynamic-config providers, conditional).
- Runtime evidence exists for the final code only in the form of the two final-verify exits (clippy, test). Every gate lane is bound to an older, unrecorded binary, and the two-node MySQL gate is failing. A fresh gate run on a recorded binary is needed before any gate row here can be upgraded to "matches on head".
