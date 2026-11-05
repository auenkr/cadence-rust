# History wiring (branch history-port-wiring, started 2026-11-05)

Goal: runnable History service == go/service/history (service.go, handler/, cmd/server history wiring). No stubs on startup path.
Base: master 6bcc45d8d. CI: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-history` (no local cargo).

## Units (sub-branches `history-wiring-<unit>` off history-port-wiring, new files under rust/crates/cadence-history/src/wiring/<unit>/, merged by orchestrator)
- A persistence: adapters implementing ShardStore/ShardPersistence/execution/history/task-store/visibility traits of every module (engine, task, queue, queuev2, replication, ndc, events, workflowcache, taskdlq) on cadence-persistence SQL stores.
- B resources: domain cache, cluster metadata, membership resolver, matching/history/frontend clients, metrics, dynamic config, time source, archival/visibility, replication fetcher clients.
- C server: Thrift TChannel + gRPC History server over handler, health, graceful shutdown.
- D assembly (after A,B): shard controller, shard context, engine factory, queue processors, replication, failover, main.rs startup.

## Stubs replaced
Unit B (branch history-wiring-resources, rust/crates/cadence-history/src/wiring/resources/):
- clock.rs `SystemClock`: engine::TimeSource, task::TimeSource, replication::TimeSource, failover::TimeSource, queuev2::TimeSource (+ `TokioTimer`, `TokioTimerGate` for queuev2 Timer/TimerGate).
- cluster.rs `ClusterMetadataImpl` (cadence_common::cluster::Metadata): engine::ClusterMetadata, task::ClusterMetadata.
- domain_cache.rs `HistoryDomainCache` (wraps cadence-matching MatchingDomainCache; caller builds/starts it with a persistence source): engine/task/replication/failover DomainCache.
- metrics.rs `MetricsRegistry` + `Scope` + `MetricsClientImpl` + `DomainMetricsScopeCacheImpl`: task::MetricsScope/MetricsClient/DomainMetricsScopeCache, queue::MetricsScope, queuev2::MetricsScope/MetricsClient/Stopwatch, ReplicationMetrics, FailoverMetrics, EngineMetrics (real counters; replaces NoopScope/NoopMetrics once assembly uses them).
- dynconfig.rs `FileDynamicClient`: config_collection::DynamicClient from YAML file (constraint matching, reload).
- membership.rs `MembershipResolver`: Ringpop-compatible ring (cadence_common::membership::Ring) lookup/who_am_i/members/owns.
- client_checker.rs `VersionChecker`: engine::ClientChecker (sticky query).

Unit B part 2 (branch history-wiring-resources2, same dir):
- config_adapters.rs `ConfigAdapter(Arc<Config>)`: task::HistoryConfig, engine::EngineConfig, failover::FailoverConfig, replication::ReplicationConfig.
- domain_source.rs `open_domain_metadata_source`: DomainMetadataSource for SQLite/MySQL/Postgres/Cassandra via cadence-matching `open_configured_default_store` (blocking; call in spawn_blocking).
- failover_domain_manager.rs `PersistenceDomainManager`: failover::DomainManager (Go CleanPendingActiveState, retry 50ms x2, 2 attempts).
- membership_feed.rs: Ringpop events/snapshot -> `MembershipResolver::update_hosts`.
- persistent_dynconfig.rs `PersistentDynamicClient`: DynamicClient over cadence-persistence `ConfigStoreClient` (+ poller).
- replication_clients.rs `GrpcAdminClient`, `GrpcFrontendClient` (SignalWithStart data-corruption scan), `ReplicationClientBean` (remote gRPC channels from cluster metadata; HistoryClient injected).
- query_matching.rs `GrpcQueryMatchingClient`: engine::QueryMatchingClient over Matching gRPC, channel resolver injected.

## Unit B gaps (not done, need follow-up)
- Replication `HistoryClient` (SyncActivity / ReplicateEventsV2 / etc.): no proto conversions in cadence-proto; bean takes an injected impl.
- Visibility WRITE client: cadence-persistence only has a visibility reader; no writer to wrap. Archival client: only primitives in cadence-common (archiver, cloud_archiver, archival_state); no history-facing client written.
- Channel resolver for GrpcQueryMatchingClient (task list -> matching host via MembershipResolver) must be supplied by assembly; tchannel remotes unsupported in replication bean.
- None of the new impls is exercised by tests yet (compile + clippy only).

## Stubs remaining
Per-module local trait universes (engine/deps.rs, task/deps.rs, queue/deps.rs, queuev2/deps.rs, replication/deps.rs, ...), NoopScope/NoopMetrics/NoopObserver, InMemoryShardStore, InMemoryClient, main.rs delegating to cadence_matching::launcher.

## CI runs
- 41924739463 head e0aa7a3fe (history-wiring-resources, unit B part 1): see earlier result.
- 41926280725 (resources2 batch 1): failure, clippy while_let in membership_feed.
- 41927175770: failure, missing tonic/cadence-proto deps.
- 41927893949: failure, clippy result_large_err.
- 41928648196: success (cadence-history clippy+test).
- 41929681628 head 5c2cae3fd (adds query_matching.rs): success. https://github.com/auenkr/cadence-rust/actions/runs/41929681628

## Unit D1 (branch history-assembly-d1): assembly env, shard factory, engine factory, registry
Code: rust/crates/cadence-history/src/wiring/assembly/{mod,env,shard_factory,engine_factory,controller}.rs (+ `pub mod assembly;` in wiring/mod.rs).
Also: cadence-matching `ConfiguredMatchingStores` gained `history_sql_backend` (SqlBackend) and `history_shard_store` (MySQL/Postgres only, None elsewhere); engine `HistoryNotification` gained domain_id/workflow_id/run_id; removed duplicate Cargo deps (cadence-proto, tonic) and the duplicate `pub mod wiring;` in lib.rs (base branch had them).
Replaced with real impls:
- env.rs `HistoryEnv::build(HistoryEnvParams)`: persistence opened via `open_configured_default_store` (SqlWorkflowStore / SqlHistoryV2Store / HistoryNodeStore / SqlShardStore / SqlShardPersistence), started MatchingDomainCache, SystemClock, ClusterMetadataImpl, ConfigAdapter, metrics, MembershipResolver, VersionChecker, GrpcQueryMatchingClient. Traits `QueueFactory`, `ReplicationFactory`, `ReplicationParts`, `ShardBuildCtx` (engine_slot: Weak engine set after build) for D2/D3.
- shard_factory.rs `ShardFactory::new_shard_context` (create row if missing, read, `ShardContext::acquire` range steal/fencing), `ShardClockAdapter` (engine::ShardClock), `DomainActiveClusterManager` (engine + task ActiveClusterManager; non active-active only), `TaskShardContext` (task::deps::ShardContext; CrossCluster maps to Transfer), `ReinjectorSlot` (D2 must `bind` the real TaskReinjector; errors until then).
- engine_factory.rs `build_engine(env, shard, queue_processors, ReplicationParts)`: all 24 HistoryEngine fields; real: domain cache, cluster metadata, clock, metrics, config, client checker, both matching clients (same gRPC client, no retry wrapper), SqlHistoryReader, SqlCurrentExecutionReader, event notifier (NotifierAdapter over events::Notifier), shard clock, active cluster manager.
- controller.rs `ShardRegistry`: lazy OnceCell per shard, ownership via `MembershipResolver::lookup(shard_id)` (pure `check_ownership`, ShardOwnershipLost message carries owner address; the error type has only `message`, so the thrift `owner` field is not set), `engine_for_workflow`, `engine_for_shard`, `close_shard`, `on_membership_change`, `close_all`. Does not call `ShardController::acquire` (would double-bump range).
Remaining stubs (explicit `Unwired*` returning internal errors, in engine_factory.rs): WorkflowUpdater, DecisionHandler, ExecutionCache, WorkflowResetter, MutableStateTaskRefresher. Cause: no concrete impl of the engine-local MutableState/ExecutionContext traits (engine/deps_ms.rs) exists; crate mutable_state.rs is a separate universe. Other gaps: SqlHistoryTaskDLQManager is explicit-error on SQL; queue/queuev2 shard adapters (D2); no integration test (only 4 unit tests for ownership).
CI: https://github.com/auenkr/cadence-rust/actions/runs/41936357375 success (cadence-history clippy+test, 80 tests) head 236c4535e0348ce844c9e6267cc1f7202a98698c.

## Unit D5 (branch history-assembly-d5, from history-assembly-d1)

Replaced (real, in `rust/crates/cadence-history/src/wiring/assembly/`):
- `engine_state.rs`: `EngineMutableState` implements the engine `MutableState` over a persisted `WorkflowSnapshot` (Go `Load`): execution info, activities, child executions, signal-requested ids, version histories (thriftrw blob), current version/branch token, sticky TTL check, `QueryRegistryAdapter` (query::Registry -> engine QueryRegistry), in-memory mutators (delete_activity, signal-requested add/delete, clear_stickyness, set_workflow_state, heartbeat progress) with a dirty flag.
- `engine_cache.rs`: `EngineExecutionContext` (load via `GoExecutionStore::get_workflow_execution`), `EngineExecutionCache` (key domain/workflow/run, per-workflow tokio lock held until `ReleaseFunc`, clear-on-error / force-clear for current-run lock, `validateWorkflowExecutionInfo` incl. current-run resolution and UUID check, `GetAndCreate` hit/miss semantics), `EngineWorkflowUpdater` (`workflow.Load`, `UpdateWithActionFunc`, `UpdateCurrentWithActionFunc`, `updateHelper` with stale-state / conflict retry x5).
- `engine_factory.rs`: updater and execution_cache stubs replaced.

Remaining stubs / explicit errors (not silent):
- `UnwiredDecisionHandler`, `UnwiredWorkflowResetter`, `UnwiredTaskRefresher` in `engine_factory.rs`.
- `EngineMutableState`: every history-event adding method (AddXxxEvent family, RetryActivity, TerminateWorkflow, GetStartEvent, GetActivityScheduledEvent, GetResurrectedActivities, CopyToPersistence) returns an internal "not ported" error.
- `EngineExecutionContext::update_as_active` persists nothing: no-op when state is unmodified, error when dirty; `update_workflow_execution_tasks` errors. `AddDecisionTaskScheduledEvent` in `updateHelper` errors when a decision must be created.
- Needs next: history builder + task generator over EngineMutableState, close-transaction producing `WorkflowMutation` with shard task ids.
- `auto_reset_points`/`active_cluster_selection_policy` of the engine `WorkflowExecutionInfo` are not decoded (None).

CI evidence: see rust-ci run https://github.com/auenkr/cadence-rust/actions/runs/41938883186 (package=cadence-history) on head efb16e6a7, conclusion success (code identical to later memory-only head)

## Unit D6 (branch history-assembly-d6, from history-assembly-d5)
Replaced:
- `EngineMutableState` event methods now emit history events (buffering, flush, transient activity started, activity retry) and transfer/timer tasks (`engine_events.rs`, `engine_tasks.rs`).
- Close transaction -> `WorkflowMutation` (`engine_close.rs`); `engine_cache.rs` persists it: history append, task id allocation, `update_workflow_execution` (UpdateCurrent / IgnoreCurrent). `update_as_active`, `update_workflow_execution_tasks`, `update_helper` (schedules decision) now work.
- `EngineDecisionHandler` (`engine_decision.rs`) replaces `UnwiredDecisionHandler`: scheduled, started (incl. transient attempt>0), failed.
Remaining stubs/gaps:
- `handle_decision_task_completed` (decision command processor) returns explicit internal error.
- `UnwiredWorkflowResetter`, `UnwiredTaskRefresher` still stubs.
- Replication tasks, new-run writes (continue-as-new/create paths), user timers, checksum, buffered reorder/dedup, active-domain check, GetStartEvent on first decision.
CI: see run URL and head SHA appended below once green.
CI green: https://github.com/auenkr/cadence-rust/actions/runs/41944524657 (package=cadence-history, clippy + tests), head d6510a424e0d75f602abe68018433b7de548fa8a (code identical to the following memory-only commit).
