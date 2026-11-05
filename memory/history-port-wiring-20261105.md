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

## Unit D2/D3 (branch history-assembly-d2)
CI green (clippy + test, package cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/41935841904
Added rust/crates/cadence-history/src/wiring/assembly/{mod,queues,replication,failover}.rs; fixed base-branch manifest/lib.rs duplicates (cadence-proto, tonic, `pub mod wiring`).
Real: failover coordinator + marker notifier + shard adapter (stale-marker rule), local single-host failover HistoryClient; replication ThriftPayloadSerializer, ReplicationShardAdapter (repl::ShardContext), LocalReplicationEngine (sync_shard_status); engine QueueProcessor adapter over queue::Processor.
Remaining stubs: UnwiredHistoryResender; replication Engine sync_activity/replicate_events_v2 need injected inner engine; no SQL MutableStateProvider; no replication builders for fetchers/processors/DLQ/task_ack_manager; build of TransferQueueProcessor/queuev2 (QueueShardContext and queuev2::ShardContext adapters, executors, visibility writer, archival client) not done; no timer processor in queue/; multi-host failover marker routing; domain failover watcher not started (frontend/worker).

### Unit D2b (branch history-assembly-d2b)
- Added `wiring/assembly/shard_adapters.rs`: `RealQueueShardContext` (real ShardContext -> queue::QueueShardContext) and `RealQueueV2ShardContext` (-> queuev2::ShardContext). Lossy shard fields (failover domain ids, last queue states) are cached adapter-side.
- Added `wiring/persistence/visibility.rs`: `SqlVisibilityWriter` (record started/closed, delete; upsert unsupported like Go SQL store).
- CI green: https://github.com/auenkr/cadence-rust/actions/runs/41938199109 head 009cec99a (package=cadence-history).
- Still remaining: SQL `MutableStateProvider`, replication builders (ack manager, hydrator, DLQ, fetchers/processors), real HistoryResender (`UnwiredHistoryResender` stays), real TransferQueueProcessor/queuev2 factory construction in `build_queue_processors`, timer queue processor (not ported in queue/), standby variants.
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
