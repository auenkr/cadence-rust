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

## Unit B gaps (not done, need follow-up)
QueryMatchingClient (needs cadence_api<->proto conversion + gRPC MatchingApiClient, see cadence-frontend/src/matching_client.rs), replication AdminClient/FrontendClient/HistoryClient/ClientBean, HistoryConfig/EngineConfig/ReplicationConfig/FailoverConfig adapters over config::Config, DomainManager (failover), archival/visibility clients, persistence-backed DomainMetadataSource for MySQL/Postgres/Cassandra (cache only has SQLite source), feeding MembershipResolver from Ringpop events.

## Stubs remaining
Per-module local trait universes (engine/deps.rs, task/deps.rs, queue/deps.rs, queuev2/deps.rs, replication/deps.rs, ...), NoopScope/NoopMetrics/NoopObserver, InMemoryShardStore, InMemoryClient, main.rs delegating to cadence_matching::launcher.

## CI runs
(none yet)
