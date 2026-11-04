# History wiring (branch history-port-wiring, started 2026-11-05)

Goal: runnable History service == go/service/history (service.go, handler/, cmd/server history wiring). No stubs on startup path.
Base: master 6bcc45d8d. CI: `gh workflow run rust-ci.yml --ref <branch> -f package=cadence-history` (no local cargo).

## Units (sub-branches `history-wiring-<unit>` off history-port-wiring, new files under rust/crates/cadence-history/src/wiring/<unit>/, merged by orchestrator)
- A persistence: adapters implementing ShardStore/ShardPersistence/execution/history/task-store/visibility traits of every module (engine, task, queue, queuev2, replication, ndc, events, workflowcache, taskdlq) on cadence-persistence SQL stores.
- B resources: domain cache, cluster metadata, membership resolver, matching/history/frontend clients, metrics, dynamic config, time source, archival/visibility, replication fetcher clients.
- C server: Thrift TChannel + gRPC History server over handler, health, graceful shutdown.
- D assembly (after A,B): shard controller, shard context, engine factory, queue processors, replication, failover, main.rs startup.

## Stubs replaced
- Unit C (server, branch history-wiring-server-c): rust/crates/cadence-history/src/wiring/server/{mod,tchannel,grpc,limiter,errors}.rs. `HistoryServer::start(ServerConfig, Arc<dyn Handler>) -> RunningServer` (+`shutdown()`: prepare_to_stop, stop listeners, handler.stop). Thrift TChannel (as=thrift, service `cadence-history`) serves Meta::health + CloseShard/RemoveTask/ResetQueue/DescribeQueue; gRPC HistoryApi serves the same 4 admin RPCs. Host rate limit (token bucket) and error mapping (HistoryError -> thrift exception / grpc Status) in place.
  REMAINING for unit C: History-internal request/response Thrift codecs (cadence-thrift has no HistoryService method table nor FromThrift/ToThrift for engine::types) and proto<->engine::types converters for the other ~38 RPCs; those answer TChannel bad-request / gRPC UNIMPLEMENTED today. Also DescribeHistoryHost conversion, gRPC health service, TLS, per-domain/workflow-id quotas.

## Stubs remaining
Per-module local trait universes (engine/deps.rs, task/deps.rs, queue/deps.rs, queuev2/deps.rs, replication/deps.rs, ...), NoopScope/NoopMetrics/NoopObserver, InMemoryShardStore, InMemoryClient, main.rs delegating to cadence_matching::launcher.

## CI runs
(none yet)
