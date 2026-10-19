# Matching shard-distributor client contract (2026-10-19)

Scope: typed server configuration and the standalone Rust outbound executor/spectator assignment owner. This document records behavior from the exact `shard-manager` revision pinned in `go/go.mod` (`v0.0.0-20260610143419-4bef35311802`, commit `4bef35311802`). It does not claim Matching registry or `main.rs` integration.

## Pinned sources

- `service/sharddistributor/client/clientcommon/config.go`: `Config` has `namespaces` and `peer_ttl`; namespace fields are `namespace`, `heartbeat_interval`, `ttl_shard`, `ttl_report`. Validation requires at least one namespace, nonempty unique names and positive heartbeat intervals. `ttl_report` is consumed by the Cadence processor factory; it is not sent in Heartbeat. `ttl_shard` drives client-side idle `DONE` reporting. Peer TTL defaults to two minutes.
- `service/sharddistributor/client/executorclient/client.go`: executor construction validates config, selects exactly one namespace, creates a fresh UUID executor ID, carries metadata and a live `Enabled` callback, and applies a per-call client timeout. Its processor contract is `Start(ctx)`, `Stop()`, `GetShardReport()`, and `SetShardStatus(status)`.
- `service/sharddistributor/client/executorclient/clientimpl.go`: executor behavior detailed below.
- `service/sharddistributor/client/spectatorclient/{clientimpl,stream,peer_chooser}.go`: spectator watches namespace state, waits for its initial state before lookup, checks its owner cache and falls back to `GetShardOwner` on cache miss. Its stream is forcibly reconnected every 10 minutes; retry/disabled polling is 1 second with 10% jitter. Disabling it resets the first-state signal and waits before reconnecting. `peer_ttl` belongs to `peer_chooser.go`'s idle peer cache (default two minutes), not to the spectator stream itself. Resolver-level onboarding/fallback policy remains separate.
- `go/service/matching/handler/engine.go`: no-namespace config installs Noop executor and a processor factory with default `ttl_report=1m`; configured namespace installs executor, factory and metadata. Executor onboarding is `percentageOnboarded > 0` and is reevaluated on each heartbeat tick.
- `go/common/config/config.go`, `go/config/development.yaml`: exact YAML keys are `shardDistributorClient.hostPort` and `shard-distributor-matching`.
- Existing wire types are in `rust/proto/shard-manager/uber/cadence/sharddistributor/v1/{executor,service}.proto`; Rust tonic generation already includes those services and messages.

## Executor state and lifecycle contract

- `Start` runs heartbeat and cleanup loops. Heartbeat cadence is configured interval with 10% jitter. When disabled, it skips heartbeats but checks enabled again on every tick.
- A heartbeat reports only processors in `started` state. Each report is the processor's own status and load, and request metadata is a copied snapshot. Request identity includes namespace and fresh executor UUID.
- Assignment responses are authoritative: processors missing from response or not `READY` are stopped; `READY` assignments create processors unless one is already tracked. Processor creation/start failures remove the entry so a later heartbeat retries. Start/stop operations have 10-second timeout watchers; these log/measure timeouts but do not abort the actual Go operation.
- `GetShardProcess` records last use, returns an existing processor immediately, or when enabled does a synchronous heartbeat/assignment pass and checks again. Disabled or unassigned returns typed `ErrShardProcessNotFound`; heartbeat failure remains an operational error.
- Idle cleanup is disabled for `ttl_shard <= 0`. Otherwise it uses a TTL interval with 10% jitter; when `last_use + ttl_shard < now` and executor enabled, it sets the processor report status to `DONE` and forgets last-use state. It does not stop the processor.
- Stop/context cancellation synchronously stops all processors, sends one `DRAINING` heartbeat with a five-second RPC deadline when enabled, and joins the two loops. A drain signal similarly stops processors and reports DRAINING, waits for undrain or stop, then resumes heartbeat. A fresh observer channel is obtained after undrain.
- Metadata is mutable thread-safely and heartbeat takes a copy. Cadence Matching supplies `tchannel`, `grpc`, and `hostIP` metadata.

## Spectator and RPC policy boundary

- Spectator's watched-state cache is namespace-specific and first-state gated. Lookup cache miss calls `GetShardOwner`; metadata is part of the returned owner. Rust config should permit a channel to be used for both executor and spectator APIs, but this source slice does not claim Go's retry decorators or spectator resolver fallback/onboarding semantics unless implemented and tested explicitly.
- The pinned upstream Go executor wraps Heartbeat with a 10-second timeout. The Go server spectator path applies a 10-second unary timeout and retries transient gRPC statuses (`Unavailable`, `Unknown`, `Cancelled`, `Internal`) with 200ms exponential backoff, 10-second maximum interval and 15-second expiration; the retry policy applies 20% downward jitter. Stream establishment also goes through the retry wrapper, then the spectator reconnect loop waits 1 second with 10% jitter. Rust implements the unary policy approximately and the spectator reconnect loop; exact timer behavior still needs fake-server validation.

## Rust slice boundaries

1. `config.rs`: parse typed client address and namespace config/durations; keep zero namespaces valid for Go Matching's local/no-op mode, while configured executor initialization requires exactly one valid namespace and a client address.
2. `matching_shard_distributor.rs`: implement generic processor callbacks rather than constructing queue managers, so later `matching_registry.rs` integration can adapt the existing owner lifecycle without duplicate managers. Add deterministic tests with an in-process fake tonic server and injectable clock/jitter where needed.
3. `lib.rs`: export only the new module. Do not wire `main.rs`, Matching handlers/registry, or production lifecycle in this slice.

## Open integration gaps

- No Rust caller currently owns/starts this client, supplies Matching queue processor callbacks, provides advertised host/ports or drain observer, gates task-list ownership, or stops/joins it from server shutdown.
- No mixed Go/Rust History/Frontend/Worker validation is claimed here.
