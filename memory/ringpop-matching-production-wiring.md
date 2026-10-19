# Matching Ringpop production-wiring audit

Read-only source audit for the next Matching slice after `ringpop-swim-runtime`.
No Rust source or runtime behavior was changed for this audit; Cargo was not run.

## Go lifecycle and membership contract

- `go/common/peerprovider/ringpopprovider/provider.go:New` validates
  `ringpopprovider.Config`, builds the selected discovery provider, creates
  Ringpop on the service's shared TChannel with optional broadcast address, and
  saves `MaxJoinDuration` for `Bootstrap`.
- `Provider.Start` calls `ringpop.Bootstrap` first, then subscribes to ring
  changes, then sets labels: each named port from `membership.PortMap` and
  `serviceName=<full Cadence service name>`. Preserve this exact order; do not
  move labels ahead of bootstrap as a convenience. The local Ringpop address
  and TChannel listener must exist before bootstrap. Label publication follows
  successful bootstrap and listener registration. `GetMembers(service)` uses
  `GetReachableMembers`, filters exact `serviceName` plus ALIVE/SUSPECT, maps
  identity, and reads gRPC/TChannel port labels (falling back to the Ringpop
  address port for legacy TChannel peers). `WhoAmI` prefers Ringpop's identity
  label, otherwise address. `SelfEvict` calls Ringpop self eviction;
  `Stop` removes listener then destroys Ringpop.
- Pinned `swim/node.go:Bootstrap` clarifies what happens inside that first
  call: `sendJoin`; on success start gossip and healer unless `Stopped`; set
  Ringpop `ready=true`; return. Only after it returns does
  `ringpopprovider.Provider.Start` add its Ringpop listener and set service/port
  labels. Do not delay Ringpop protocol readiness until after labels or
  membership snapshot; service handler health readiness is a separate gate.
- `go/common/membership/resolver.go:MultiringResolver.Start` starts the peer
  provider before each service ring; each ring obtains its initial peer list
  through the provider. `server.startService` injects this resolver into the
  service resource. `go/service/matching/service.go:Start` registers RPC
  handlers, starts the base resource (which owns the resolver), then starts
  the Matching handler/engine. Health waits for `handler.Start`. Shutdown in
  `Service.Stop` self-evicts, waits `ShutdownDrainDuration`, then stops handler
  and base resource/provider. Preserve this traffic-withdrawal order.
- `go/service/matching/handler/membership.go` subscribes as
  `service.Matching / matching-engine`; a ring-change event prompts task-list
  and semaphore ownership rechecks/unloads. `getNonOwnedTasklistsLocked` and
  `getNonOwnedSemaphoreManagers` compare owner identity from `Lookup` against
  `WhoAmI` identity.
- Matching's History traffic is a discovered service dependency. The engine's
  calls are `GetMutableState` (`engine.go:777`),
  `RecordDecisionTaskStarted` (`:1487`), `RecordActivityTaskStarted` (`:1517`),
  and `RefreshWorkflowTasks` (`:1542`). `go/client/history/peer_resolver.go`
  maps domain/workflow to History shard then `membership.Lookup(service.History,
  shardKey)` and resolves the named transport port; `go/client/clientfactory.go`
  and `go/common/resource/resource_impl.go` construct the resolver-aware
  History client. It must route per shard and refresh as membership changes,
  rather than pin all Rust Matching calls to local loopback History.

## Current Rust boundary and gaps

- `rust/crates/cadence-server/src/config.rs:RingpopConfig` parses name,
  broadcast address, static/file/DNS/DNS-SRV seeds and max join duration.
  `ringpop_discovery.rs` implements those discovery modes. Config validation
  exists, but `main.rs` does not consume Ringpop config or start this path.
- `ringpop_provider.rs:RingpopNode` owns one memberlist, JOIN call, SWIM
  runtime, snapshot, readiness and stop/destroy primitives. It accepts the
  local `Member` at construction, including optional labels, but this code
  does not yet make a configured provider that selects seeds, installs
  production labels/ports, subscribes as a provider, or derives self-eviction
  and shutdown from Matching lifecycle. Preserve Go's independent Ringpop
  readiness point: bootstrap success, before provider listener and labels.
- `matching_peer_provider.rs:PeerMember::matching_host` correctly filters
  exact `serviceName` and ALIVE/SUSPECT and parses a gRPC port. It omits Go's
  legacy TChannel-port fallback and does not carry the full named-port map.
  `MatchingMembershipLifecycle` already sequences provider start, subscribe,
  initial snapshot, refresh and withdrawal, but is currently a generic seam;
  no `RingpopNode` production provider is supplied by `main.rs`.
- `matching_membership::Membership` provides atomic Ring placement, endpoint
  caching and watcher notification. `main.rs` currently forms
  `MatchingHistoryClient` from `http://127.0.0.1:<history_addr>`; this bypasses
  `Membership` and Go's shard-keyed History peer resolution.
- Current matching bootstrap cannot fabricate remote members. Initial peers
  must come from the configured Ringpop bootstrap/JOIN response, then ordinary
  accepted SWIM gossip. `set_ready` models Ringpop readiness; the Go node sets
  it at successful Bootstrap return, before the provider installs labels.
  Matching handler health/startup is a distinct readiness gate.

## Smallest correct next implementation slices

### Patch A: concrete Matching Ringpop provider and lifecycle wiring

Add a concrete Rust `MatchingPeerProvider` backed by the actual local
`RingpopNode`, and wire it into `MatchingMembershipLifecycle` for `main.rs`.
Construct the node only after matching TChannel has selected its real bound or
advertised address. Run configured seed discovery and `join_cluster`/bootstrap
with Go's order: send JOINs; on bootstrap success start owned SWIM and mark the
Ringpop node ready; return from bootstrap; register the provider's membership
listener; then set named TChannel/gRPC port labels and `serviceName`. Do not
pre-seed remote members. After bootstrap returns, listener is registered, and
labels are set, expose the first filtered snapshot to Matching placement; keep
service handler health readiness separate and preserve
`matching.handler.Start` ordering. Expose real reachable ALIVE/SUSPECT
snapshots and ring-change hints. On
shutdown, self-evict before the drain interval, stop accepting requests after
drain, and tear down provider/listener/tasks in Go-compatible ownership order.
The existing generic lifecycle calls provider start, consumer subscribe and
initial snapshot synchronously. The concrete provider's `start` must itself
preserve Ringpop's internal sequence above (including Ringpop ready before its
provider-listener/label steps); consumer subscription and initial placement
snapshot then follow.
Test two real nodes and restart/recovery so JOIN and gossip, rather than
fabricated members, rebuild membership. Keep this patch to discovery,
membership and lifecycle wiring; it does not change History client routing.

### Patch B: discovered Matching-to-History shard routing

Separately replace the hardcoded local History channel in `main.rs` with a
resolver/client that consumes discovered History ring hosts and routes by the
original Go shard key selection (`DomainIDToHistoryShard` or
`WorkflowIDToHistoryShard`, then ring lookup). It must use the discovered
History transport port, handle membership changes and preserve existing RPC
protocol, timeout, retry, and Cadence error behavior. Verify against multiple
Go History peers with a Rust Matching node and prove that the same workflow or
domain key reaches the same Go-selected shard owner. Do not combine this
substantial client-routing change with Patch A; each patch needs its own mixed
cluster evidence.

Defer broader Matching parity gaps until these two separately reviewed slices
have reproducible compatibility evidence.
