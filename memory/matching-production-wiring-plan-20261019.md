# Matching production assembly wiring contract (2026-10-19)

Source audit was corrected against accepted root `a3745a7d7` (the `b1a7f52e6`
checkpoint predates the concrete provider/listener additions). The provider,
History snapshot adapter, and prebound-listener APIs below already exist in
that accepted source. Production wiring in `main.rs` remains incomplete. This
is an assembly recipe for the Matching owner; it does not claim service
completion. The configured-store worker owns `main.rs` and config edits, so
this note deliberately changes no production code.

## Go lifecycle and transport ordering

The reference is `go/service/matching/service.go:89-157`,
`go/common/resource/resource_impl.go:419-504`,
`go/common/membership/resolver.go:120-185`, and
`go/common/rpc/factory.go:54-171`:

1. `NewService` builds the common Resource and retains shard-distributor,
   drain-observer, and percentage-onboarded dependencies.
2. Matching `Start` wins the Initialized -> Started transition, constructs its
   engine/handler, and registers both Thrift and gRPC handlers on the shared
   dispatcher before starting the base Resource.
3. Resource starts the shared RPC factory and dispatcher first. The factory
   owns one prebound TChannel channel transport (the same channel is returned
   by `GetTChannel` for Ringpop), plus configured gRPC/HTTP inbounds and
   outbound builders. It starts the optional shard-distributor spectator,
   then the membership resolver and its service rings, caches, and config
   stores; it obtains `WhoAmI` before declaring the resource started.
4. Matching starts its handler/engine only after the base Resource. The engine
   starts the shard-distributor executor and registers domain callbacks.
   Handler health waits until this start completes. Listener/dispatcher
   startup must not expose Matching as ready before its dependencies and
   ownership state are attached.
5. On Stop, Matching calls membership `EvictSelf`, waits the live
   `matching.shutdownDrainDuration` while still serving, closes its stop
   signal, and stops the handler/engine (executor and task managers) before
   stopping Resource. Resource then stops rings/provider and spectator,
   dispatcher, RPC factory/shared TChannel, caches, and backing resources.

## Rust production wiring sequence

Use one lifecycle owner retained alongside the `MatchingRuntime` in main:

1. Validate Matching, shard-distributor, persistence, and transport settings.
   Bind the TChannel listener early enough to obtain its actual local address.
   Construct `MatchingRingpopProvider` with that address, service label
   `cadence-matching`, configured gRPC port, and Ringpop config. Its
   `handler()` belongs on this listener; `start()` performs bootstrap/JOIN,
   starts SWIM, sets readiness, publishes local service/port labels and starts
   its member snapshot monitor.
2. Pass the same prebound listener to the existing
   `tchannel::serve_matching_with_ringpop_listener` API. It dispatches
   Ringpop's `ringpop` JSON calls and Matching's `cadence-matching` Thrift
   calls over one socket. Do not bind another TChannel listener or advertise
   a different port.
3. Start `MatchingMembershipLifecycle` with the provider and a Matching
   membership object. It owns provider start, watch registration, initial
   refresh, subsequent refresh and withdrawal/teardown. Wait for its initial
   snapshot before exposing Matching readiness. Keep the same provider/node
   available for History routing, Matching forwarding/cleanup, local identity,
   and self-eviction.
4. Use `RingpopHistoryMembership::new(Arc<RingpopNode>)` as the current
   snapshot-backed History adapter, construct
   `MatchingHistoryPeerResolver` with `cfg.persistence.numHistoryShards` and
   `NamedPort::Grpc`, then call `MatchingHistoryClient::with_peer_resolver`
   before passing it to the durable runtime. This source adapter currently
   supplies only named gRPC endpoints.
5. Construct the shard-distributor spectator when its client endpoint is
   configured, and create the executor only for a configured Matching
   namespace. Give the executor a processor factory that connects shard
   ownership to the current Matching queue/processor owner; a placeholder
   processor or no-op factory is not production assembly. Attach the
   membership view and executor to the same Matching handler/runtime.
6. Only after all providers, cache/store, queue registry, History resolver,
   executor, and listeners are started should startup be announced ready.
   Every startup error must unwind already-started resources in reverse order.
7. For signal or listener failure, first evict self, then honor the live drain
   duration with service listeners still running. Stop Matching queue/handler
   work and await executor DRAINING/processor shutdown; then stop membership
   watches/SWIM and spectator, shut down the runtime/cache/store, and finally
   close the shared listener. Preserve and return both the serving failure
   and teardown failure where both occur.

## Concrete API mismatches to resolve in the assembly owner

- At accepted root `a3745a7d7`, the prebound listener API
  `tchannel::serve_matching_with_ringpop_listener(listener, matching,
  handler)` exists. `MatchingRingpopProvider` implements
  `MatchingPeerProvider`, derives the advertised address from the already-bound
  listener, and owns bootstrap, SWIM, readiness, labels, snapshots, eviction
  and stop. `main.rs` still calls the separate `serve` / `serve_matching`
  paths and does not construct or inject this provider, so the gap is main
  assembly, not missing provider/listener APIs.
- `MatchingMembershipLifecycle` already starts the provider, registers the
  service-scoped watch and obtains an initial snapshot, maintains the Matching
  ring, then withdraws and stops the provider after drain. It is not wired in
  `main.rs` and `MatchingRuntime` does not own it.
- `MatchingRuntime` has `with_membership_evictor`, but its public
  `matching()` returns `&Matching`; it has no builder/constructor that installs
  `MatchingMembership` before serving. `Matching::with_membership` consumes
  the handler value. Assembly needs a runtime constructor/builder that accepts
  membership and installs it before extracting the service and starting
  maintenance.
- `RingpopHistoryMembership` already adapts the live `RingpopNode` memberlist
  into Go-filtered History `Host` snapshots and maps the `grpc` named port.
  `MatchingPeerProvider`/`PeerMember::named_address` separately preserves
  parsed named ports and TChannel fallback for matching membership snapshots.
  Main must instantiate and inject the existing History adapter/resolver.
  `RingpopHistoryMembership` itself is gRPC-only; `Host` has no TChannel field.
- `MatchingHistoryClient` is gRPC-only (`HistoryApiClient<tonic::Channel>`).
  A `NamedPort::Tchannel` resolver endpoint cannot be called by this client.
  Go's RPC factory can route History using configured gRPC or TChannel
  outbounds; if production config selects TChannel, a TChannel/Thrift History
  client adapter (covering the Matching->History RPC methods used here) is
  required. The existing `tchannel_json_client` is only Ringpop JSON and does
  not satisfy that contract.
- `MatchingHistoryClient::with_peer_resolver` and
  `RingpopHistoryMembership` exist but current `main.rs` constructs History
  with `http://127.0.0.1:<history port>` and does not call the resolver
  builder. The resolver's shard count must use configured `numHistoryShards`;
  a fixed loopback channel breaks multi-host routing and owner redirects.
- `ShardDistributorExecutor::new` requires a `ShardProcessorFactory`, but
  `MatchingRuntime`/`main.rs` currently neither constructs the executor nor
  binds processor creation to the current queue registry. `TonicSpectator`
  and executor lifecycle exist standalone; their `start`/`shutdown` must be
  owned across the same readiness, signal, and listener-error paths as the
  service. The existing executor uses `Arc<dyn ShardProcessorFactory>` and
  the runtime constructors accept no SD dependencies.
- `AppState`/`Matching` currently lacks Go Matching's stable self identity,
  percentage-onboarded policy, drain observer, and provider lifecycle owner.
  Do not replace Go's local Ring behavior wholesale with SD ownership: retain
  the configured onboarding/exclusion fallback and service-specific
  membership/hash ownership path.

## Current Rust source anchors

- Main assembly and current loopback History/TChannel split:
  `rust/crates/cadence-server/src/main.rs` (`run` branch around runtime and
  `serve_matching`).
- Runtime ownership/drain hooks:
  `rust/crates/cadence-server/src/matching_runtime.rs`.
- Matching's consuming membership hook and maintenance:
  `rust/crates/cadence-server/src/grpc_matching.rs` (`with_membership`,
  `maintain_membership_until`).
- SD client APIs:
  `rust/crates/cadence-server/src/matching_shard_distributor.rs`.
- History resolver and gRPC-only client:
  `rust/crates/cadence-server/src/matching_history_peer_resolver.rs`,
  `rust/crates/cadence-server/src/matching_history.rs`.
- Shared TChannel route exists but is not wired by main:
  `rust/crates/cadence-server/src/tchannel.rs`
  (`serve_matching_with_ringpop` / internal `TcpListener::bind`).
- Accepted root has the concrete provider/lifecycle/adapters, still not wired
  by main: `rust/crates/cadence-server/src/matching_ringpop_provider.rs`,
  `matching_membership_lifecycle.rs`, `matching_history_membership.rs`,
  `matching_peer_provider.rs`, `ringpop_provider.rs`, and `ringpop_runtime.rs`.
- Prebound shared listener entry point:
  `rust/crates/cadence-server/src/tchannel.rs`
  (`serve_matching_with_ringpop_listener`).

No builds/tests were run for this source audit. This contract does not validate
production wiring and does not authorize History service migration.
