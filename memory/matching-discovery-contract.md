# Matching production discovery contract (2026-10-17)

Scope: production peer discovery and bootstrap/configuration needed to feed Rust Matching. This is an audit artifact, not a claim of completed multi-host parity. Maintenance and HTTP listener work are separate. The relevant prior status is `memory/matching-membership-next.md` lines 42-52: runtime membership provider/wiring is explicitly outstanding; cached channels are already implemented and tested.

## Go contract: types and provider selection

The common abstraction is `membership.PeerProvider` (`go/common/membership/hashring.go:56-64`): daemon lifecycle `Start(context.Context)`, `Stop(context.Context)`, `GetMembers(service)`, `WhoAmI`, `SelfEvict`, and `Subscribe(name, handler)`. `membership.Resolver` (`go/common/membership/resolver.go:47-84`) is the public multi-service API: WhoAmI/EvictSelf, Lookup/LookupN, Subscribe/Unsubscribe, MemberCount, Members, LookupByAddress. It owns `map[string]SingleProvider`; `SingleProvider` (`go/common/membership/singleprovider.go:29-41`) is the ring API (lookup(s), subscriptions, address conversion, member list/count, refresh). `membershipfx.Module` creates one `NewHashring` per `service.ListWithRing`, then a `NewResolver` using the same injected peer provider (`go/common/membership/membershipfx/membershipfx.go:38-82`).

The production open-source implementation is `ringpopprovider.Provider`, not `host.NewSimpleResolver`. The concrete `ringpopfx.Module` provides it as `membership.PeerProvider` (`go/common/peerprovider/ringpopprovider/ringpopfx/ringpopfx.go:16-61`); it reads the `ringpop` config subtree, and supplies `rpc.Factory`'s TChannel and gRPC ports as port labels. `host.NewSimpleResolver` is a static host fixture (`go/host/membership_resolver.go:30-45`), unsuitable for production discovery: its lifecycle, evict and subscriptions are no-ops (`:47-67`).

## Exact configuration and defaults

Go's `ringpop.Config` fields (`go/common/peerprovider/ringpopprovider/config/config.go:34-50`):

* `name` (required; validation error if empty).
* `broadcastAddress` (optional IP, used for advertised address when bind/listen differs).
* `bootstrapMode`: supported strings `hosts`, `file`, `dns`, `dns-srv`; `custom` is explicitly rejected. Empty/unknown mode is rejected.
* `bootstrapHosts`: required and nonempty for hosts/DNS/DNS-SRV.
* `bootstrapFile`: required for file mode.
* `maxJoinDuration`: defaults to 10 seconds if zero (`config.go:30-32,62-71`).

Ringpop config is absent/empty by design allowed by the Fx adapter only as a nil optional result with a log message; app-level required PeerProvider dependency then fails unless another build supplies one (`ringpopfx.go:45-51`). Thus a standalone Rust production binary needs an explicit provider selection/config contract rather than silently substituting a static ring.

Reference dev config `go/config/development.yaml:21-25` uses `name: cadence`, `bootstrapMode: hosts`, seeds `127.0.0.1:7933, :7934, :7935`, `maxJoinDuration: 30s`. Matching service ports are RPC/TChannel `services.matching.rpc.port` (default development 7935) and gRPC `grpcPort` (7835) at `:41-46`; analogous frontend/history ports are independently configured. `ringpopfx.New` registers the local service full name as each process's membership role; in its bootstrap labels, `serviceName` differentiates service rings. Config struct for service ports in Rust already has `RpcConfig.port`, `grpc_port`, `host`, `bind_on_local_host` (`rust/crates/cadence-server/src/config.rs:118-136`), but Rust `Config` has no `ringpop` field (`:14-34`).

## Lifecycle, watches, refresh, removal

Startup ordering in Go Fx: `rpcFactory.Start(resolver)` first, then `resolver.Start()` (`membershipfx.go:84-92`). Resolver starts the peer provider once, then each service ring (`resolver.go:119-141`). Ring start subscribes to peer-provider updates and does an initial synchronous `Refresh`; errors are fatal, then periodic worker starts (`hashring.go:120-143`). Ringpop provider starts by bootstrapping SWIM (bounded by max join), adds itself as listener, then publishes port and `serviceName` labels (`ringpopprovider/provider.go:127-169`).

Ringpop ring-change callbacks are hints: provider maps RingChanged added/updated/removed lists to `ChangedEvent` and notifies its subscribers (`provider.go:172-195`); each ring coalesces callbacks into a refresh signal (`hashring.go:236-246`). A 2s ticker also forces refresh; calls within 1s of previous refresh are suppressed (`hashring.go:50-54,292-309,336-353`). Refresh obtains a fresh provider member list, diffs by address, builds/swaps the ring, updates address->HostInfo map and timestamp, emits the view metric, then notifies subscribers (`:292-325`). Stopping the resolver stops rings then provider (`resolver.go:143-165`); ring stops provider (idempotent), empties ring, clears subscribers, shuts worker and waits up to 1m (`hashring.go:145-171`). Self-eviction delegates to Ringpop SWIM `SelfEvict` (`resolver.go:172-175`, provider.go:198-200); it is graceful membership withdrawal, not local static removal.

Provider membership filtering is contract-critical: `GetMembers(service)` selects only members with label `serviceName == service` and SWIM status Alive or Suspect, maps address, Ringpop identity and port labels into HostInfo (`provider.go:202-259`). Missing TChannel port label falls back to port parsed from member address for backward compatibility; gRPC label is optional. `WhoAmI` returns Ringpop's address with identity label if available, otherwise address, and local advertised port map (`provider.go:262-283`). Bootstrap semantics: hosts use static seed list, file uses JSON file discovery, DNS resolves host:port seeds, DNS-SRV resolves service.domain SRV targets (`factory.go:168-184`); DNS no-record behavior can yield single-node bootstrap (`:52-85,117-166`).

## Identity and endpoint mapping

`HostInfo` has distinct address, identity and named port map (`go/common/membership/hostinfo.go:37-54,66-88`). Address is the Ringpop advertised host:port/TChannel endpoint and the ring placement member key. Identity defaults to address if empty (`:115-123`); otherwise stable identity drives ownership. `PortGRPC = "grpc"`, `PortTchannel = "tchannel"` (`:32-38`). `GetNamedAddress(name)` combines host/IP from advertised address with mapped port (`:82-89`). Provider stores local gRPC and TChannel port labels and remote member's same labels. Rust `Host` models these separately as `address`, `identity`, `grpc_address` (`rust/crates/cadence-common/src/membership.rs:5-18`). Matching remote transport must use `grpc_address`, not Ringpop placement address; placement and endpoint are independent (file header lines 1-2). Rust owner uses identity comparison while ring key/ring host address remain placement values (`matching_membership.rs:33-64`).

## Rust present state / non-overlap corrections

Rust `Membership` already atomically replaces ring snapshots, broadcasts generation updates, retains cached channels for still-present gRPC endpoints and evicts removed endpoints (`rust/crates/cadence-server/src/matching_membership.rs:6-31`, test `:112-125`). Remote channels are cached by `grpc_address`, normalize missing scheme to `http://`, and use lazy Tonic connect (`:44-65`). Do not re-implement caching as discovery work. `main.rs:354-358` merges Matching routes into the shared HTTP app; it does not start a dedicated Matching HTTP listener. Discovery should be wired into Matching's existing startup, not conflate the shared HTTP routes with production service discovery. Config has no Ringpop/bootstrap fields today, and main does not instantiate membership; evidence in main's Matching construction starts around `:300` and membership-next note above.

The existing `Ring` is a placement primitive and not a live resolver: it accepts a host iterator and computes 100 FarmHash replica points (`rust/crates/cadence-common/src/membership.rs:20-50`); it has no peer source, role filtering, watch, startup, retry, or self-eviction APIs. `Membership::new` accepts a caller-supplied initial `Vec<Host>` only; `replace` is externally driven (`matching_membership.rs:13-31`). The Go ring's 2s/1s refresh cadence and notifications are described above. Static hosts or an empty trait would only be scaffolding and are not discovery parity.

## Smallest real implementation slice (prioritized)

1. **Runnable bootstrap source + faithful Matching provider**: add Rust config deserialization for the `ringpop` block (`name`, `broadcastAddress`, `bootstrapMode`, `bootstrapHosts`, `bootstrapFile`, `maxJoinDuration`) and real seed resolution for hosts first, then file/DNS/DNS-SRV if claiming those Go config modes. Implement a concrete provider that actually joins/watches a distributed membership protocol (Ringpop SWIM-compatible wire behavior or an explicitly compatible provider); static host list alone is not cluster discovery. It must publish role/service name and TChannel/gRPC port labels, produce `(address, identity, grpc_address)`, filter Alive/Suspect service peers, and support self-eviction. Missing/unsupported config should be an explicit startup error, not silently claim cluster mode.
2. **Resolver/ring lifecycle integration**: initial synchronous discovery before serving Matching, watch event -> coalesced re-fetch and atomic `Membership::replace`, periodic refresh/retry with Go's relevant cadence, and graceful provider stop/self-evict. Feed one service-scoped membership into Matching; preserve already-cached channels and `replace` eviction behavior.
3. **Compatibility test slice**: two independent Rust server processes (or Rust plus Go Ringpop oracle) join via actual seed config; verify both see only matching role, ownership follows address/identity semantics, advertised gRPC endpoint routes real cross-host Matching RPC, adding/removing a node changes snapshots and evicts stale cached endpoint, and shutdown self-evicts. Also exercise invalid config, seed loss/rejoin, and configured dev ports. Unit tests over fabricated Host vectors do not prove provider/discovery parity.

## Source index

* Repository policy: `AGENTS.md:1-19`; Go architecture/provider/Fx requirements: `go/AGENTS.md:76-115`.
* Interfaces/algorithms: `go/common/membership/hashring.go`, `resolver.go`, `singleprovider.go`, `hostinfo.go`.
* Production provider/config/wiring: `go/common/peerprovider/ringpopprovider/{provider.go,factory.go,config/config.go,ringpopfx/ringpopfx.go}`, `go/common/membership/membershipfx/membershipfx.go`, `go/cmd/server/cadence/fx.go:44`, `go/config/development.yaml:21-46`.
* Rust comparison: `rust/crates/cadence-common/src/membership.rs`; `rust/crates/cadence-server/src/matching_membership.rs`, `config.rs`, `main.rs`; current task ledger `memory/matching-membership-next.md:42-52`.
