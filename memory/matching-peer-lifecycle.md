# Matching peer-provider lifecycle slice

Implemented a transport-independent async `MatchingPeerProvider` boundary in
`rust/crates/cadence-server/src/matching_peer_provider.rs` and a service-scoped
refresh driver in `matching_membership_lifecycle.rs`. It starts provider, installs
watch, then requires a successful initial snapshot before returning. Event messages
are hints; callbacks are collapsed by a single receiver and snapshots are throttled
to 1s minimum; a 2s interval forces refresh. Snapshot errors leave the last good
`Membership` snapshot untouched. Stop ordering is self-evict, worker exit, clear
local ring, then provider stop. The current API keeps Membership's atomic replace
and channel cache eviction as-is.

## Go evidence followed

* `go/common/membership/hashring.go:50-54,120-143,145-171,236-246,292-325,336-353`:
  100 point ring context, initial subscribe+refresh, callback coalescing, 2s
  periodic trigger, 1s refresh minimum, and stop behavior.
* `go/common/peerprovider/ringpopprovider/provider.go:127-169,172-200,202-259,262-297`:
  bootstrap then listener and labels, ring-change notification, self-evict, filter
  `serviceName`, Alive/Suspect states, address/identity, tchannel label fallback,
  optional gRPC label, and provider destroy.
* `go/common/membership/hostinfo.go:66-89,115-123`: identity defaults to address;
  named endpoint retains advertised host and substitutes the port.

## Scope and gaps

The in-process fake-provider tests are lifecycle contract tests, **not network
interoperability tests**. They do not prove Ringpop compatibility. This implementation
is not wired into the server startup and does not implement a concrete provider,
seed discovery, config, actual Ringpop SWIM, or a production static-host substitute.
Current member mapper represents the gRPC endpoint and identity; malformed/missing
gRPC ports map to an empty endpoint. TChannel port metadata should be carried in the
provider representation when a concrete peer protocol requires it.

## Repair review (2026-10-17)

Cherry-picked lifecycle implementation `581d908bb6986cccb24cb43515651353097030fe`
for review, then repaired: start is idempotent while running and after stop; provider
teardown runs after startup/start/subscription/snapshot errors; receiver lag remains a
refresh hint and closed receivers are disabled; stopping aborts a potentially blocked
snapshot worker. Initial synchronous snapshot is retained, and change hints are
rate-limited with periodic 2s retries. Mapping applies Go's base-10 unsigned-16-bit
port rule (0 valid, signs and overflow invalid), IPv6 bracket-host extraction, and
empty-identity fallback. Invalid placement host:port values produce an empty gRPC
endpoint; no Ringpop/network parity is claimed.

Added tests cover Go port bounds, IPv6, identity fallback, malformed endpoint, failed
subscribe cleanup, start/stop idempotence, coalescing by fetch count, closed-event
periodic refresh, and last-good snapshot after failures.

## Validation (2026-10-17)

In isolated checkout `t3-77b82231`, using named windows in tmux session
`cadence-rust`:

* `rustfmt --edition 2021 --check rust/crates/cadence-server/src/matching_membership_lifecycle.rs rust/crates/cadence-server/src/matching_peer_provider.rs` — passed.
* `cargo test -p cadence-server --lib matching_membership_lifecycle -- --nocapture` — passed, 6 passed, 0 failed, 162 filtered out (3.15s).
* `cargo clippy -p cadence-server --all-targets -- -D warnings` — passed.

The first test execution exposed a real mapping bug: an invalid placement address
could produce a `:grpc-port` endpoint because the invalid host was replaced with
an empty string before formatting. Mapping now requires both a valid parsed host
and an in-range unsigned decimal port. The initial compile also caught and fixed a
test referencing the provider's private port parser directly. Other worktrees were
using the shared tmux session/build cache concurrently; no other process was killed
or modified.

## Next exact Ringpop/TChannel JSON wire slice

Implement the actual Ringpop protocol endpoints, not a bespoke SWIM: first audit
the vendored/upstream `ringpop-go` and `tchannel-go` protocol registry for exact
method names, request/response structs, serializer (TChannel JSON), call options,
and semantics. Then build an endpoint inventory and golden request/response
fixtures against the Go implementation. Cover bootstrap join (`join` request and
membership response), membership gossip/updates, ping/ping-req suspicion/refutation,
ring checksum/view dissemination, labels (`serviceName`, `grpc`, `tchannel`, identity),
and graceful self-evict/leave. Record exact wire headers/call routing (service name,
endpoint, timeout), JSON field casing/optional fields, status transitions Alive,
Suspect, Faulty, Left, and convergence expectations. Next implement those wire
handlers/client frames over TChannel v2 with multi-process Go-Ringpop oracle tests:
join, label visibility, role filtering, update/removal, failure suspicion and
rejoin, and graceful eviction. Do not call the provider production-ready until
those tests pass and Matching startup consumes it.

Pinned `ringpop-go v0.10.0` was found in `/home/ubuntu/go/pkg/mod` and
`swim/handlers.go:38-81` confirms TChannel JSON endpoints registered on the
Ringpop channel: `/protocol/join`, `/protocol/ping`, `/protocol/ping-req`,
`/admin/debugSet`, `/admin/debugClear`, `/admin/gossip`, `/admin/gossip/start`,
`/admin/gossip/stop`, `/admin/healpartition/disco`, `/admin/tick`,
`/admin/gossip/tick`, `/admin/member/leave`, `/admin/member/join`, `/admin/reap`.
The primary next wire slice is exactly the first three protocol endpoints; next
inspect `swim/handlers.go`, `swim/{join,ping,gossip}*.go`, request structs, and
TChannel JSON registration/call code for byte-compatible schemas. Admin routes
are operational/test aids, not a substitute for protocol interoperability.

This commit implements only the transport-independent lifecycle seam and member
mapping. It is not production peer discovery and does not complete Matching. Still
missing: concrete Ringpop-compatible TChannel provider and seed/bootstrap modes;
Rust config/port-label wiring; lifecycle consumption by Matching server startup;
production two-process or Go Ringpop interoperability tests; and validation of
leave/rejoin, membership convergence, role filtering on real peer data, and actual
cross-host RPC endpoint routing.
