# Matching production main assembly checkpoint (2026-10-20)

Source-only implementation on `t3/matching-runtime-owner`, based on the
configured-store evidence chain through `0cdd87e2c`. No Cargo build or tests
were run for this patch; `rustfmt` on the three changed Rust files and Python
syntax compilation of the modified compatibility runner passed. The source
must be rebased onto the root's merged healer/configured-store candidate and
built before any production wiring claim.

`main.rs` now binds Matching's configured TChannel socket before constructing
`MatchingRingpopProvider`, derives the advertised endpoint from that prebound
socket, and starts the Ringpop+Matching TChannel listener before bootstrap so
JOIN requests can reach it. `MatchingMembershipLifecycle` publishes the
initial membership only after provider startup; `MatchingRuntime` attaches
that snapshot to the handler and starts the service membership cleanup loop.
The production History client uses `RingpopHistoryMembership` and
`MatchingHistoryPeerResolver` with configured history shard count and the
named gRPC port. If `system.enableGRPCOutbound` is false, startup fails
explicitly because Matching's History Thrift/TChannel client path is not yet
implemented; Rust must not silently override Go's transport choice.

Shutdown now calls lifecycle withdrawal while Matching listeners remain
available, then drains/stops the Matching runtime, stops membership refresh and
Ringpop, and only then closes the TChannel listener. Startup errors unwind the
same resources. Listener, signal, withdrawal, runtime, and membership teardown
errors are collected so one failure does not skip later cleanup or hide the
other failures. `MatchingRuntime::with_membership` also starts the watcher
that unloads queues/semaphore owners after membership changes.

The focused configured-store runner now rewrites its generated Ringpop seeds
as a one-node local ring on the selected Matching TChannel port. This keeps
that native persistence/Thrift component harness independent of a separate
Ringpop cluster when the server binary is rebuilt with production main wiring;
it is not mixed-cluster discovery evidence.

Known implementation and parity gaps:

- The `system.enableGRPCOutbound=false` case is rejected because no outbound
  History TChannel Thrift adapter is wired. Go chooses transport from this
  dynamic setting (`go/common/rpc/params.go`). Complete that adapter and honor
  runtime transport changes before claiming production interoperability.
- `MatchingRuntime` and `main.rs` still do not assemble the shard-distributor
  spectator/executor or a processor factory tied to queue ownership. Do not
  use a placeholder processor to claim parity; this requires a separate
  lifecycle implementation.
- The production mixed Go Frontend/History/Worker and Rust Matching SDK gate,
  no-Go-Matching restart/recovery gate, and full Matching parity audit have
  not been run. The passing configured-store probe only covers the dedicated
  one-node harness and the captured output/hash manifest in
  `memory/evidence/matching-configured-store-20261019T220052Z/`.
- Go indexes `DataStores[DefaultStore]` exactly without normalization, while
  Rust configured-store resolvers currently call `.trim()` before lookup.
  This source-only parity gap is recorded in
  `memory/evidence/matching-configured-store-20261019T220052Z/go-default-store-lookup-audit.txt`.
