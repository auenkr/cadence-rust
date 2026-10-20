# Matching production main assembly checkpoint (2026-10-20)

Source-only implementation on `t3/matching-runtime-owner`, rebased as
`c67621ed3` + `c24b1abd5` onto root `160fcc6f3`. The old tip is preserved at
`archive/matching-runtime-owner-main-before-root-rebase`. No Cargo build or
tests have been run for the main assembly; do not claim production wiring
until the serialized build slot and mixed production gate are complete.

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

## Source-only main review follow-up

Fixed a control-flow bug where tuple pattern evaluation took and dropped the
runtime when membership was absent, which affected embedded/no-config startup.
The runtime is now taken only after a membership snapshot exists. The focused
Thrift runner's embedded case now drives the original Go generated client
through an actual task flow and Go-seeded SQLite task delivery, so it exercises
the data-dir durable queue path rather than only checking a listening socket.

Production Matching now prebinds its gRPC listener before Ringpop publishes the
gRPC label. `grpc_matching::serve_with_listener` uses the same Tonic service
registration and message-size limit as the address-based path. The bounded
Thrift runner includes an occupied-gRPC-port startup check before Ringpop
membership starts. SIGTERM/SIGINT during Ringpop bootstrap now unwinds the
partially started membership/runtime/listener and exits cleanly. The production
mixed runner's result metadata now records the discovered History gRPC path
and its `system.enableGRPCOutbound=true` requirement. Source review verified Go
defaults that property to true and the Go config-store lookup applies the last
unfiltered global value.

These changes are source-only. Python syntax compilation and `git diff --check`
passed; no build or compatibility test was run while the SWIM/shard-distributor
lane owns the build slot. Existing untracked `artifacts/` were preserved.

Frozen post-slot commands (use `tmux` session `cadence-rust` and preserve the
result directory): build with
`CARGO_BUILD_JOBS=1 cargo build --manifest-path rust/Cargo.toml -p cadence-server --bin cadence-server`,
then run `python3 tests/run-matching-thrift-compat.py --server-bin rust/target/debug/cadence-server`
and the actual mixed production SDK/restart gate:
`python3 tests/run-go-service-baseline.py --rust-matching --matching-restart --rust-production-binary rust/target/debug/cadence-server --artifacts-dir /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-main-gate-20261020`.
The latter starts Go Frontend/History/Worker only, leaves Go Matching absent,
requires original Go Ringpop discovery, runs the SDK workflow, SIGKILL/restarts
Rust Matching against the same SQLite database, and verifies recovery. Its
runner intentionally marks the current production mixed result as preliminary
until the separate parity audit and failure/recovery cases are complete.

Gate attempt 1 on source commit `1a8591566` stopped fail-fast at strict server
Clippy (exit 101): Clippy flagged `.last()` on a double-ended iterator in
`dynamic_bool_property`. Replacing it with `.next_back()` preserves the audited
Go behavior (last unfiltered global value wins) while satisfying the lint.
Attempt 1 raw output and exit markers remain under
`memory/evidence/matching-main-gate-20261020-1a8591566/`; it did not reach
focused or mixed live probes. Attempt 2 uses fresh evidence and source hashes,
rebuilds the binary, reruns strict server Clippy and the live probes. The
attempt-1 no-run, runtime/lifecycle/provider tests, and common/persistence
Clippy apply to unchanged crate/test sources; no full no-run rebuild is needed
for the one-line main-only correction.
