# Matching shard-distributor Go/Rust wire gate

This gate exercises the public Rust `TonicExecutorRpc`/`ShardDistributorExecutor`
and `TonicSpectator` clients against the actual pinned Go shard-manager handler
and generated YARPC gRPC wrappers. Its deterministic in-memory `store.Store`
fixture only supplies persistence and controlled assignment transitions; it
runs the real Go RPC handlers and their protobuf conversion code.

From the repository root, run:

```sh
tests/run-shard-distributor-go-interop.sh
```

The ignored Rust integration test starts the Go fixture with `go run`, then
checks initial assignment and spectator metadata, executor metadata and shard
load reports, reassignment, shard TTL status, assignment loss and processor
stop, and the final `DRAINING` heartbeat. It needs the repository Go module's
pinned dependencies and the Rust crate's normal test prerequisites. The fixture
accepts assignment commands on stdin and exposes only generated gRPC RPCs to
the Rust clients; it does not implement a substitute gRPC service.

This is a focused client/wire compatibility gate. It does not validate the
production Cadence process assembly, real persistence backends, leader election,
recovery across process restarts, or SDK workflows. Those remain separate
Matching parity-audit requirements.

Source review status: the harness and command are prepared, but this gate has
not been built or run in the current serialized validation lane. No runtime
result is claimed here.
