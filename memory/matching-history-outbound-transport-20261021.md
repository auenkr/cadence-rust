# Matching outbound History transport audit (2026-10-21)

## Current-source result

The audit item saying production Rust Matching rejects
`system.enableGRPCOutbound=false` predates implementation and is false for
current source. Commit `8b3e7991a` wired the existing native YARPC/TChannel
Thrift transport into production startup. No implementation rewrite was needed.

Pinned Go behavior:

- `go/common/dynamicconfig/dynamicproperties/constants.go` defaults
  `system.enableGRPCOutbound` to `true`.
- `go/common/rpc/params.go:76,120-134` samples the value while constructing
  RPC outbounds, and passes it to direct History and Matching outbound builders.
  It is a startup selection, not a live per-call transport switch.
- `go/common/rpc/outbounds.go:221-259` builds gRPC outbounds when true and
  TChannel outbounds when false.
- `go/service/matching/service.go:103` injects `GetHistoryClient()` into the
  Matching engine. `go/common/resource/resource_impl.go:286-300` wraps the
  raw History client with the Go retry policy.
- `go/client/history/peer_resolver.go` resolves workflow/domain/shard ownership
  through History membership and returns the selected named port. The TChannel
  path therefore requires live membership data with the `tchannel` label.

Rust `main.rs` reads the last unfiltered global boolean from its loaded dynamic
config overlay, defaults to true, and uses `RingpopHistoryMembership` plus
`MatchingHistoryPeerResolver` for the selected named port. With false, it builds
`MatchingHistoryClient::with_tchannel_outbound`; if no production Ringpop
provider exists, it errors rather than silently sending gRPC. The typed client
maps every History operation used by Matching: RefreshWorkflowTasks,
DescribeWorkflowExecution, GetMutableState, RecordDecisionTaskStarted, and
RecordActivityTaskStarted. Caller/operation deadlines bound the TChannel call;
cancelling its future drops the owned socket operation. Retry classification,
redirect to the current owner, and typed status conversion remain in the shared
History client path.

Added a source regression in `main.rs` for default-true selection, ignoring
domain-filtered values at startup, selecting from the last unfiltered global
entry, and mapping false to the TChannel named port. Existing `matching_history` tests assert TChannel connection selection,
named-port redirect, operation deadline, and cancellation. The ignored
`generated_go_history_server_accepts_native_tchannel_calls` test in
`matching_history_tchannel.rs` verifies the native wire against a generated Go
History peer for the five operations and typed errors when run with its peer
binary.

## Existing evidence

`memory/evidence/matching-dns-interop-20261020/mixed-tchannel-isolated/` records
a successful production mixed probe with `system.enableGRPCOutbound=false` in
Rust's isolated dynamic config and no override in Go's config. Go Matching was
absent; Go Frontend, History and Worker remained alive. The probe completed the
SDK workflow, query and isolation dispatch, native partition config, process
restart, and graceful drain/recovery. Its source commit `7030f2e` is an ancestor
of this branch; the History transport source files have not changed since it.
This is useful preliminary mixed evidence, but the harness labels it
`acceptanceEligible: false`, and transport selection is not traced per call.

The companion `mixed-tchannel/` attempt failed before the probe because Node
could not load `@grpc/grpc-js`; do not use that failed attempt as evidence.
`mixed-tchannel-isolated/run/results.json`, `run/probe.log`, and
`config-load-check.json` are the successful artifacts.

## Fresh telemetry-lane run queued

Do not execute until the telemetry owner releases the build lane. From the
repository root, build the current source and then run the production mixed
probe with the false selector:

```sh
CARGO_BUILD_JOBS=1 cargo build --manifest-path rust/Cargo.toml -p cadence-server --bin cadence-server
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
  CARGO_BUILD_JOBS=1 python3 tests/run-go-service-baseline.py \
  --rust-matching --matching-restart --history-transport tchannel \
  --rust-production-binary /home/ubuntu/.t3/worktrees/cadence-rust/matching-dns-provider/rust/target/debug/cadence-server \
  --artifacts-dir /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-history-outbound-tchannel-20261021
```

Review the binary hash and source HEAD; generated Rust dynamic config contains
`system.enableGRPCOutbound: false`; Go dynamic config retains its default;
results say `historyTransport: tchannel`, `goMatchingStarted: false`, and
`goServicesAliveAfterProbe: true`; startup has no missing-config or missing
TChannel membership error; the probe records completed SDK workflow events,
query/isolation/partition checks, and restart/drain recovery. Preserve
`acceptanceEligible: false` because this is one transport configuration probe,
not the Matching parity audit. No Cargo, Go, build, service, or container
command was run for this audit while telemetry owns the lane.
