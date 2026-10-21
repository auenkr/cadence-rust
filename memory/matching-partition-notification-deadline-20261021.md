# Matching partition notification caller context (2026-10-21)

Source-only follow-up to review of `d30f2ef85a65f34fd0e478d1c8345f97f3fcefe5`.

## Go contract and P1 review

Pinned `go/service/matching/tasklist/task_list_manager.go:506+` builds the union
of old/new nonzero read partition IDs, launches every refresh in an `errgroup`
with the caller context, waits, and logs per-peer plus aggregate errors. Those
errors do not fail the root update. Rust now starts all peer refresh RPCs before
waiting, preserves best-effort logging, and places the remaining caller
`grpc-timeout` on each outgoing RPC. No deadline is added when the incoming
request has none. Dropping/cancelling the handler drops its `JoinSet`, which
aborts outstanding RPC tasks.

The review's P1 claim that domain callback releases must map child managers was
rejected after source verification. Pinned
`go/service/matching/handler/engine.go:1206-1213` enumerates loaded managers and
indexes descriptions by `tl.GetRoot()`. Pinned
`go/service/matching/tasklist/identifier.go:86-88` defines `GetRoot()` as the
base name, so all partitions under one root intentionally collapse to the root
key before the callback releases the manager. Rust's root mapping matches this
behavior; no change was warranted.

## Regression and validation

Added `matching_partition_update_fans_out_to_fast_peer_while_slow_peer_uses_caller_deadline`
in `matching_rpc_tests.rs`. It calls the real Matching update handler, uses live
membership and a real in-process gRPC Matching server for the later partition,
and a TCP peer that accepts but does not answer for the earlier partition. The
caller supplies an 800ms gRPC deadline. Before awaiting the update response, the
test requires the later owner to persist the refresh within 600ms. It then
requires the handler to wait through the delayed peer's caller deadline and the
root update to succeed despite that peer failure. The ascending partition IDs
make this distinguish the old sequential implementation: it would wait on the
first peer until the deadline and leave no time to refresh the later owner.

Queued telemetry-lane command (not run here):

```sh
cargo test -p cadence-server --bin cadence-server grpc_matching::matching_rpc_tests::matching_partition_update_fans_out_to_fast_peer_while_slow_peer_uses_caller_deadline -- --exact --nocapture
```

No Cargo, Go, cluster, or Docker command was run because the telemetry build
lane is active. `rustfmt --check` parsed the edited Rust file but exited 1 due to
pre-existing formatting differences throughout that file; no formatter was
applied. `git diff --check` passed. Compilation and runtime behavior remain
unverified for this follow-up.
