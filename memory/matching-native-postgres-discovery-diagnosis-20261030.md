# PostgreSQL mixed Matching discovery failure — 2026-10-30

## Outcome

Run [40808511931](https://github.com/auenkr/cadence-rust/actions/runs/40808511931)
failed at frozen source `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` before SDK
workflows or recovery scenarios. The failure is not explained by a missing
Ringpop bootstrap host or wrong advertised Matching ports: retained Go logs
show Frontend refreshed a one-member `cadence-matching` ring at
`127.0.0.1:12973` with `grpc:14329`, received `HostsAdded`, and notified its
Matching subscribers. The Rust process log confirms both configured listener
addresses were bound. Nevertheless Go Frontend `DescribeTaskList` requests
returned `Not enough hosts to serve the request` or `Call cancelled`.

The retained evidence does not distinguish an unusable Go peer/client route
from a Rust endpoint that accepted a connection but did not complete the RPC.
`Not enough hosts` is Go `membership.Ring.LookupRaw`'s empty-hashring error
(`go/common/membership/hashring.go:173-180`); the INFO refresh proves one
membership snapshot was installed earlier, but not the ring contents at the
instant of each failed request. Do not claim that bootstrap membership alone
proves a successful RPC route.

## Route and harness correction

Production route is Go Frontend WorkflowAPI `DescribeTaskList` in
`go/service/frontend/api/task_list_handlers.go:35-55`; it resolves the domain
and calls `GetMatchingClient().DescribeTaskList`. The client bean constructs
the production Matching client in `go/common/resource/resource_impl.go:267-275`,
backed by Go's configured peer/ring resolver. The generated Go config and Rust
config use the ephemeral Ringpop ports recorded in
`/mnt/storage/service/ci-evidence/matching-native-postgres-40808511931/gate/run/ports.json`;
the Rust bootstrap list includes Go Frontend, History and Worker. Go Matching
was not started (`results.json: goMatchingStarted=false`).

Before this change, `tests/matching-service-compat.cjs::waitForRustReady`
attempted the direct Rust `ListTaskListPartitions` probe only after Go
Frontend succeeded. It therefore had no direct endpoint evidence when the
Frontend route failed. It now samples the exact primary Rust gRPC endpoint
independently and includes that status beside the last Go Frontend error in the
timeout. The Go Frontend readiness requirement remains mandatory. Also, when
the probe body has already failed, a Rust SIGTERM timeout is logged and
SIGKILLed without replacing the original readiness error; clean-probe shutdown
timeouts remain fatal. This is a diagnostic harness repair, not a production
Matching fix or acceptance result.

## Current source and validation

Compared with the failed frozen SHA, current Matching source differs only in
`matching_engine_control_native.rs`; current `launcher.rs`,
`matching_membership_lifecycle.rs`, and `matching_runtime.rs` are unchanged from
the failed pin. No later integrated native startup change is evidenced as
addressing this failure. The lifecycle/bootstrap possibilities in
`memory/matching-native-startup-independent-review-20261029.md` remain
unproven; do not edit those lifecycle-owned files as part of this harness task.

No Cargo, Go, formatter, runtime, or mixed-gate command ran locally. No Actions
run was dispatched: the current source does not yet have a coherent fresh
workspace validation pin, and dispatching PostgreSQL against the old frozen
candidate would repeat a failure without exercising current source. The next
meaningful validation is fresh uncached CI on a chosen current immutable
candidate (Clippy then workspace tests), followed by the PostgreSQL mixed gate
using that exact SHA and binary hash. That gate must retain no-Go-Matching,
SDK workflow, restart, failure/recovery, and cleanup assertions. It must inspect
the new direct-primary status and Go Frontend error separately. No mixed pass
is claimed.
