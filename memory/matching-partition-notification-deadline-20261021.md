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

Added `partition_notification_fanout_is_concurrent_and_obeys_caller_deadline`
in `matching_registry.rs`: a 10ms peer completes while a 500ms peer is bounded
by the shared 80ms caller deadline, and the best-effort collector observes both
outcomes. This source regression covers fanout timing/deadline semantics; the
existing two-host integration source gate covers owner routing and config
refresh. No Cargo, Go, cluster, or Docker command was run because the telemetry
build lane is active. Compilation and runtime behavior remain unverified for
this follow-up.
