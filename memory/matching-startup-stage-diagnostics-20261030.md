# Matching startup stage diagnostics (2026-10-30)

## Corrected diagnosis

Base: `origin/t3/postgres-gate-diagnostic` at
`c5ac7c2e8821b4a2a8b1190051e7a414a55db562`, including diagnostic source commit
`5755021dbaaeeb70b87ec9ba3c69bd06065398e4`.

The retained PostgreSQL mixed-gate trace described in
[`matching-ringpop-startup-source-audit-20261030.md`](matching-ringpop-startup-source-audit-20261030.md)
has inbound PING #1–#8, each accepted by the ready-node path. In Rust,
`handle_ping_with_full_sync` rejects pre-ready PINGs before it emits that trace
(`rust/crates/cadence-matching/src/ringpop_provider.rs`, `handle_ping_with_full_sync`),
and `MatchingRingpopProvider::start` sets `ready` only after `join_cluster`
returns and `start_swim` succeeds. The evidence therefore establishes that
Ringpop JOIN and bootstrap completed. The stall is after Ringpop Ready and
before `Matching::started`, not a JOIN stall.

The health gate is the separate `Matching::started` watch in
`rust/crates/cadence-matching/src/grpc_matching.rs` (`wait_started`), set by
`launcher.rs` after membership lifecycle startup returns. The relevant awaited
boundaries are provider start, provider subscription, then initial membership
snapshot. The latter calls `provider.members`; membership replacement and
refresh-worker spawn are synchronous. Within Ringpop provider startup, the
post-Ready work registers monitor/healer tasks, publishes labels and a snapshot,
then publishes the `Started` lifecycle state; there is no awaited operation
after `set_ready` in that method. A missing provider completion marker can
therefore be narrowed by its internal post-Ready markers.

The inspected limiter and Matching assembly do not establish a lock cycle or a
post-Ready blocker. The datastore limiters are constructed before lifecycle
startup. Their initial member-count callback reads the optional published
membership and then the ring; before publication it returns zero. Later
membership publication writes the optional value, while ring replacement does
not acquire that publication lock in reverse order. Configured persistence and
semaphore stores, partition/cache/registry construction, and runtime assembly
also occur before the membership lifecycle starts. Semaphore managers load
lazily on acquire; their registry releases its synchronous mutex before
`manager.start().await`. No startup path was found that recursively acquires a
held `std::sync::Mutex` or awaits while holding the relevant synchronous
registry lock.

Go contract comparison: `go/common/peerprovider/ringpopprovider/provider.go`
`Provider.Start` runs Bootstrap, registers the listener, sets the service and
port labels, and returns. Go Matching starts its resource and handler in
`go/service/matching/service.go` `Service.Start`. Rust preserves the same
ordering by holding the Matching start gate until provider startup, watch
registration, and initial membership refresh have completed.

## Diagnostic change

Added one-time, bounded stage messages in:

- `rust/crates/cadence-matching/src/matching_ringpop_provider.rs`: provider
  lifecycle lock acquisition; Ringpop Ready; monitor/healer registration;
  label and snapshot publication; provider lifecycle transition.
- `rust/crates/cadence-matching/src/matching_membership_lifecycle.rs`: provider
  start, subscription, initial snapshot, membership publication and refresh
  worker boundaries.
- `rust/crates/cadence-matching/src/launcher.rs`: startup-failure cleanup and
  normal shutdown boundaries for membership withdrawal, runtime shutdown, and
  membership teardown.

Messages contain only fixed stage names and success booleans; they do not log
member lists, addresses, or request payloads. They make no lifecycle or
admission changes. The next mixed run should preserve the ordered startup and
shutdown stage lines alongside the existing JOIN and PING trace. The last
`begin` without a matching `complete` identifies the held operation. If all
provider and membership stages complete but the health gate remains closed,
inspect the `mark_started`/watch transition next.

No actual deadlock or unbounded startup stage is proven by current source or
the retained trace. No production behavior fix is justified. No Cargo, Go,
formatter, runtime, CI, PR, or mixed-gate command was run for this diagnostic
change; the owning parent run `41035815248` was not polled or dispatched.
