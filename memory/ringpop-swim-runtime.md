# Ringpop SWIM probe and transition runtime

## Scope and pinned-Go audit

Candidate branch `t3/matching-swim-runtime` starts at accepted Matching source
`13975741153ece6e5acdb44479bb67f7a9adb194`. Source of truth is pinned
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/`.

- `node.go:handleChanges` is the sole accepted-change pipeline: adjust the
  disseminator's maximum propagation count, record each accepted change, then
  cancel timers for ALIVE/LEAVE, schedule suspect→faulty, faulty→tombstone, or
  tombstone→eviction. `memberlist.Update` calls it only for accepted updates.
- `memberlist.go:Update` is a no-op when `Node.Stopped()` or the change slice
  is empty. `AddJoinList` calls `Update` first, then clears dissemination state
  for every accepted nonlocal member. Therefore JOIN timer scheduling and
  accepted-change observation happen before JOIN's dissemination cleanup.
- `node.go:pingNextMember` obtains the next iterator member, skips when no
  pingable members exist, serializes a round with `state.pinging`, does direct
  PING, then indirect PING-REQ on direct failure. The inconclusive check is
  `len(errs) == configured pingRequestSize`, even when the available helper
  pool is smaller or empty. Otherwise a false indirect result makes the
  selected target suspect at its selected incarnation.
- `memberlist_iter.go` shuffles on iterator construction and whenever its
  cursor reaches the end. `Next` scans at most the member count as measured
  during that call, returning the first ALIVE/SUSPECT nonlocal member. It can
  reshuffle and return nil after a membership-size race; a runtime should keep
  iteration and list mutations behind the memberlist lock.
- `state_transitions.go` serializes schedule/cancel/enable/disable using one
  mutex. Same-address/same-state schedule is ignored without replacing the
  captured incarnation. Different-state schedule stops and replaces the timer.
  A fired entry stays in the map; callbacks apply captured address/incarnation.
  Stop disables and clears timers; Start enables them. Local transitions are
  ignored. Tombstone timeout evicts remote membership; local eviction is
  prevented by the memberlist.
- `Node.Ready` is independent of `Stopped` and `Destroyed`; stop must not
  invent a readiness transition. Existing Go request methods retain their
  readiness/error behavior while memberlist updates are stopped.
- The already accepted common scheduler has deterministic deadlines, captured
  incarnation, state replacement/cancellation and generation tokens. Its
  pinned-Go core trace established the 5s suspect callback / 15s cascaded
  tombstone deadline; it does not yet invoke runtime membership actions.

## Implementation status and evidence

Latest validated source checkpoint: `2026-10-19`, branch
`t3/matching-swim-runtime`. The Go oracle now measures the Go metrics sample
size directly and uses a 1ns minimum so its unclamped p50 protocol rate is
observable (`50ns` for samples 10, 20, 30, 40). Final evidence from this source
is saved under `memory/evidence/ringpop-swim-runtime/`:

- `common-tests-final.log/.exit`: `cargo test --locked -p cadence-common
  ringpop_memberlist --lib`, exit 0, 8 passed.
- `server-tests-final.log/.exit`: `cargo test --locked -p cadence-server
  --lib ringpop_`, exit 0, 59 passed. Includes async stop cancellation,
  concurrent shutdown, pre-start stop, and transition timers firing while a
  probe is blocked during shutdown.
- `go-oracle-current.log/.exit`: pinned Go oracle, exit 0. Fixture records
  iterator/probe/timer behavior, p50 rate `50ns`, floor `1ns`, sample size 10.
- `compat-final.log/.exit`: pinned Go package with Rust runtime live
  interoperability and blocked Go Stop test, exit 0. All three cases pass;
  Go's post-exit SUSPECT observation is polled up to its timer transition.
- `clippy-common-final.log/.exit` and `clippy-server-final.log/.exit`:
  strict all-target Clippy for both crates, exit 0.

Async shutdown cleanup is owned by a task spawned through the executor captured
at runtime start, so cancelling the first `stop_and_wait` waiter does not strand
the runtime in `stopping`. The transition timer receives its stop signal only
after probe and rate tasks drain, following Go's `gossip.Stop` then transition
stop ordering. The synchronous `stop` API still aborts immediately.

This worktree implements a common Go-style memberlist iterator, accepted
membership-change routing into dissemination and transition scheduling, remote
member eviction with checksum refresh, and an owned Rust SWIM runtime adapter.
The runtime performs one sequential direct-then-indirect probe round, preserves
the configured helper-count inconclusive guard, marks failed targets suspect,
and owns probe/transition tasks across start, stop, and destroy. Injectable
transport/randomness supports deterministic tests. A probe example and a Go
interoperability test exercise a Rust runtime against a live pinned Go peer.

Earlier baseline evidence is retained alongside final logs for diagnosis. The final
`compat-final` run includes the refreshed cadence oracle and live peer/blocked
shutdown tests; consult that log for the complete current oracle values.

The runtime now follows pinned Go adaptive cadence: randomized first delay,
then `max(lastPeriod + lastRate - now, minProtocolPeriod)`; a 10-entry uniform
reservoir seeded with one minimum-period sample; immediate and one-second rate
updates; and `max(2*p50, minProtocolPeriod)` using go-metrics percentile
interpolation. The Go oracle records the first-delay range, behind-target
minimum floor, known-sample rate, and reservoir capacity. Rust has an async
`stop_and_wait`/`destroy_and_wait` path that cooperatively cancels timer and
sleep waits, waits for an in-flight probe, then disables transitions and marks
the node stopped. Concurrent async stop callers share completion, and restart
is rejected until that shutdown completes. The existing synchronous `stop`
still aborts tasks for synchronous callers, so callers needing Go's wait
boundary must use the async method.

The final source passed the commands listed above. Historical failed and baseline
logs remain retained and do not supersede the final evidence.

## Remaining production integration and parity gates

- The adaptive cadence and async stop paths passed the focused tests, refreshed
  Go oracle, live interoperability, and strict Clippy listed above. The
  synchronous `stop` wrapper retains abort semantics; production shutdown must
  call and await `stop_and_wait` to match Go's in-flight protocol wait boundary.
- Wire the owned SWIM runtime into the production Ringpop node lifecycle and
  configured Matching startup/discovery path. The example is not production
  service wiring; verify bind address/advertised address and shutdown ownership.
- Confirm transport details against Go in the mixed cluster: helper selection,
  timeouts, membership gossip merge, startup join, restart/recovery, and actual
  SDK workflow traffic with Go History/Frontend/Worker and no Go Matching.
- Repeat combined validation after production wiring or other source changes;
  the focused component evidence above covers the accepted runtime source.
- Complete Matching parity audit beyond this SWIM slice, including persistence,
  configuration, discovery, transports, lifecycle, and all service-facing
  interactions. Do not treat this adapter/oracle as service completion.

## Original implementation sequence

1. Add a memberlist iterator with caller-supplied shuffle/randomness and Go's
   cursor/round semantics; write a pinned Go iterator trace before wiring it.
2. Add one state transition controller to the Ringpop node. A single
   accepted-update helper will own memberlist mutation, disseminator recording,
   maximum-propagation adjustment, timer scheduling/cancel, and JOIN's
   post-update dissemination clearing. Route JOIN, incoming/outgoing PING,
   PING-REQ relay/requester responses, and locally generated SUSPECT changes
   through it. Add remote eviction/removal with checksum recomputation.
3. Add a cancellable SWIM round loop that serializes member selection, direct
   PING, indirect fallback, and suspect declaration. Inject monotonic time,
   iterator randomization, and the probe transport so unit traces are
   deterministic. Run due transition actions in the same owned runtime and
   schedule any cascade from the resulting accepted change. Generation checks
   reject stale wakes; start/stop/destroy own and cancel every runtime task.
4. Add pinned Go differential traces for probe outcomes and timer lifecycle,
   then a live Rust runtime probe with original Go peers. Run focused Rust,
   actual-Go interoperability, and strict cadence-server all-target Clippy
   only after the parent grants the serialized build slot.

No Cargo was run during the source audit. Production main/discovery wiring and
complete Matching parity remain outside this slice.
