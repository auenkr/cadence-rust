# Ringpop concurrent JOIN group slice

Status: implemented, focused Rust and actual pinned-Go peer comparisons pass.
Implementation commit: `9005ae105065e5b4de188626dad683a73cd0f066`.
Pinned Go source hash for `swim/join_sender.go`:
`b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6`.
Rust implementation hash for `rust/crates/cadence-server/src/ringpop_provider.rs`:
`96cde190a9aa2167b4df0f26bc45711069e16da4d078eb848dbf8e0025ff0183`.
This is one `JoinGroup` attempt only. Matching remains incomplete; this does
not add `JoinCluster` retry/deadline/delay behavior, provider/bootstrap wiring,
readiness transitions, gossip startup, or service lifecycle integration.

## Source contract and implementation

Authority is `github.com/uber/ringpop-go v0.10.0`,
`swim/join_sender.go:330-407` (`JoinGroup`) and `:409-449` (`sendJoinRequest`).
`JoinGroup` selects once, increments `numTries` once, starts one goroutine per
selected address, waits for all requests, appends successful target addresses
and failed target addresses as collections, and merges each complete successful
response through `AddJoinList`. Any request error is a failed target; timeout
only changes Go's debug message. Response collection order is concurrent and
not stable.

Rust now exposes `JoinGroupState` to retain one sender's selection and signed
Go `int`-width attempt count, and `RingpopNode::join_group` for one group call.
It uses the integrated `BootstrapSelection`, launches all selected calls in a
Tokio `JoinSet`, waits for every result, records target outcomes, and reuses
`join_seed_signed` for the exact `ringpop` `/protocol/join` TChannel JSON path
and atomic membership decode/merge. `JsonCallError` variants both become failed
targets, matching Go's `err != nil` collection. Outcome vector ordering is
completion order. No seed de-duplication, truncation, per-target retry, or
cross-group policy was added.

The JOIN request's `timeout` stays signed nanoseconds all the way into the JSON
request. Nonpositive values map to an immediate local Tokio deadline without
unsigned conversion. Zero-option defaulting and signed nonzero retention remain
owned by the selector's `Options::from_signed` logic.

Dropping the group future drops its `JoinSet` and aborts the child calls. A
response decoded after `RingpopNode::stop()` is not applied by the existing
stopped guard in `join_seed_signed`. A malformed full response is decoded
before membership is touched. Readiness is unchanged.

## Evidence

Evidence is under `memory/evidence/ringpop-join-group/`:

- `join59f/go-peer.log`, `go-peer.exit`: exit 0. The existing Go overlay runner
  now runs both the prior one-peer JOIN regression and
  `TestRustOutboundJoinGroupAgainstGo`, which sends one Rust group to two real,
  independent pinned Go Ringpop nodes and verifies both successes and both
  merged members.
- `join59h/group-tests.log`, `group-tests.exit`: exit 0; two tests cover mixed
  success/application-error collection, concurrent overlap, wait-all behavior,
  membership merge, group-future cancellation, and stop during an in-flight
  response.
- `join59h/join-regression.log`, `join-regression.exit`: exit 0; existing JOIN
  nil/empty response, application failure, malformed JSON atomicity, deadline,
  and cancellation probe.
- `join59h/ping-regression.log`, `ping-regression.exit`: exit 0; existing PING
  response/decode/delta regression.
- `join59h/clippy.log`, `clippy.exit`: strict locked all-target `cadence-server`
  Clippy with `-D warnings`; final result recorded in the exit marker.

Earlier numbered attempt directories preserve initial fixture/compile failures
and their exit markers. They were corrected; use the final `join59f` Go result
and `join59h` Rust evidence above for current status.

## Next bounded task

Review this primitive, then add the `JoinCluster` controller: joined count,
success threshold, max-duration check after each complete group, elapsed-time
error, and delay seam/Go backoff behavior. Keep readiness and broader provider
wiring out until their source-backed slices are handled. Matching is not
complete and History must not start yet.

## JoinCluster retry/deadline/delayer slice (2026-10-18)

Implemented the bounded controller and backoff primitive in
`rust/crates/cadence-server/src/ringpop_provider.rs`, with the selector's
source-backed single-node predicate in
`rust/crates/cadence-common/src/ringpop_bootstrap_selection.rs`. It reuses
`RingpopNode::join_group` for complete concurrent groups and carries the
existing `JoinGroupState` across retries. It accumulates each successful
target verbatim (duplicates count), checks success before elapsed time, checks
the strict `elapsed > max` deadline only after a whole group, and sleeps only
when another group is needed. Negative effective sizes therefore succeed
after one group on a multi-node bootstrap; exact self-only bootstrap returns
before any group as Go `SingleNodeCluster` does. No readiness, gossip, or
provider wiring was added.

`ExponentialJoinDelayer` preserves the Go 100ms/60s defaults, integer
millisecond window, first exponent zero, `[next_min, capped)` jitter, fixed
cap behavior, escalation state, and source-equivalent invalid random-bound
panic. `JoinClock` provides elapsed, signed-nanosecond sleep, and random seams;
`SystemJoinClock` supplies production wall-clock sleep/random behavior.
`new_signed` retains signed duration inputs and Go's millisecond truncation.

The actual pinned-Go overlay test calls original `joinSender.JoinCluster` with
a failed real JOIN group, `maxJoinDuration: -1`, and the source's no-op delayer.
It asserts one complete group before the deadline error without depending on
exact wall time. The test was run through a temporary overlay with an explicit
test regex; the checked-in peer probe runner remains unchanged. The original overlay result is in
`memory/evidence/ringpop-join-cluster/slice1/go-original.log` (exit 0).

Final Rust evidence: 18 focused JOIN/PING/controller tests passed; strict
locked all-target Clippy for `cadence-common` and `cadence-server` passed.
Logs and exit markers are in `memory/evidence/ringpop-join-cluster/slice1/`.
The full current Go overlay runner passed the one-peer JOIN and new original
JoinCluster cases but failed its existing two-peer Rust group probe three
times with `Error: invalid socket address`; that failure is distinct from the
controller comparison. The prior `join59f` evidence above still records a
successful two-peer group comparison. No change was made to that probe.
The latest full overlay transcript and exit marker are
`memory/evidence/ringpop-join-cluster/slice1/go-full-overlay.log` and
`.exit` (exit 1); the controller-only original-Go run above exits 0.

Pinned Go source hashes remain unchanged:

- `swim/join_sender.go`: `b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6`
- `swim/join_delayer.go`: `f33d52c1835289fc176570011d046cccd0b299f063b62218b6a9925a4c202745`

Implementation SHA-256 at slice completion:

- `rust/crates/cadence-server/src/ringpop_provider.rs`:
  `5e490a00c5763252ff4af6393b375eedfc32b93a6f980be4d2694d1b0e704c85`
- `rust/crates/cadence-common/src/ringpop_bootstrap_selection.rs`:
  `590c462f660f8384074544b86bbde26b48b67201108c91793cab35b3df793ede`

Matching is still incomplete. Do not treat this controller slice as provider
bootstrap or readiness completion; these are separately bounded Matching gaps.

## Bounded controller repair (2026-10-18)

The previously unintegrated controller commit
`88f96334649121294bb651dbf3fbdd667e7de4e2` was reapplied on the current branch.
The parent rollback was followed by this repair:

- Replaced `SystemJoinClock`'s blocking `std::thread::sleep` with Tokio's
  awaitable timer. `JoinClock` now exposes an async sleep seam while the
  delayer returns its computed nanoseconds independently. A current-thread
  runtime test proves another task runs during retry sleep and aborting the
  controller cancels that sleep.
- Reset the system elapsed timer at `join_cluster` invocation, so time spent
  after constructing the clock is excluded from `maxJoinDuration`.
- Matched Go `time.Duration.String()` fractional millisecond/microsecond
  precision, with original-Go string fixtures.
- Modeled `Destroyed` separately from `Stopped`; the controller checks it at
  each round boundary and returns Go's `node destroyed while attempting to
  join cluster` error. Deadline/destruction errors retain accumulated JOIN
  successes, matching Go's `(nodesJoined, error)` return. A stopped but
  non-destroyed node remains distinct.
- Compared the delayer to pinned ringpop-go v0.10.0 with actual Go source and
  injected randomizer/sleeper: exact integer-ms conversion, shifting jitter
  windows, cap behavior, and zero/negative option edges. Rust tests execute
  the actual `join_cluster`/JOIN transport path and timer behavior, rather
  than comparing serialized fixture shapes.

Final reproducible evidence is under
`memory/evidence/ringpop-join-cluster/slice1/`:

- `rust-focused.log`, `rust-focused.exit`: 22 controller/provider tests passed
  (exit 0), including async yield/cancellation, fractional formatting, and
  zero/negative duration cases.
- `go-original.log`, `go-original.exit`: actual pinned Go original
  `JoinCluster`, delayer and `time.Duration.String()` overlay tests passed
  (exit 0). The Go delayer test substitutes only its randomizer and sleeper.
- `clippy.log`, `clippy.exit`: `cargo clippy --manifest-path rust/Cargo.toml
  -p cadence-server -p cadence-common --all-targets --locked -- -D warnings`
  passed (exit 0).
- `go-full-overlay.log`, `go-full-overlay.exit`: the earlier full overlay's
  two-peer `RustOutboundJoinGroupAgainstGo` probe still fails with
  `Error: invalid socket address`; the one-peer probe and original-Go
  controller tests pass. This is the separate stale binary runner assigned
  to dependency 64. This repair did not change the runner or repeat the
  two-peer failure run.

Pinned Go hashes remain `swim/join_sender.go`
`b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6` and
`swim/join_delayer.go`
`f33d52c1835289fc176570011d046cccd0b299f063b62218b6a9925a4c202745`.
Final source hashes at commit preparation:

- `rust/crates/cadence-server/src/ringpop_provider.rs`:
  `c69db1a6a49b9b8580bde8b68b579508df55f65d94210699e729a67e9bb7873c`
- `rust/crates/cadence-common/src/ringpop_bootstrap_selection.rs`:
  `590c462f660f8384074544b86bbde26b48b67201108c91793cab35b3df793ede`

Matching remains incomplete. The bounded change excludes readiness, gossip,
and main/provider wiring and does not establish mixed-cluster compatibility
completion.
