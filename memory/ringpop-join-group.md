# Ringpop concurrent JOIN group slice

Status: implemented, focused Rust and actual pinned-Go peer comparisons pass.
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
