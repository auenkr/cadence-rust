# Independent Matching Ringpop review (2026-10-30)

## Scope and reviewed identity

Source review was performed against candidate commit
`6c73d9aa447171d4eeda37d71dd0a955d0996a9b` on
`origin/t3/matching-grpc-connection-diagnostic-565c8fc`; its parent is
`565c8fc91893fae1a81cd8e47f10f2ee4744c046`. The review covered Ringpop direct
PING and relayed PING-REQ completion, Go SWIM behavior, TChannel framing and
response writes, connection lifetime, state-lock scope, and the retained failed
PostgreSQL run. No Cargo, Go, formatter, runtime, or CI command was run or
dispatched.

## Source comparison

- Go `ringpop-go v0.10.0` `swim/ping_sender.go::sendPing` calls
  `IssueAsSender`, waits for `json.CallPeer`, and only bumps sender
  piggyback counters after a successful call. Candidate Rust
  `rust/crates/cadence-matching/src/ringpop_provider.rs::RingpopNode::ping`
  (lines 1310-1357) follows that order: TChannel response and complete JSON
  decode precede `complete_sender(..., true)` and membership merge.
- Go `swim/ping_request_sender.go::MakeCall` bumps sender counters only on
  call/decode error; successful calls update received membership in
  `SendPingRequest`. Rust `ping_request` (lines 1359-1445) keeps the call in a
  detached task so an outer timeout does not cancel the transport/error
  callback, applies successful changes only after receiving the result, and
  does not bump on success.
- Go `swim/ping_request_handler.go::handlePingRequest` applies request changes,
  sends a normal PING using `node.pingTimeout`, merges target changes only on
  success, then issues receiver changes for its reply. Rust
  `relay_ping_request` (lines 1513-1605) follows that sequence and releases the
  state mutex before the outbound network await. This review found no concrete
  SWIM callback-order or lock-across-network-await mismatch.
- Candidate `rust/crates/cadence-matching/src/tchannel.rs` handles TChannel
  transport `PING_REQ` with an empty `PING_RES` on the same frame ID (line 653).
  Completed Ringpop JSON calls dispatch as independent tasks (lines 664-670);
  successful responses retain the call ID and hold the shared writer mutex
  across all response fragments (lines 464-486). The call path holds no Ringpop
  state lock while writing. The outbound client in
  `rust/crates/cadence-service-core/src/tchannel_json_client.rs` uses a fresh
  TCP/TChannel connection per call, initializes ID 1, sends call ID 2, then
  reads all fragments through the final response (lines 350-475). This differs
  from Go's pooled peer reuse but no source evidence here shows that one-shot
  connection ownership violates the protocol or explains a lost response.

## Failed run evidence and limits

The retained terminal artifact is `/tmp/cadence-41094318642-review-20261030/`
`matching-production-postgres/`. It identifies the binary source as the
candidate above and records SHA256
`618ae960e2f0f8c68d3c6643351936343fad480c4e1d0d8e27afb660746a8605`.
The PostgreSQL gate failed before SDK acceptance: `run/results.json` has
`acceptanceEligible: false`, and `run/probe.log` says Go Frontend readiness
failed and direct primary gRPC timed out after 10.005s with `Waiting for LB
pick`.

`run/rust-matching/rust-server-before-restart.log` records three successful Go
JOINs, Rust `Ringpop ready`, membership monitor/healer startup, and eight
`Matching Ringpop received PING` entries. Those messages are handler-entry
evidence only. They contain no TChannel call completion, JSON response write,
or Go `CallPeer` result. The first `ListTaskListPartitions` trace completes
through `engine-complete`; a later entry reaches `engine-begin` and has no
subsequent stage in the retained log. That is a separate readiness-path
observation and does not establish a Ringpop cause.

Go log membership removals occur at `2026-10-30T17:05:00.170Z`, alongside
service shutdown and membership resolver/hashring stop logs. Therefore the
reported removal roughly 23 seconds after Rust PING handler entries is
consistent with cleanup after the failed readiness probe. The timing cannot
be used as proof that Go peers failed SWIM probes before cleanup.

## Discriminating evidence required

On a fresh mixed run using this exact candidate and binary identity, retain
correlated events for each Ringpop application call:

1. On Rust, record peer address, service/method, TChannel call ID and trace ID,
   handler completion or error, response-frame count/byte lengths, and each
   response write completion/error. Emit a final `call-response-written`
   event only after all fragments have completed writing.
2. On Rust outbound calls, record connect/init completion, request write
   completion, response frame IDs/types through the final fragment, JSON
   decode completion, and success/timeout/error. Use a unique trace identifier
   per call so concurrent probes can be joined to peer logs.
3. On Go peers, capture `sendPing`/`CallPeer` completion or timeout for the
   Rust address, including the peer address and matching trace/call identifier.
   Capture Go peer shutdown/signal timestamps independently.
4. Preserve these events from process start through the failed readiness
   probe, then distinguish removals timestamped before the first shutdown
   signal from removals emitted after it. Include PING-REQ relay outcomes if
   the direct probe fails.

If Rust records full response writes but Go records call timeouts, inspect the
wire response IDs/fragments and server-side peer connection state. If Rust
records a write failure or no terminal response event after handler entry,
the Rust write task/connection lifecycle becomes the source target. If Go
records successful PING completions, this run's readiness failure should be
diagnosed on the gRPC/readiness path instead. No production fix is justified
until one of those outcomes is captured.
