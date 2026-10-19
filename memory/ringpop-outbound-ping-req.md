# Ringpop outbound PING-REQ and indirect probe

This task adds the outbound half of Ringpop v0.10.0 `swim/ping_request_sender.go` to the Rust matching provider. Keep it within Matching; it does not add the SWIM scheduler or runtime wiring.

## Pinned Go behavior audited

Source: `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/ping_request_sender.go`, with helper selection in `swim/memberlist.go` and scheduler outcome handling in `swim/node.go`.

- `MakeCall` invokes `IssueAsSender` and calls the returned counter callback on RPC/JSON error only. Successful PING-REQ responses do not increment the sender's dissemination counters. This differs from direct PING.
- The sender caller merges all decoded response `Changes`, regardless of `pingStatus`; Stop/Destroy only suppresses that memberlist update. The MakeCall error callback runs independently of Stop/Destroy and can run after the caller's context timeout.
- `sendPingRequests` selects random pingable members without replacement, excluding local and target addresses; only ALIVE and SUSPECT members qualify. `RandomPingableMembers(0, ...)` still selects one eligible member because it appends before testing `len >= n`; a negative size panics while allocating the result/channel in Go.
- `indirectPing` returns on the first positive response without cancelling other sends. It preserves errors received before that response; with no selected helpers it returns false and no errors. `pingNextMember` later compares error count with configured `pingRequestSize`, not the selected-helper count; scheduler wiring is outside this task.

## Rust implementation and validation status

`RingpopNode::ping_request` launches an independent sender task for TChannel call/decode and its error-only dissemination callback. The caller separately waits with the configured timeout; cancellation or timeout cannot cancel the sender task or merge a late successful response. `send_ping_requests` randomly selects eligible helpers and returns concurrent results; `indirect_ping` returns at first positive result while remaining tasks continue.

Added Rust tests for true/false decoded responses, response membership merge, decode and transport errors, callback after caller cancellation, concurrent error aggregation, early success, and completion of a late helper. Added a Rust requester example and a pinned Go helper/target interoperability harness; its runner builds the example unconditionally so it cannot reuse a stale binary. Added a pinned-Go counter oracle for success true, success false, and RPC error.

Checks completed so far: `rustfmt`, `gofmt`, `git diff --check`, Python runner syntax check, Go test compile check, and pinned Go `TestGoPingRequestSenderCounterOracle` all pass. Final Cargo, Go interoperability, and Clippy results are recorded below.

Known boundary: the Rust API takes `usize`, so it can model the Go zero-size selection quirk but not a negative size's Go panic. The current Rust primitive does not expose a signed amount.

## Final validation — 2026-10-19

Frozen candidate branch `t3/matching-outbound-indirect` at parent baseline `440ff60c74c11426665a358cab536cfc5bf6cdb4`, with only this outbound PING-REQ slice added. No scheduler, discovery, main, or metadata files are in scope.

With `CARGO_BUILD_JOBS=1` and `CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-975b4b2d/rust/target`, all focused Rust tests passed (exit 0):

- `cargo test --locked -p cadence-server --lib outbound_ping_req_merges_false_responses`
- `cargo test --locked -p cadence-server --lib indirect_ping_runs_helpers_concurrently`
- `cargo test --locked -p cadence-server --lib ping_request_selection_excludes_local_target`

The selection case confirms local/target/faulty-member exclusion, ALIVE and SUSPECT eligibility, the Go amount-zero one-helper quirk, no-replacement selection, and sparse helper pools. The concurrency case confirms errors observed before early positive success are retained and remaining sends complete.

Fresh interoperability passed against the pinned `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0`: `python3 tests/run-ringpop-outbound-ping-req-compat.py` rebuilt the current example, then both `TestRustOutboundPingRequestAgainstGo` (Rust requester → Go helper → Go target) and `TestGoPingRequestSenderCounterOracle` passed (exit 0). Strict lint passed: `cargo clippy --locked -p cadence-server --all-targets -- -D warnings` (exit 0).

Raw logs and exit codes are in `memory/evidence/ringpop-outbound-ping-req/` (`focused-final.*`, `interop-final.*`, `clippy-final.*`). `rustfmt`, `gofmt`, `python3 -m py_compile`, and `git diff --check` also passed. Evidence is scoped to this outbound PING-REQ behavior; it does not complete Matching or establish production scheduler/runtime parity.
