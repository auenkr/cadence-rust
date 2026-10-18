# Ringpop TChannel JSON boundary slice (2026-10-17)

## Implemented

- Added `cadence_server::ringpop_tchannel`, an injectable async handler API. The
  handler receives the method, optional JSON arg2 headers (preserving `null` vs
  `{}`), and raw arg3 JSON bytes. It returns optional response headers and JSON
  body bytes, or an application/system error.
- Extended the existing TChannel call path to dispatch service `ringpop` only
  when a caller supplies a `RingpopHandler`. Calls require transport format
  `as=json`; responses advertise JSON transport headers and preserve normal
  Thrift framing/checksum behavior for existing services. JSON arg2/arg3 uses
  newline-terminated JSON like tchannel-go. Application errors set the response
  application-error flag and return `{"type":"error","message":"..."}`;
  system errors remain TChannel ERROR frames.
- Added `serve_ringpop` and `serve_ringpop_with` entry points. Normal frontend
  and Matching listener setup remains unchanged and does not register Ringpop.
- Added a test-only typed probe example and Go overlay test. The probe is not a
  production responder and contains no SWIM or discovery logic.
- Documented the focused runner in `tests/README.md`.

## Final review and repair on TLS-enabled parent

Candidate `994a12bdec7f4ad1fcb047675e503fd25ccea45b` was cherry-picked onto
the current parent (`86b06d35a`, including enforced Matching HTTP TLS) as
`bbbfd299c`. The compatibility fix and final evidence are recorded in
the repair commit.

- Added a deadline around injected Ringpop handlers. Completed calls now time
  out at the TChannel call TTL; connection handling already aborts the task on a
  TChannel CANCEL frame. The Go probe verifies a 100 ms call against a 2 second
  handler returns an error.
- Extended the test-only probe and pinned Go JSON client check: default nil
  request headers arrive at the handler as `None`/JSON `null`, and an explicitly
  empty header map arrives as `Some({})`/JSON `{}`. The probe echoes that shape
  in arg3. `tchannel-go/json.Client.Call` reads response arg2 internally but
  does not expose it on its Context, so the compatibility assertion covers the
  request arg2 shape; server response encoding preserves `null` vs `{}` in code.
- The test-only probe does not add a production Ringpop responder. Normal
  frontend/Matching listener setup still passes no Ringpop handler, and
  `handle_call` rejects an unregistered `ringpop` service.

### Final validation

- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warmed t3-0a3d52d6 target> cargo test
  -p cadence-server --lib tchannel::tests`: 4 passed.
- Strict `cargo clippy -p cadence-server --all-targets -- -D warnings`: passed.
- `python3 tests/run-ringpop-tchannel-compat.py` with the rebuilt test-only
  example: passed against the actual pinned Go `tchannel-go/json.Client`,
  including join, ping, ping-req, app error, malformed JSON, unknown method,
  unknown service, nil/empty arg2, and handler deadline.
- `python3 tests/run-matching-thrift-compat.py` with the rebuilt server: all ten
  Matching Thrift methods passed on TChannel, HTTP, and HTTP trailing slash;
  enforced TLS checks passed.
- `git diff --check` passed. The matching pinned IDL sources were temporarily
  linked from the existing read-only worktree because this checkout's submodule
  is uninitialized. The temporary link was removed; `git submodule status`
  remains at pinned commit `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.
- No cargo/rustc process was active at final inspection. No target cleanup was
  performed in this round; the warmed target was 8.0 GB and filesystem free
  space was 12 GB afterward. Probe logs/configs remain under `/tmp/ringpop-review-*`.

## Earlier candidate evidence

- `CARGO_BUILD_JOBS=1 cargo test -p cadence-server --lib tchannel::tests
  --manifest-path rust/Cargo.toml`: passed, 4 tests.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-tchannel-compat.py`: passed with
  the actual pinned `tchannel-go/json.Client` against the Rust listener. Join,
  ping payload echo, ping-req typed response, decoded application error,
  malformed JSON, unknown method, and unknown service all passed. This run was
  before a final small refinement that preserved JSON arg2 `null` vs `{}` at the
  handler API; the Go client sends `null`, and final source compiles through
  all-target Clippy below.
- `python3 tests/run-matching-thrift-compat.py`: passed against the rebuilt Rust
  server. All ten native Matching Thrift methods passed over TChannel, HTTP,
  and HTTP trailing-slash routes.
- `CARGO_BUILD_JOBS=1 cargo clippy -p cadence-server --all-targets
  --manifest-path rust/Cargo.toml -- -D warnings`: passed on final source.
- `git diff --check`, Go `gofmt`, and Python syntax compilation passed; the
  generated Python cache directory was removed.

The workspace's `go/idls` submodule directory was empty. Its proto sources were
copied from the read-only prior checkout only after confirming both use pinned
submodule commit `bfbf273b41b9e36ce21e47a49600d22d73e4614c`; no git fetch/init or
prior-checkout mutation was performed.

## Remaining work

This slice is only the JSON transport boundary. Production Ringpop still needs
an actual provider/handler and SWIM implementation: join coordination and
membership validation, ping and ping-req probe behavior, incarnation and
checksum handling, membership change dissemination, state transitions and
tombstone normalization, labels/identity, bootstrap/discovery configuration,
provider lifecycle/health integration, and SDK compatibility tests against
real Go Ringpop nodes. Do not treat the test-only probe as production discovery.

## Outbound client task status (2026-10-18)

- Pinned `github.com/uber/tchannel-go@v1.34.4` was downloaded into the Go module cache and inspected at `/home/ubuntu/go/pkg/mod/github.com/uber/tchannel-go@v1.34.4/json/call.go` and `messages.go`. JSON calls encode arg2 and arg3 with `WriteJSON` (newline terminated), use transport `as=json`, and JSON application failures are response application-error bodies decoded as maps. `messages.go` confirms CALL_RES includes response code, tracing, and transport headers; continuation frames carry args only.
- Added an initial one-shot Rust outbound JSON client in `rust/crates/cadence-server/src/tchannel_json_client.rs`. Each call owns a TCP stream; dropping the future drops the socket. Current client has not yet received Go interoperability evidence and should be treated as incomplete pending tests, particularly fragmented response decoding and edge cases.
- Clippy was started in tmux session `cadence-rust`, window `outbound-json-review`, with `CARGO_BUILD_JOBS=1`; log `/tmp/cadence-outbound-json-clippy.log`, exit `/tmp/cadence-outbound-json-clippy.exit`. Do not claim completion until exit exists and is zero.
- Required follow-up remains: test-only Go JSON server and Rust runner; call-path protocol repairs as needed; Go evidence for null/empty request headers, fragmented response, application and framing errors, malformed JSON, unknown service/method, deadline and dropped-future connection; focused tests and inbound regressions; then record final exits and commit.

## Reusable outbound JSON call client (2026-10-18)

Implemented `cadence_server::tchannel_json_client::call` as a one-shot TCP
TChannel v2 JSON call client. It performs INIT/INIT_RES v2 framing, emits JSON
transport headers and newline-terminated JSON args, fragments request args,
assembles response fragments, validates NONE/IEEE/CRC32C response checksums,
decodes JSON response headers/body and application errors, decodes TChannel
ERROR frames, and applies a deadline. Dropping/canceling the call future drops
the owning TCP stream. This client is suitable as a reusable transport primitive
for the Matching Ringpop dependency; discovery/provider integration is not part
of this slice.

A test-only pinned-Go server is at `go/cmd/tchannel-json-test-server`; its runner
is `tests/run-tchannel-json-client-compat.py`. The helper serves echo (including
observed JSON headers), a 200 KB body, app error, malformed JSON, and a delayed
handler, plus unknown method/service failures. It exercises the actual Rust
client against `github.com/uber/tchannel-go v1.34.4`; this is not a self-only
codec test.

### Final evidence

- `CARGO_BUILD_JOBS=1 cargo clippy -p cadence-server --all-targets -- -D warnings`
  passed; final log `/tmp/cadence-outbound-json-final-clippy3.log`, exit
  `/tmp/cadence-outbound-json-final-clippy3.exit` (0).
- `CARGO_BUILD_JOBS=1 cargo test -p cadence-server --lib tchannel::tests`
  passed, 4 tests; final log `/tmp/cadence-outbound-json-inbound-test3.log`,
  exit `/tmp/cadence-outbound-json-inbound-test3.exit` (0).
- `CARGO_BUILD_JOBS=1 python3 tests/run-tchannel-json-client-compat.py` passed
  against the real Go server: null/empty arg2 headers, a 200 KB fragmented request and reply,
  decoded app error, malformed JSON response rejection, unknown service and
  method TChannel ERROR frames, 100 ms deadline, and dropped future/call socket.
  Log `/tmp/cadence-outbound-json-interop-final3.log`, exit
  `/tmp/cadence-outbound-json-interop-final3.exit` (0); retained server/helper
  artifacts `/tmp/tchannel-json-client-v4p3et1g`.
- Existing inbound regression `CARGO_BUILD_JOBS=1 python3
  tests/run-ringpop-tchannel-compat.py` passed with the pinned Go JSON client
  against the unchanged Rust inbound listener. Log
  `/tmp/cadence-outbound-json-inbound-go.log`, exit
  `/tmp/cadence-outbound-json-inbound-go.exit` (0), artifacts
  `/tmp/ringpop-tchannel-compat-t8wr176g`.
- Pinned v1.34.4 source was inspected in Go module cache, especially
  `json/call.go`, `messages.go`, and `fragmenting_reader.go`/`fragmenting_writer.go`.
  JSON `WriteJSON` newline behavior, init/call response headers, chunk boundary
  markers and continuation semantics informed the client.

### Transport boundaries

This is deliberately a one-call-per-connection primitive: it does not pool or
reuse connections, retry calls, resolve Ringpop peers, or send an explicit
TChannel CANCEL frame. Cancellation closes the socket and is tested against the
Go handler. It accepts typed `serde_json::Value` requests/responses and string
maps for JSON headers; raw arbitrary wire payload APIs and tracing propagation
are not exposed. INIT uses a placeholder local host port because the socket is
short-lived and the server accepts it; production provider integration may want
an advertised local identity. No SWIM join sender/member merge was added.

## Outbound JSON transport repair on latest parent (2026-10-18)

Applied candidate `b2441650ebf2f4c7224d7db55691e4c021f509ca` on latest local
parent `1878bcdfde3c4b83bfa8c983374316f3c5390cf6` as commit
`8bcc48481b279d26275498d17723cde0e120d5c0`. Decoder repair and adversarial
validation are in `595a3614e0d2b8a7451b615ce8c473d1b2c8796e`.

- CALL_RES fixed fields now use checked reads for flags, application code,
  tracing, transport-header count/strings, checksum type/value, and argument
  chunk lengths. No response indexing depends on unchecked offsets. Malformed
  continuation type/id/flags, unsupported checksum, checksum type changes,
  truncated chunks, missing final arguments, and excess arguments are errors;
  argument transitions no longer clamp excess chunks into arg3.
- The response decoder validates cumulative IEEE CRC32 and CRC32C checksums on
  every fragment. Tests run these checks over a mock TCP peer, including a
  corrupted checksum. Every fixed-header truncation prefix from 0 through 28
  bytes is exercised, along with a truncated header pair and malformed chunk.
- Application failures use an internal typed error wrapper; wire message text
  cannot make a transport error appear to be an application error.
- The deadline starts before connect. CALL TTL subtracts elapsed connect/INIT
  time; a delayed-INIT mock peer verifies the emitted TTL. INIT response id and
  ERROR frame id are validated.
- Dropping the future closes its owned TCP socket. The client sends no explicit
  TChannel CANCEL frame; remote cancellation therefore depends on peer
  disconnect handling. The real-Go cancellation probe verifies disconnect
  behavior for the test handler, not an explicit CANCEL protocol exchange.

Final evidence (all commands run on the repaired worktree):

- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-765b79ff/rust/target cargo test --locked -p cadence-server --lib tchannel_json_client::adversarial_tests -- --nocapture` — exit 0, 4 tests; log `/tmp/cadence-outbound-json-repair-focused.log`, exit `/tmp/cadence-outbound-json-repair-focused.final.exit`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-765b79ff/rust/target cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — exit 0; log `/tmp/cadence-outbound-json-repair-clippy.log`, exit `/tmp/cadence-outbound-json-repair-clippy.final.exit`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-tchannel-json-client-compat.py` — exit 0 against pinned tchannel-go v1.34.4; null/empty headers, 200 KB request/reply fragmentation, application error, malformed JSON, unknown method/service, deadline, and dropped-future disconnect. Log `/tmp/cadence-outbound-json-repair-interop.log`, exit `/tmp/cadence-outbound-json-repair-interop.exit`; runner artifacts `/tmp/tchannel-json-client-n48n9jjy`.
- `CARGO_BUILD_JOBS=1 python3 tests/run-ringpop-tchannel-compat.py` — exit 0, Go inbound regression. Log `/tmp/cadence-outbound-json-repair-inbound.log`, exit `/tmp/cadence-outbound-json-repair-inbound.exit`; artifacts `/tmp/ringpop-tchannel-compat-pwu12r20`.

The malformed-frame cases are Rust mock-peer tests; no differential oracle was
added for Go's exact rejection behavior on those malformed wire frames. This
repair only covers the outbound JSON transport primitive; it does not establish
Matching service completion, add discovery/SWIM sender behavior, or validate
service-level restart/recovery.

## Bounded PING-REQ inbound relay slice (2026-10-18)

Pinned source oracle: `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/ping_request_handler.go`, `ping_request_sender.go`, `ping_sender.go`, and `ping_handler.go`. `handlePingRequest` checks Ready first; it updates request changes, attempts `sendPing(target, node.pingTimeout)`, merges target response changes only on successful ping, issues response changes for the original requester, and returns `pingStatus` false on direct target failure. `sendPing` advances its sender dissemination callback only on successful call/JSON decode. The pinned default `pingTimeout` is 1500ms; outer requester timeout defaults to 5000ms and is outside this inbound handler slice.

Rust change: production `RingpopNode::handler` now dispatches `/protocol/ping-req`; its async relay releases the state mutex across direct `/protocol/ping` I/O, applies request/target memberlist updates and dissemination accounting, then serializes `PingResponse`. Added `ping_req` request count. Focused production-handler relay test covers a successful direct TChannel target. Rust node configuration still has no ping-timeout field, so this slice uses pinned default 1500ms (config extraction remains pending). Reverse full-sync and full bootstrap/readiness remain outside this slice.

Evidence so far: `CARGO_BUILD_JOBS=1 cargo check -p cadence-server --locked` in `rust/` passed after fixing a borrow conflict. `rustfmt --edition 2021 --check rust/crates/cadence-server/src/ringpop_provider.rs` and `git diff --check` passed. A focused lib test build/run is underway; record its final exit before relying on this section. Workspace-wide `cargo fmt --check` is not a usable gate because it reports extensive pre-existing unrelated formatting differences; no workspace formatting was applied. No mixed Go/Rust three-node probe or strict locked Clippy run has been completed yet; Matching remains incomplete.
- Focused test completed: `CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-server --lib production_ping_req_handler_relays_direct_ping_and_returns_ping_response -- --nocapture` — exit 0, 1 passed; command run from `rust/`.
- `CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-server --all-targets -- -D warnings` — exit 0.
