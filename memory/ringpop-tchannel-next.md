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
