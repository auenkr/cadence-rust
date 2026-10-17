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

## Evidence

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
