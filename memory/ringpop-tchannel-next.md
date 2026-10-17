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
