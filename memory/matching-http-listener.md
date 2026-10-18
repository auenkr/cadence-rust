# Dedicated Matching HTTP listener (2026-10-17)

## Follow-up correction (2026-10-18)

The earlier claim that fragmented TLS ClientHello records should successfully
handshake was an unauthorized behavior change. Go YARPC v1.88.0's
`transport/internal/tls/muxlistener/tls_checker.go:isTLSClientHelloRecord`
calls `Read` exactly once into a five-byte buffer. A short, non-error read is
classified as plaintext. Rust now does one bounded read and uses that same
classification, preserving the consumed bytes for the HTTP path. A read error
or a 15 second sniff timeout closes the connection. TLS handshakes remain
concurrent with a 120 second timeout, so a stalled peer does not block accepts.

The candidate's `MAX_PENDING=128` / `try_acquire_owned` rejection behavior had
no matching Go bound: YARPC's muxlistener starts one goroutine per accepted
connection and tracks them with a wait group. Rust therefore has no invented
pending-connection cap and uses independently spawned tasks. Dropping the
listener's `JoinSet` aborts its connection tasks on listener cancellation.

The compatibility harness's fragmented ClientHello case now forces the first
read to be one byte and checks the plaintext response path. This replaces the
prior Rust-only success expectation. The Rust production timeouts remain 15s
sniff / 120s handshake; the connection test's independent probe verifies that
a stalled handshake cannot cause accept head-of-line blocking. Earlier listener
test evidence and the ten-method Go runner still apply, but those checks do not
constitute Matching service completion.

### Correction validation record

- `cargo test --locked -p cadence-server matching_http_listener_tests -- --nocapture` (from `rust/`, `CARGO_BUILD_JOBS=1`, this worktree's `rust/target`): exit 0; 3 listener tests passed. Full output is in `memory/http-parity-cargo.log`.
- `cargo clippy --locked -p cadence-server --all-targets -- -D warnings`: exit 0.
- `cargo build --locked -p cadence-server`: exit 0.
- `python3 tests/run-matching-thrift-compat.py --server-bin rust/target/debug/cadence-server`: exit 0. It first ran Go YARPC v1.88.0's `TestIsTLSClientHelloRecord` (including short-buffer/read-error cases); then all ten generated Go methods passed via TChannel, HTTP, and HTTP trailing slash, plus fragmented-short-read plaintext classification, TLS/plaintext, timeout/recovery, listener shutdown cancellation, and configuration mode probes. Output: `memory/http-parity-mixed-go.log`.
- Final rerun after adding the 129-silent-connection pending-load probe: exit 0; all checks above passed against the rebuilt binary, including service progress while 129 connections were pending. Output: `memory/http-parity-final-go.log`, exit in `.exit`.
- `cargo test --locked -p cadence-server --bin cadence-server tls_handshake_timeout_is_injectable -- --nocapture`: exit 0. The paused-time test injects a 10 ms timeout and verifies an incomplete TLS handshake returns `TimedOut`; full test, strict Clippy, and locked build output is `memory/http-timeout-cargo.log`, exit in `.exit`.
- `cargo build --locked -p cadence-server --example ringpop_tchannel_probe --manifest-path rust/Cargo.toml` and `python3 tests/run-ringpop-tchannel-compat.py --server-example rust/target/debug/examples/ringpop_tchannel_probe`: both exit 0; original Go Ringpop TChannel compatibility passed. Same log as above.
- First compatibility attempt passed the Go YARPC oracle and ten-method runner but the shutdown probe expected FIN and observed TCP RST (exit 1). The harness now accepts EOF or reset as connection closure; first-attempt evidence is retained in `memory/http-parity-mixed-go-attempt1.log` and `.exit`.
- An initial build using a shared warm target was interrupted when another task began using it (exit 130); the successful checks above used the isolated worktree target. The interruption log is retained as `memory/http-parity-cargo-shared-interrupted.log` and `.exit`.

These checks validate this HTTP transport slice only. They do not complete Matching or its remaining service parity audit.

## Go behavior inspected

- `go/common/config/config.go`: `rpc.http` is optional and includes port, procedure allowlist, TLS config, and TLS mode.
- `go/common/rpc/params.go`: a configured HTTP transport requires `port > 0`, binds on the RPC service's listen IP, and builds a procedure set. It reports `HTTP port is not set` for a missing/zero port. Bind IP policy is localhost, explicit `bindOnIP`, then auto-detected `ListenIP`; localhost plus explicit IP is rejected.
- `go/common/rpc/factory.go`: each request is served only if its `Rpc-Procedure` header is in the configured set. Configured HTTP TLS is passed to YARPC's TLS inbound.

## Implementation in this slice

- Added optional Go-shaped Matching HTTP RPC config parsing.
- A configured positive port creates a dedicated Axum listener on the selected bind address. Configured procedures are enforced as an allowlist; an empty list rejects all calls, matching Go's map lookup behavior.
- With the dedicated transport configured, Matching routes are not mounted on the JSON listener and its generic YARPC catch-all rejects MatchingService/Meta procedures too. A real listener probe found that the catch-all initially still dispatched Matching requests; it now blocks those methods whenever the dedicated listener is configured. Without `rpc.http`, the prior shared listener behavior remains.
- Matching service is cloned for gRPC and HTTP listener futures, avoiding moving the service into one future while borrowing it in another.
- If HTTP TLS is enabled with `TLSMode: enforced`, Matching loads the configured certificate and key and serves that listener through Rustls; the HTTP listener branch cannot fall back to plaintext. Missing/invalid certificate or key files fail startup with a contextual error. If TLS is disabled, the existing plaintext listener remains in use.
- TLS configuration is now typed with Go-shaped fields (`enabled`, `certFile`, `keyFile`, `caFile`, `caFiles`, `enableHostVerification`, `requireClientAuth`, `serverName`, `sslmode`). This slice explicitly rejects enabled TLS modes other than `enforced`, and rejects configured CA trust, client authentication, host verification, server name, and `sslmode` options rather than silently ignoring them.
- Go/YARPC v1.88.0 `api/transport/tls.Mode.UnmarshalText` accepts only exact lowercase `disabled`, `permissive`, and `enforced`; matching startup now rejects any other spelling. Go's RPC factory only passes an HTTP TLS mode to YARPC when `TLS.Enabled` is true. Rust additionally rejects `TLSMode: enforced` when TLS is absent or disabled, to avoid a config that asserts enforced transport but would otherwise open plaintext.
- The configured listener uses `bindOnLocalHost`, explicit `bindOnIP`, then auto-detected IP selection. This implements address selection but does not inspect interface UP flags.

## Validation

- Added focused address/bind/TLS tests in the server binary test module and a router-level procedure allowlist test.
- Initialized `go/idls` at pinned revision `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.
- `cargo test -p cadence-server matching_http -- --nocapture`: previously passed (2 listener configuration tests; other targets filtered).
- `cargo test -p cadence-server dedicated_http_listener_applies_configured_procedure_allowlist -- --nocapture`: passed (router test; other targets filtered).
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warm prior-worktree target> CARGO_PROFILE_DEV_DEBUG=0 cargo test -p cadence-server dedicated_http_listener_enforces_tls_handshake -- --nocapture`: passed. The test uses a generated local certificate and a trusted Rustls client, checks HTTP 200 for allowlisted `Meta::health` over TLS, and checks that plaintext receives a TLS alert. The build required initializing `go/idls` at the pinned revision `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warm prior-worktree target> CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p cadence-server --all-targets -- -D warnings`: passed with no warnings.
- Earlier real listener evidence on the current Rust server: `tests/run-matching-thrift-compat.py` starts with a generated YAML config and distinct JSON/Matching HTTP ports, then the original Go-generated Matching clients pass all ten methods through TChannel and both HTTP path spellings. It confirms allowlisted `Meta::health` succeeds, unconfigured `WorkflowService::ListDomains` is rejected by the configured allowlist, and `MatchingService::ListTaskListPartitions` is rejected on the JSON listener with both `cadence-matching` and `cadence-frontend` service headers. The runner’s prior check that enabled TLS fails startup has been superseded by the enforced-TLS handshake test above.
- `tests/check-service-listeners.py <cadence-server>` passed for Matching and History with the development config, where Matching HTTP is disabled; this checks the disabled HTTP configuration still starts its normal isolated listeners and does not expose frontend JSON routes on service-only mode.
- Focused tests passed: `cargo test -p cadence-server --bin cadence-server matching_http_listener_tests -- --nocapture` (2) and `cargo test -p cadence-server --lib dedicated_http_listener_applies_configured_procedure_allowlist -- --nocapture` (1).
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warm prior-worktree target> CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p cadence-server --all-targets -- -D warnings`: passed.
- Follow-up validation: `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warm prior-worktree target> CARGO_PROFILE_DEV_DEBUG=0 cargo test -p cadence-server --bin cadence-server matching_http_listener_tests -- --nocapture` passed all 3 listener tests, including exact mode parsing, missing/disabled TLS with enforced mode, unsupported permissive mode, required files, and invalid PEM. Strict `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=<warm prior-worktree target> CARGO_PROFILE_DEV_DEBUG=0 cargo clippy -p cadence-server --all-targets -- -D warnings` passed.
- `python3 tests/run-matching-thrift-compat.py --server-bin <built cadence-server>` passed against the production config path. The existing Go-generated suite passed all ten methods via TChannel, HTTP, and HTTP trailing slash; allowlist and JSON listener isolation checks passed. Startup rejected enforced mode without TLS, a missing certificate file, malformed PEM, and a mismatched key. With a generated certificate, the configured Matching listener served `Meta::health` through the Go YARPC HTTP client with certificate validation enabled (`RootCAs` and `ServerName=localhost`), and a plaintext probe received a TLS alert. No CA/client-auth/host-verification/permissive parity is implied.

## Remaining parity gaps

- Auto-selection mirrors `go/common/rpc/localip.go` address scoring and first-highest tie behavior. `if-addrs` does not expose interface UP flags, so configured non-loopback addresses are treated as UP; UP/down-interface scoring remains a parity gap.
- TLS parity remains partial. Implemented here: default/disabled, permissive mixed TLS/plaintext, and enforced TLS-only server behavior, plus certificate/key handling and rejection of unsupported configured client-auth/CA/verification settings. Not implemented here: Go CA file trust behavior, mutual TLS/client authentication, and host verification/server-name config. Go builds HTTP TLS only when `HTTP.TLS.Enabled`, then passes the configured mode. No broad TLS parity claim is made.
- This slice does not complete Matching service replacement or any Matching migration gate.

## YARPC HTTP TLS mode parity (2026-10-18)

Recovered and completed the interrupted `http-permissive-standard-10` draft
(`58637404a` in sibling worktree `t3-cf940e1b`). Matching HTTP now follows
YARPC v1.88.0 mode behavior when `rpc.http.tls.enabled` is true:

- Missing `TLSMode` defaults to `disabled`; `disabled` serves plaintext only.
- `permissive` sniffs the initial TLS record and accepts TLS and plaintext on
  the same bound port.
- `enforced` accepts TLS only and rejects plaintext during the TLS handshake.
- Exact lowercase values are accepted; unknown/case-variant values fail
  startup. If TLS is absent or disabled, YARPC does not pass its mode/config to
  the HTTP inbound, so the listener remains plaintext even with an `enforced`
  field present.
- TLS cert/key loading follows Go's pair semantics. When both are provided,
  invalid or mismatched PEM fails startup; if either is omitted, startup is
  allowed and TLS handshakes have no server certificate. CA/client-auth and
  host-verification options remain explicitly unsupported in Rust.

Evidence from this worktree, using the warmed target at
`/home/ubuntu/.t3/worktrees/cadence-rust/t3-0a3d52d6/rust/target` and the pinned
Go IDL submodule `bfbf273b41b9e36ce21e47a49600d22d73e4614c`:

- `cargo test -p cadence-server --bin cadence-server matching_http_listener_tests -- --nocapture`: 3 passed.
- `cargo clippy -p cadence-server --all-targets -- -D warnings`: passed.
- `python3 tests/run-matching-thrift-compat.py --server-bin <built cadence-server>`: passed. The Go-generated suite exercised all ten Matching methods over plaintext and trusted TLS on the same permissive listener, plus TChannel and both HTTP path spellings. It also checked enforced plaintext rejection, default/disabled plaintext with `tls.enabled`, invalid TLS modes and cert/key files, procedure allowlisting, and JSON listener isolation. Artifacts: `/tmp/matching-thrift-compat-qkes1qi4`; runner log: `/tmp/cadence-permissive-final-probe.log`.

This closes only Matching HTTP TLS mode behavior. Advanced TLS configuration,
interface-UP selection and the broader Matching/service migration remain open.

## Fragmented and concurrent permissive accepts (2026-10-18)

Repairing candidate `30805ee32f58c467d87ff74a10b9e333e8233650` on the latest
Ringpop parent found that its single `TcpStream::peek` rejected a ClientHello
when fewer than five bytes were available in the first read, and performed
both sniffing and TLS negotiation inside `Listener::accept`, serializing every
new client behind a silent or slow peer. The repaired listener accumulates the
five byte record prefix under YARPC's 15 second sniff deadline, replays those
bytes through a prefix stream, and runs sniff/handshake work in an owned
`JoinSet` capped at 128 in-flight sockets. TLS negotiation is bounded at the
YARPC v1.88.0 deadline of 120 seconds. Dropping the listener drops the
`JoinSet`, aborting outstanding connection tasks; timed out and failed
connections are dropped without blocking subsequent accepts.

Pinned source references under `$GOPATH/pkg/mod/go.uber.org/yarpc@v1.88.0`:

- `transport/internal/tls/muxlistener/tls_checker.go` checks five bytes for
  content type `22`, major version `3`, minor version `>= 1`.
- `transport/internal/tls/muxlistener/listener.go` sets 15 seconds for sniffing
  and 120 seconds for TLS handshake; its `serve` accepts sockets continuously
  and dispatches each to a goroutine. Its `matchTLSConnection` applies the
  sniff read deadline and clears it afterward.
- `transport/internal/tls/muxlistener/conn_sniffer.go` buffers consumed sniff
  bytes and replays them to the selected protocol handler.
- Note: YARPC's `isTLSClientHelloRecord` performs one `Read`, so a short read is
  treated as plaintext. The repaired Rust listener intentionally waits for all
  five bytes to satisfy the required byte-fragmented ClientHello behavior while
  retaining YARPC's timeout, prefix replay, and classification predicate.

TLS config behavior was checked against `go/common/rpc/params.go` and
`go/common/config/tls.go`: `TLS.Enabled` gates `ToTLSConfig` and the mode is
only forwarded when enabled; disabled mode is a raw plaintext listener;
`ToTLSConfig` loads a certificate only when both cert and key are nonempty,
otherwise startup succeeds and TLS handshakes have no server certificate.
When both paths are present, read/parse/mismatch errors are returned during
startup. Therefore an `enforced` field without enabled TLS still results in
plaintext, and enabled TLS with incomplete cert/key does not fail startup.

Bound validation in `tests/run-matching-thrift-compat.py` covers a
one-byte-at-a-time TLS ClientHello with a successful `Meta::health` request,
plaintext and TLS clients behind an initially silent peer and behind a stalled
TLS handshake, recovery after peer close/handshake errors, and closure/recovery
after the 15 second sniff timeout. These live checks now passed on the repaired
binary; earlier stopped-build notes below are superseded by the final run.

Final repair verification in the isolated checkout at `t3-b9747ee8`, starting
from parent `b2143c8ca` and recovering the permissive-mode changes from
`30805ee32` plus the uncommitted repair diff from sibling worktree `t3-54eb476d`:

- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-b9747ee8/rust/target cargo test -p cadence-server --bin cadence-server matching_http_listener_tests -- --nocapture`: exit 0, 3 passed. Log: `/tmp/http-repair-focused.log`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-b9747ee8/rust/target cargo build --locked -p cadence-server`: exit 0. Log: `/tmp/http-repair-build.log`.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-b9747ee8/rust/target cargo clippy --locked -p cadence-server --all-targets -- -D warnings`: exit 0. Log: `/tmp/http-repair-clippy.log`.
- `python3 tests/run-matching-thrift-compat.py --server-bin rust/target/debug/cadence-server`: exit 0. Log: `/tmp/http-repair-compat.log`; generated server configs, logs and certificates: `/tmp/matching-thrift-compat-7atapa07`.
- The live runner passed all ten Go-generated Matching methods on the permissive listener over plaintext HTTP and trusted TLS HTTP, plus TChannel and HTTP trailing-slash paths. It also passed default/disabled plaintext behavior, enforced TLS plaintext rejection, allowlist and JSON-route isolation, cert/key startup rejection, fragmented ClientHello, simultaneous plaintext and TLS service behind silent/stalled connections, sniff timeout closure and post-timeout recovery.
- The runner verifies concurrent service behind stalled handshakes and the 15 second sniff timeout, but does not wait 120 seconds to observe the TLS handshake deadline or explicitly cancel the listener while handshakes are pending. The 120 second deadline and `JoinSet` drop-abort lifecycle are implemented and reviewed; direct runtime assertions for those two lifecycle events remain open.
- All four exit marker files (`/tmp/http-repair-focused.exit`, `/tmp/http-repair-build.exit`, `/tmp/http-repair-clippy.exit`, `/tmp/http-repair-compat.exit`) contain `0`. `tokio-rustls 0.26.6` was already represented in `rust/Cargo.lock`; no lockfile update was required.

The cancelled task's T3 thread read returned `thread_not_found`; recovery used
the extant worktree, its uncommitted diff and this memory file. No source was
modified in the sibling or parent worktrees. No push was performed.
