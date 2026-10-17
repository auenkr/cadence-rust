# Dedicated Matching HTTP listener (2026-10-17)

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
- TLS parity remains partial. Implemented here: enabled + enforced server TLS with certificate/key loading and TLS-only listener, plus rejection of unsupported configured client-auth/CA/verification settings and unsupported enabled modes. Not implemented here: Go CA file trust behavior, mutual TLS/client authentication, host verification/server-name config, and permissive mode. Go builds HTTP TLS only when `HTTP.TLS.Enabled`, then passes the configured mode. No broad TLS parity claim is made.
- This slice does not complete Matching service replacement or any Matching migration gate.
