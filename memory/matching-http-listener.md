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
- If HTTP TLS `enabled` is true, startup fails explicitly because this Rust inbound has no TLS termination. It never silently serves plaintext.
- The configured listener uses `bindOnLocalHost`, explicit `bindOnIP`, then auto-detected IP selection. This implements address selection but does not inspect interface UP flags.

## Validation

- Added focused address/bind/TLS tests in the server binary test module and a router-level procedure allowlist test.
- Initialized `go/idls` at pinned revision `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.
- `cargo test -p cadence-server matching_http -- --nocapture`: passed (2 listener configuration tests; other targets filtered).
- `cargo test -p cadence-server dedicated_http_listener_applies_configured_procedure_allowlist -- --nocapture`: passed (router test; other targets filtered).
- `cargo clippy -p cadence-server --all-targets -- -D warnings`: passed with no warnings.
- Real listener evidence on the current Rust server: `tests/run-matching-thrift-compat.py` starts with a generated YAML config and distinct JSON/Matching HTTP ports, then the original Go-generated Matching clients pass all ten methods through TChannel and both HTTP path spellings. It confirms allowlisted `Meta::health` succeeds, unconfigured `WorkflowService::ListDomains` is rejected by the configured allowlist, and `MatchingService::ListTaskListPartitions` is rejected on the JSON listener with both `cadence-matching` and `cadence-frontend` service headers. The runner also verifies enabled TLS fails startup with the explicit unsupported-inbound-TLS error.
- `tests/check-service-listeners.py <cadence-server>` passed for Matching and History with the development config, where Matching HTTP is disabled; this checks the disabled HTTP configuration still starts its normal isolated listeners and does not expose frontend JSON routes on service-only mode.
- Focused tests passed: `cargo test -p cadence-server --bin cadence-server matching_http_listener_tests -- --nocapture` (2) and `cargo test -p cadence-server --lib dedicated_http_listener_applies_configured_procedure_allowlist -- --nocapture` (1).
- Strict server lint passed: `CARGO_BUILD_JOBS=1 cargo clippy -p cadence-server --all-targets -- -D warnings`. The preserved timeout draft had one unused test helper; an `allow(dead_code)` annotation documents that it is intentionally staged for its follow-up test.

## Remaining parity gaps

- Auto-selection mirrors `go/common/rpc/localip.go` address scoring and first-highest tie behavior. `if-addrs` does not expose interface UP flags, so configured non-loopback addresses are treated as UP; UP/down-interface scoring remains a parity gap.
- TLS remains unfinished. Go `config.TLS.ToTLSConfig` reads certificate/key pairs, CA file(s), server name, host verification, and client-auth settings. YARPC v1.88.0 defines `disabled`, `permissive` (TLS and plaintext), and `enforced` (TLS only); the inbound installs TLS handling only for non-disabled mode and errors when mode requires TLS but config is missing. Go builds HTTP TLS only when `HTTP.TLS.Enabled`, then passes the mode. Rust currently parses TLS as untyped YAML, does not consume `tls_mode`, and rejects enabled TLS to prevent plaintext fallback. No TLS mode or handshake parity is claimed.
- This slice does not complete Matching service replacement or any Matching migration gate.
