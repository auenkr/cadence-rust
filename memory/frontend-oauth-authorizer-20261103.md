# Frontend oauthAuthorizer port (2026-11-03)

Branch: t3/frontend-authorization-cluster-redirection

Ported (Go common/authorization/oauthAuthorizer.go + factory.go):
- Config `authorization.{oauthAuthorizer,noopAuthorizer}` parsed in cadence-service-core config; validation errors mirror Go.
- RS256 JWT verification with PKIX public key file; internal-token path (no kid, or iss=internal-jwt); exp/nbf/iat checks, ttl claim -> exp, maxJwtTTL; admin claim allows; groups vs domain data READ_GROUPS/WRITE_GROUPS/PROCESS_GROUPS; domain lookup error returned (Unknown); deny -> AccessDenied "Request unauthorized.".
- Token read from `cadence-authorization` header (lower-cased transport headers, so TChannel and gRPC alike) via RequestAuthorization::authorize_with_headers.
- Selection in workflow_backend.rs build_workflow_api: oauth when enabled, else nop.
- Unit tests in oauth_authorizer.rs with an RSA keypair in rust/crates/cadence-frontend/testdata (generated with openssl).

NOT ported / gaps (audit rows stay open):
- oauthAuthorizer.provider (JWKS + jmespath groups/admin paths): rejected at startup with an explicit error.
- Admin API authorization (Go NewAdminHandler, PermissionAdmin on 39 methods): admin dispatch lives in cadence-service-core yarpc.rs/admin and is not wrapped yet.
- No mixed gate with oauth enabled (Go Worker JWT provider config not set up); not run. Unit tests only.
- Part 2 (gRPC remote cluster client, schedule resolver, strong-consistency header) not started.
- Formatting of the 3 touched files was done with standalone rustfmt (not cargo) after CI fmt diff; Cargo.lock not committed by me.

CI evidence: rust-ci (package=cadence-frontend) https://github.com/auenkr/cadence-rust/actions/runs/41584179907 conclusion success at head d181bc0bedd93777b293f8e8c432fb5c37ef55a8 (fmt+clippy+tests). Earlier runs 41583447308 (compile errors), 41583815107 (fmt) failed. Full-workspace run not done. No binary SHA256 (no gate).

## Update: provider mode, Admin API wrapper, oauth gate phase (branch t3/frontend-oauth-admin)

Ported:
- `oauthAuthorizer.provider` (JWKS fetched once at startup via blocking reqwest, no refresh; jmespath groups/admin attribute paths). Internal tokens still verify with `jwtCredentials.publicKey`; external tokens use the JWKS key by `kid`.
- Admin API access control (Go `accesscontrolled.NewAdminHandler`): `AdminAuthorizer` seam in cadence-service-core `workflow_api.rs` (no crate cycle). Admin permission for every method except DescribeCluster (Read), no domain, error "Request unauthorized.". TChannel: `yarpc.rs dispatch_with_headers` (tchannel.rs now passes `AdminService::<Method>` for AdminService calls; the bare name previously skipped the check). gRPC: `AdminAuth` tower wrapper in cadence-matching `grpc.rs`.
- Legacy domain APIs (DescribeDomain Read/domain=name, FailoverDomain Write/domain=domainName, Register/Update/Deprecate/DeleteDomain Admin) were bypassing authorization because they are served by legacy handlers, not `workflow_api`. Now checked via `authorize_domain_api` on TChannel (yarpc.rs) and in each gRPC DomainApi handler before validation. ListDomains stays unauthenticated as in Go.
- Gate: oauth phase in tests/gates/run-frontend-gate.py, run after the normal phase because the Go Worker/History cannot send a JWT to the Rust Frontend. Rust Frontend restarted with oauth enabled and a gate-generated RSA key; JWTs minted by the script. Steps `oauth-{tchannel,grpc}-*` cover missing/garbage/expired/wrong-key denied, admin allowed, reader describe allowed / write denied, Admin API no-token/expired/non-admin denied and admin allowed.

Gaps: JWKS has no refresh; `RequestBody` attribute is not populated for the domain/Admin checks (OAuth ignores it); provider-mode JWKS path is covered by unit tests only, not the gate.

CI evidence (head 81f54af1480b12f8d603d00903cb7fe43b0ef659):
- rust-ci package=cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/41594403425 success
- frontend-mixed-gate (mysql, kafka=false): https://github.com/auenkr/cadence-rust/actions/runs/41594417872 success, all oauth steps ok
- Rust binary SHA256 f340e8c0b836378a5bfa4ea58d45d148e3164ea26a8e75834e1dc335b75a470d (cadence-server)
- Earlier: rust-ci 41592413541 success (ba9cad1); gate 41593096606 failed on 11 oauth steps (TChannel Admin + DescribeDomain unauthenticated), fixed here.
- Full-workspace rust-ci (fmt+clippy+workspace tests) at f84c3e95: https://github.com/auenkr/cadence-rust/actions/runs/41595554763 success
