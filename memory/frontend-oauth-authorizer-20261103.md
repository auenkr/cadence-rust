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
