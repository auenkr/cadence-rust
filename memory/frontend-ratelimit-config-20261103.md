# Frontend rate limiting + static config (2026-11-03)

Branch: `t3/frontend-ratelimit-config-w2` (the name `t3/frontend-ratelimit-config` is held by another worktree). Head: 027e6fa04b3558cd7c7431a718e2bef420c95b5e (master merged, validator-config included; no `limit.*` duplication).

## Ported
- Quota stages (Go `MultiStageRateLimiter`): task-list -> domain -> global; `PerMember` (global/members capped by instance; members fixed at 1). Keys: `frontend.rps/domainrps/globalDomainrps/tasklistrps/globalTaskListrps`, worker/visibility/async variants.
- Bypass: `system.rateLimiterBypassCallerTypes` + `cadence-caller-type` header (`allow_with_headers`).
- ServiceBusy message "Too many outstanding requests to the cadence service".
- `rpc.http` Frontend inbound (YARPC thrift over HTTP, `procedures` allowlist, TLS modes) in `cadence-matching/src/launcher.rs`; `yarpc_http_router` in service-core.
- Config parse: `publicClient`, `headerForwardingRules` (+ `header_forwarding.rs` helper, tests).

## Not ported / limitations
- Global rate-limiter mode / aggregator client; `string_list` differs slightly from Go ListProperty.
- `publicClient` parse only; `headerForwardingRules` helper not wired to outbound calls.
- kafka/blobstore not touched here.

## Evidence
- rust-ci.yml (full workspace) success: https://github.com/auenkr/cadence-rust/actions/runs/41682921803 (head a1071e97c)
- frontend-mixed-gate.yml mysql, rust_ref=branch, success: https://github.com/auenkr/cadence-rust/actions/runs/41686994489 (head 027e6fa04)
- Own gate steps ok: ratelimit-http-config-generated, ratelimit-http-restart, rpc-http-allowed-procedure (GetSearchAttributes), rpc-http-disallowed-procedure-rejected (404), ratelimit-frontend-rps-service-busy (frontend.rps=1).
- Rust cadence-server SHA256: 4dee4c5341ee24d2440e7ad096885a08f796874d9808c28ddc21463169ca67a1
- Earlier failed runs (fixed): 41680791336 compile (shadowed helper), 41681418753/41682043373 rustfmt, 41684898566 gate (Health not a frontend procedure; DescribeDomain not limited).
