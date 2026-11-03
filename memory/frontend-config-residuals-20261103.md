# Frontend config residuals (2026-11-03)

Branch: `t3/frontend-config-residuals`. Validated head: 15bab12a5d96c8abbc467e7cf35d3288a1987a31 (not merged).

## Ported
- Gap 1: Go `common/quotas/global` limiting-host side in `cadence-frontend/src/global_ratelimit.rs`: key modes (disabled/local/global/local-shadow-global/global-shadow-local; unknown = disabled), FallbackLimiter (startup/failing, 9 failed updates), CountedLimiter, `boostRPS`, background update loop + GC, Thrift `WeightedRatelimitUsage`/`...UsageQuotas` in `Any`, History-ring aggregator client (`RatelimitUpdate`, keys grouped per History peer). Wired for user/worker/visibility/async plus userTaskList/workerTaskList/asyncTaskList collections via `TokenBucketPolicy::with_global_collections`; mode key `frontend.globalRatelimiterMode` (ratelimitKey filter, default `disabled`), interval `frontend.globalRatelimiterUpdateInterval` (3s). `frontend.globalRPS`/`globalDomainRPS` now divide by the live Frontend ring member count (was fixed at 1).
- Gap 2: `admin.HeaderForwardingRules` (dynamic; default = all except `^grpc-.*`; falls back to static `headerForwardingRules` when empty) applied to outbound History/Matching Thrift calls via task-local inbound headers (`header_forwarding::INBOUND_HEADERS`).
- Gap 3: Go's `PublicClient` is only consumed by the Worker service (`cmd/server/cadence/server.go` -> worker params), not by Frontend handlers; nothing to wire in Frontend (config parse already present).
- Gap 4: `string_list_lenient` (Go CallerBypass per-element semantics) for `system.rateLimiterBypassCallerTypes`; `string_ratelimit_key`; `json_list`.
- Gap 5: SQL/Cassandra visibility unsupported query constructs return Go `BadRequestError{"Operation is not supported"}`.

## Not done / limitations
- No new gate steps in `tests/gates/run-frontend-gate.py` (modes default to disabled; aggregator path covered by unit tests only, not exercised end-to-end).
- Remote-frontend (cross-cluster redirect) calls do not yet apply header forwarding rules; only History/Matching Thrift calls do.
- Aggregation itself (History side) is not part of this change.
- Limiter shadow semantics are approximations of Go's `both()`; token consumption order across stages differs slightly.

## Evidence
- rust-ci.yml package=cadence-frontend success: https://github.com/auenkr/cadence-rust/actions/runs/41691289157
- rust-ci.yml full workspace success (head 15bab12a5): https://github.com/auenkr/cadence-rust/actions/runs/41692614912
- frontend-mixed-gate.yml mysql kafka=true two_history=true success: https://github.com/auenkr/cadence-rust/actions/runs/41694175545 ; Rust cadence-server SHA256 c525f5867745bc1d67a6e7466639493991cfe8c8acc04138cb0d9f51fb778678
- frontend-mixed-gate.yml postgres two_cluster=true success: https://github.com/auenkr/cadence-rust/actions/runs/41696285222 ; Rust cadence-server SHA256 9a997168a16af24d4ee02f5a51d912c982cef4d09969dff30fb6a3d64f11f2b2
- Earlier failed run: 41690495132 (rustfmt only, fixed).
