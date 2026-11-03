# Frontend header forwarding (remote frontend) + global ratelimiter parity + gate (2026-11-04)

Branch `t3/frontend-forwarding-rl-gate` (not merged). Validated code head: 1aa726ce6b73539d9973f1451c31c760017d4888.

## Changes
- Header forwarding (`admin.HeaderForwardingRules` / static `headerForwardingRules`) now applies to RemoteFrontendClient calls (TChannel and gRPC) like Go's outbound middleware on every dispatcher; existing outbound headers (forwarding cluster, auth, rpc-*) are never overwritten. Unit tests in frontend_remote.rs and frontend_remote_grpc.rs.
- global_ratelimit.rs: replaced Allow-style consumption by Go reservation semantics: tokens taken at Reserve, `Used(false)` returns them, shadowedReservation (shadow Used(true) when primary rejected, else forwards wasUsed), countedReservation usage accounting, FallbackLimiter both() order (starting/failing/normal). quota_policy.rs holds collection reservations across later stages and releases them with the final result (Go MultiStageRateLimiter.Allow defers).
- Gate (run-frontend-gate.py): modes global, local-shadow-global, global-shadow-local each serve requests at a high limit and return ServiceBusy at limit 1. Note development.yaml already sets `frontend.globalRatelimiterMode: local`; the gate strips it before overriding (first attempt failed because of the duplicate key).

## Not done
- Header forwarding is not observable on the two_cluster gate (remote side is Go; CLI cannot inject arbitrary headers); covered by unit tests only.

## Evidence
- rust-ci.yml success (head 93378500): https://github.com/auenkr/cadence-rust/actions/runs/41700326489 (earlier failures: compile 41698574830, rustfmt 41699390866)
- frontend-mixed-gate mysql kafka two_history success (head 1aa726ce): https://github.com/auenkr/cadence-rust/actions/runs/41704397761 ; SHA256 d2ccd1911d19b9ca036321ffabf763248c25af3f47cc7f269d2371feaed4ab39 ; all 6 global-ratelimiter steps ok. (Failed earlier run 41701868449: gate duplicate-key bug.)
- frontend-mixed-gate postgres two_cluster success (head 1aa726ce): https://github.com/auenkr/cadence-rust/actions/runs/41706790692 ; SHA256 ec6924db48bbab4ead213d19f50085a61b259f3bec2e13a18b371a8fb83eecee
- Note: rust-ci ran on 93378500 (code identical to 1aa726ce; the last commit only changes tests/gates/*.py).
