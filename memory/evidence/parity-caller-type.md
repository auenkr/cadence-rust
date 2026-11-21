# Parity: caller-type propagation (`cadence-caller-type`)

Branch `parity-caller-type`. Gated SHA `ba08574686c8328d132315ee4a06afaaf6e655dd`.

## Go map
- `types.CallerInfo` in ctx, header `cadence-caller-type` (missing/empty => `unknown`).
- Inbound: Frontend, Matching and History YARPC (gRPC + TChannel) middleware.
- `quotas.CallerBypass.AllowLimiter`: limiter token consumed first; a denied call still passes when its caller type is in `system.rateLimiterBypassCallerTypes` (default empty). Used by persistence rate limiters.
- Outbound: `HeaderForwardingMiddleware` default rules (forward all but `grpc-*`); `internal` when outside any inbound request.

## Rust design
- `cadence-persistence/src/caller_type.rs`: tokio task-local (value + header-present flag), `scope_*`, `should_bypass`, `outbound_header`.
- Inbound scoping: Frontend `workflow_backend.rs`; Matching `CallerTypeScope` tower layer (+ `CallerScope` in `grpc.rs`); History gRPC (same layer) and TChannel.
- Limiter bypass: `PersistenceLimiter::build_with_bypass`; Matching limiter; History config prop `system.rateLimiterBypassCallerTypes` (new `Vec<Value>` FromDynamic).
- Outbound: `header_forwarding::apply_caller_type` on all internal gRPC clients; `call_thrift` adds `internal` outside inbound scope.
- Test updated: matching->history TChannel test now expects `cadence-caller-type: internal` app header.

## Deviations
- Internal gRPC clients forward the inbound value per Go default rules; custom `headerForwardingRules` are not applied.

## Evidence (all on head ba08574 unless noted)
- rust-ci workspace: success, https://github.com/auenkr/cadence-rust/actions/runs/44843835269
- rust-ci persistence 44838927394 / history 44839726276 (86d6917) success; frontend 44842739391 success.
- frontend-mixed-gate (mysql): success, failedSteps [], https://github.com/auenkr/cadence-rust/actions/runs/44845630915
- history-mixed-gate (mysql): success, failedSteps [], https://github.com/auenkr/cadence-rust/actions/runs/44845644482
- matching-mixed-gate (mysql): success, https://github.com/auenkr/cadence-rust/actions/runs/44845698690
- Earlier matching package run 44842739493 failed on the stale TChannel header test (fixed in ba08574).
