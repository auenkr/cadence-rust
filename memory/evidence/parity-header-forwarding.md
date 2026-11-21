# Parity: configurable `headerForwardingRules` on internal gRPC clients

Branch `parity-header-forwarding`. Gated SHA `13499b8287fe7651a7b55c3051242c14e91eadd9`.

## Go map
- `rpc.NewParams`: dynamic `admin.HeaderForwardingRules` (default: add all, remove `^grpc-.*`) first; static `headerForwardingRules` only if the dynamic list is empty. Read once at startup.
- Outbound order: `HeaderForwardingMiddleware` (rules over inbound header names, never overwrites existing) then `CallerInfoOutboundMiddleware`: sets `cadence-caller-type: internal` only when `yarpc.CallFromContext(ctx)==nil`.

## Rust changes
- `cadence-service-core/src/header_forwarding.rs`: `resolve_rules`, `set_static_rules`, `install_rules`/`install_from_dynamic` (process-wide OnceLock), `apply_outbound_with_rules`; `apply_caller_type` (used by all internal gRPC clients) now applies the installed rules to inbound headers AND to the caller-type header (a rule can block its forwarding).
- Installed at startup: Frontend (dynamic overlay + static), Matching launcher (dynamic overlay + static), History env (`admin.HeaderForwardingRules` prop + static).
- Config parsing (`HeaderRule` yaml/dynamic) already existed.

## CallerInfoOutbound check
- gRPC: `outbound_internal_default` / `outbound_header` set `internal` only outside an inbound scope; inbound without the header stays unset. Test `internal_only_outside_inbound_call`.
- TChannel (`tchannel_json_client.rs:286`): uses `outbound_internal_default` after rule forwarding — same semantics, no change needed.

## Deviations
- Inbound headers other than caller-type are only scoped (`INBOUND_HEADERS`) in Frontend; Matching/History inbound do not capture generic headers for forwarding (pre-existing, not in this change).

## Tests
Unit: internal-only-outside-inbound, rule blocks caller-type, other inbound header forwarded, resolve dynamic/static precedence.

## Evidence (head 13499b8)
- rust-ci cadence-service-core: success, https://github.com/auenkr/cadence-rust/actions/runs/44847587556
- rust-ci workspace: success, https://github.com/auenkr/cadence-rust/actions/runs/44848218192
- frontend-mixed-gate (mysql): failedSteps [], https://github.com/auenkr/cadence-rust/actions/runs/44848221959
- history-mixed-gate (mysql): failedSteps [], https://github.com/auenkr/cadence-rust/actions/runs/44848225710
- matching-mixed-gate (mysql): success, https://github.com/auenkr/cadence-rust/actions/runs/44848236494
