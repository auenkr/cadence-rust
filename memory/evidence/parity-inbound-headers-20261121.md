# parity-inbound-headers (2026-11-21)

Go refs: go/common/rpc/middleware.go (HeaderForwardingMiddleware reads yarpc.CallFromContext headers), installed for all services' outbound clients via rpc.NewParams.

Changes (Rust): Matching and History inbound handlers now scope `header_forwarding::INBOUND_HEADERS` (as Frontend does), so outbound internal gRPC calls forward inbound headers per headerForwardingRules:
- cadence-matching/src/grpc_matching.rs `CallerTypeScope` (shared by History gRPC) + new `inbound_headers()`; unit test `captured_inbound_headers_are_forwarded_on_outbound_calls`.
- cadence-matching/src/grpc.rs (gRPC layer), cadence-matching/src/tchannel.rs (Matching thrift/TChannel, app headers), cadence-history/src/wiring/server/tchannel.rs (History TChannel, app headers).

Gated SHA: e3256c0f5210cf6a8e209e97925da185392e6c8b (branch parity-inbound-headers)
- rust-ci.yml: https://github.com/auenkr/cadence-rust/actions/runs/44873541949 success
- matching-mixed-gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/44873545377 success
- history-mixed-gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/44873548055 success, failedSteps []
- frontend-mixed-gate (mysql): https://github.com/auenkr/cadence-rust/actions/runs/44873551099 success, failedSteps []
