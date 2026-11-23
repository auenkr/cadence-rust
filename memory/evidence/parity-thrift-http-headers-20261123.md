# Parity: inbound header capture on Matching HTTP thrift (2026-11-23)

Go refs: go/common/rpc/params.go (~165, HeaderForwardingMiddleware outbound), go/common/rpc/middleware.go:151-200; yarpc HTTP inbound goes through the same InboundMiddleware.

Changes (branch parity-thrift-http-headers):
- rust/crates/cadence-matching/src/thrift_matching.rs `http_handler`: scope INBOUND_HEADERS (via `grpc_matching::inbound_headers`) and caller type (`scope_from_header`) around `dispatch_with_context`. History/Frontend have no separate HTTP thrift handler.
- cadence-matching tchannel.rs: also scope caller type from app headers (History already did).
- Unit test `captured_inbound_headers_are_forwarded_on_outbound_calls` added to Matching and History tchannel.rs (x-trace + cadence-caller-type forwarded, grpc-* dropped).

CI (head SHA 1e39a03486fc43ae11a4c1980595abe4b9a8287b):
- rust-ci.yml: success https://github.com/auenkr/cadence-rust/actions/runs/45144680576
- matching-mixed-gate (mysql): success https://github.com/auenkr/cadence-rust/actions/runs/45144686699
- history-mixed-gate (mysql): success https://github.com/auenkr/cadence-rust/actions/runs/45144691797
- matching-http-tls-interop.yml: FAILURE (runs 45144697123, 45146858398, 45147864125): `run-matching-thrift-compat.py assert_embedded_start_succeeds` - no-config embedded Matching never opens TChannel (stuck in Ringpop join, peers 7933-7935 refused).
  The same failure reproduces on the unchanged parent 20b6831f3 (run 45148727359), so it is pre-existing (likely from the go-submodule/restructure), not caused by this change. Needs separate fix.
