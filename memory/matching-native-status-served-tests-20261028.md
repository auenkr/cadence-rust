# Native Matching RawStatusService served-transport test (2026-10-28)

UNVERIFIED: written without compiling or running (no local Cargo; parent owns formatter/CI).

File: `rust/crates/cadence-matching/tests/native_raw_status_transport.rs`.
Serves `RawStatusService::new(MatchingApiServer::new(Failing))` over real TCP/HTTP2 with
bounded shutdown (5s) and a 15s test timeout. A tower `Capture` wrapper around the real
`Channel` records raw HTTP response headers before tonic decodes `grpc-message`.

Asserts on the wire:
- invalid message bytes -> grpc-message equals `encode_go_grpc_message(bytes)` (RuneError per byte), no details;
- valid message + typed details -> percent-encoded message and `grpc-status-details-bin` carrying the Any type URL and detail bytes;
- invalid outer message via `raw_detailed_status` -> message sent, details omitted;
- `x-cadence-raw-grpc-message-bin` absent in all served responses.

Possible compile risks to check in CI: `Capture` generic bounds vs `MatchingApiClient::new`
(needs `Error: Into<StdError>`, body bounds; passes through Channel's types), and no `base64`
dev-dep issue (already present). No Cargo dependency changes made.
