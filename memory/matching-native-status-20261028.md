# Native Matching gRPC error envelope (UNVERIFIED: written without Cargo; parent runs CI)

File: `rust/crates/cadence-matching/src/matching_native_status.rs` (new; needs `mod matching_native_status;` in lib.rs, parent-owned).

## Finding: Go does NOT put raw invalid bytes on the wire
grpc-go v1.83.1 `internal/transport/http_util.go:encodeGrpcMessageUnchecked` decodes rune by rune; each invalid byte
becomes `utf8.RuneError` (size 1) and is written as `%EF%BF%BD`. Valid multibyte bytes are always `%XX`;
ASCII `0x20..=0x7e` except `%` is literal; everything else (NUL, newline, 0x7f, `%`) is `%XX` uppercase.
So `ff`, `fe` and U+FFFD all encode to `%EF%BF%BD` (one per invalid byte; `e2 82` -> two). Exact Go parity = this
encoding computed from the original bytes. tonic's `Status` String + `ENCODING_SET` differs (escapes space/"/#<>`?{}, and
Rust lossy maps maximal invalid sequences to one U+FFFD), hence the wrapper.

## API
- `raw_status(Code, &GoString, Bytes details) -> tonic::Status`: lossy String message + private binary marker
  `x-cadence-raw-grpc-message-bin` with the original bytes; details forwarded untouched.
- `encode_go_grpc_message(&[u8]) -> String`.
- `RawStatusService<S>::new(MatchingApiServer::new(..))`: `Service<http::Request<_>>` + `NamedService` (NAME forwarded).
  Marked responses: marker removed, `grpc-message` rewritten (empty header written for empty message, as grpc-go does).
  Unmarked responses untouched.

## Production registration requirements (parent)
1. Add `mod matching_native_status;` (pub as needed) in lib.rs.
2. Native handler error mapping (NativeQueryError, PartitionError) must build errors with `raw_status(code, &go_string, details)`
   (details = raw typed-error protobuf bytes), never `Status::new(code, String)`.
3. `Server::builder().add_service(RawStatusService::new(MatchingApiServer::new(handler)))`, NOT the bare server.
   Forgetting the wrapper leaks the marker header and uses tonic's lossy message: register it in the same place and
   add a test that the served response has no marker header.
4. Scope: unary methods only (tonic unary errors are trailers-only initial headers). Streaming errors travel in trailers and are not handled.
5. Uses existing deps only (`bytes` via `tonic::codegen::Bytes`); tower `util` is a dependency (tests use `ServiceExt::oneshot`).

## Tests (unrun)
In-module: ff/fe/U+FFFD, truncated sequence, NUL/newline/percent/0x7f, non-ASCII, empty message, details bytes + grpc-status
preserved, marker removed, unmarked headers equal `Status::into_http`, NamedService. Seam is `Status::into_http` (what tonic 0.12
unary uses) rather than a generated server.

## Parent source integration and outer-envelope correction

Registered matching_native_status and runner formatting entry. Added raw_detailed_status plus NativeQueryError::into_status and PartitionError::to_native_status; these still require RawStatusService around the eventual native server. Raw unsupported-client detail fields are manually length-delimited using the native wire helper, with proto3 empty omission; query failures carry the raw worker message.

Pinned grpc-go source confirms a further condition: http2_server.go writeStatus converts gogo/status to google.rpc.Status and uses protobuf/v2 Marshal for the outer envelope. Invalid UTF-8 Status.message fails this marshal; grpc-go omits grpc-status-details-bin while still sending normalized grpc-message. Therefore raw_detailed_status deliberately omits details for an invalid outer message. Valid outer messages retain arbitrary bytes within Any.value detail payloads. The opaque raw_status(details) API preserves its supplied details as documented; the Cadence typed-envelope helper applies Go's suppression rule.

Extended tests/matching-raw-string-oracle.go to trace the actual Cadence mapper -> YARPC gogo status -> grpc-go status -> protobuf/v2 marshal path for QueryFailedError and ClientVersionNotSupportedError. Added fixture-driven Rust comparisons of typed details and focused wrapper tests. These changes are unverified pending CI; no local Go or Cargo ran. Header rewrite remains unary-only and production registration is still open.
