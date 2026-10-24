# Matching panic → client error mapping (Go vs Rust), 2026-10-24

Question: a panicking Matching handler, what error type/code/message does a client get over
gRPC and over Thrift? Rust `Status::unknown` was surfacing as a declared `InternalServiceError`
over Thrift/TChannel.

## Go (source of truth; paths relative to repo root)

1. Panic capture. `go/common/log/panic.go:37-53`
   - non-error panic value → `fmt.Errorf("panic object is not error: %#v", errPanic)` (line 41).
   - Sets `*retError = err` (line 50-52). Plain `*fmt.wrapError`/`errors` value, not a `types.*` type.
2. Handler wrapper. Each of the 13 MatchingApi handlers defers it, e.g.
   `go/service/matching/handler/handler.go:150` (AddActivityTask); 13 sites total at 150, 183, 216,
   256, 297, 330, 356, 386, 415, 445, 474, 502, 530. The panic skips `handleErr`, so the typed
   `InternalServiceError` mapping that normal failures get is never applied.
3. gRPC. `go/common/types/mapper/proto/errors.go:34-94`
   - `FromError`: none of the `errorutils.ConvertError` branches match a plain error, so line 93:
     `protobuf.NewError(yarpcerrors.CodeUnknown, err.Error())`.
   - Generated wrapper `go/service/matching/wrappers/grpc/grpc_handler_generated.go:28-31`:
     `proto.FromError(err)`.
   - Wire: gRPC status **Unknown** (code 2), message `panic object is not error: "boom"`.
   - Client `ToError` (`errors.go:96-275`) has no `CodeUnknown` case, so it returns the raw yarpc status.
4. Thrift mapper. `go/common/types/mapper/thrift/errors.go:28-84`
   - `FromError` falls through to `return err` (line 83). A plain error is passed unchanged.
   - Generated wrapper `go/service/matching/wrappers/thrift/thrift_handler_generated.go:15-18`:
     `return mapper.FromError(err)`.
   - thriftrw generated server `go/.gen/go/matching/matchingserviceserver/server.go:223-243`:
     `WrapResponse(appErr)` returns a non-nil error for a type that is not declared
     (`go/.gen/go/matching/matching.go`, WrapResponse doc at ~7062-7069). Then `response.IsApplicationError`
     stays false and `return response, err`.
5. TChannel (primary Thrift transport; `go/common/rpc/factory.go:63-74` serves `ch.NewInbound()`).
   - yarpc `transport/tchannel/handler.go:131-135`: `err != nil && !IsApplicationError` →
     `SendSystemError(getSystemError(err))`.
   - `handler.go:388-400` `getSystemError`: not a `yarpcerrors` status → **`ErrCodeUnexpected`**
     (0x05, `tchannel-go@v1.34.4/errors.go:65`) with `err.Error()` as the message.
   - Client side (`transport/tchannel/error.go:64-89`): Unexpected → yarpc `CodeInternal` with the same message.
     It is not `types.InternalServiceError`, so the Go client does not get a typed exception.
6. HTTP Thrift (optional inbound, `go/common/rpc/factory.go:116-125`). yarpc `transport/http/handler.go:65-105`:
   - status from `yarpcerrors.FromError(plain)` → `CodeUnknown` → HTTP **500** (`http/codes.go:30`).
   - header `Rpc-Error-Code: unknown` (`http/constants.go:96`), body `message + "\n"`, `text/plain; charset=utf8`.

### Go summary

| Transport | Error type/code on wire | Client sees | Message |
|---|---|---|---|
| gRPC | status `Unknown` | yarpc status Unknown | `panic object is not error: %#v` |
| TChannel Thrift | system error `ErrCodeUnexpected` (0x05) | yarpc `Internal` status | same |
| HTTP Thrift | 500, `Rpc-Error-Code: unknown` | yarpc `Unknown` | same + `\n` |

No declared Thrift exception and no `InternalServiceError` type are involved.

## Rust before this change

- gRPC: `grpc_matching.rs` `.unwrap_or_else(|message| Err(Status::unknown(message)))` → Unknown. Matches Go.
- Thrift/TChannel: `thrift_matching.rs` `application_error` → no typed detail → fallback `_ => "InternalServiceError"`
  → declared application error. **Mismatch:** Go sends a system error (0x05), Rust sends an application exception.
- TChannel `Err(String)` path used code 0x06 (BadRequest) for every dispatch failure, which is wrong for a panic.
- HTTP `transport_error` 500 without `Rpc-Error-Code`. Mismatch.

## Rust change (this branch)

- `rust/crates/cadence-matching/src/matching_panic.rs`: `is_captured_panic(&Status)` (code Unknown, no details,
  message prefix `panic object is not error: `) and `is_captured_panic_message(&str)`.
  The fault-injected `Unhandled` status (`matching_fault_injection.rs:74`) has no prefix, so it stays a typed
  `InternalServiceError`, as Go's `handleErr` gives for non-panic errors.
- `thrift_matching.rs` `dispatch_inner`: a captured panic returns `Err(message)` instead of an application error.
- `tchannel_error_code(message)`: 0x05 for a captured panic, 0x06 otherwise. Used in `tchannel.rs` `Ok(Err(..))`.
- `transport_failure(message)`: captured panic → 500, `Rpc-Error-Code: unknown`, `message\n`, text/plain.
  Other failures keep `transport_error`.
- gRPC is unchanged. Existing test `matching_rpc_handler_panic_is_captured_as_go_unknown_error`
  (`matching_rpc_tests.rs:~1496`) already asserts gRPC Unknown.

## Tests

- `matching_thrift_captured_panic_is_not_a_declared_exception` (thrift_matching.rs): panic status is classified;
  the untyped `Unhandled`-style status still maps to `InternalServiceError`.
- `matching_tchannel_dispatch_failures_use_go_error_codes`: 0x05 vs 0x06.
- `matching_http_captured_panic_matches_go_plain_error_response`: status, headers, body.
- gRPC: existing test above.

Limitation: no Thrift-level live panic test. The Thrift IDL has no semaphore method, and the only poison seam
(`semaphores.poison_for_test`) is gRPC-only. Thrift coverage is at the classification and wire-shaping helpers.

## Verification

- No local cargo (per task). Verification via GitHub Actions: see the commit message and PR for run ids.
- Mapping was derived by reading source: Go modules `go.uber.org/yarpc@v1.88.0`, `tchannel-go@v1.34.4`,
  `thriftrw@v1.25.0` from the module cache, plus repo `go/` code. No Go run was executed.

## Memo correction

`memory/matching-cannot-port-review-20261024.md` row 41 and the corrections section said the gate default for
`matching.enableClientAutoConfig` is `false`. Corrected:
- Rust sets `auto_config_hint` (`grpc_matching.rs:1217`, `1710-2054`), reads the key with fallback `false` (`:1245-1251`).
- Go default is `false` (`go/common/dynamicconfig/dynamicproperties/constants.go:5281-5286`).
- The gate sets it to `true` (`tests/run-go-service-baseline.py:1028`). A probe must assert the gate value.
