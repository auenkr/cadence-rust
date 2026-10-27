# Native Matching tonic transport — 2026-10-28

Parent review corrected the include path to
`OUT_DIR/native-service/uber.cadence.matching.v1.rs` and the bridge's native
fully qualified type paths. Parent added a real tonic loopback test exercising
raw scalar/nested/map requests, a raw reply, the descriptor-generated bridge,
and the existing public client with valid strings. Tokio/stream test-only
dependencies already present in the workspace lock were added to this crate.
None of this has executed until the pending combined Actions run completes.

Implemented the `cadence-proto` side of a second tonic generation pass for
`go/proto/internal/uber/cadence/matching/v1/service.proto`. It keeps the
existing public generated `MatchingAPI` service intact and emits a separate
service/client in `native_service`, with matching, API, shared, history, and
well-known types redirected to the existing native model. A descriptor-driven
blanket implementation forwards all Matching service methods from the tonic
server trait to `NativeMatchingApi`.

Files changed: `build.rs`, `build/native_service.rs`, `src/native_service.rs`,
and `src/lib.rs`. No tests were added in this slice. The generated service
includes are not compile-verified locally because project instructions reserve
all Cargo validation for CI. Parent integration should run the crate CI after
the concurrent native model worker has landed `prost::Message` on every
reachable native request and response type.

Integration assumptions/gaps: tonic 0.12's generated server trait is named
`matching_api_server::MatchingApi`; the new generated file is expected at
`OUT_DIR/native-service/matching.v1.rs`. The adapter bridge references native
`api`, `history`, and nested `uber::cadence::shared::v1` paths. Confirm those
paths and tonic generic codec bounds in CI. This work only creates a usable
native transport surface; production Matching launcher registration remains
unchanged until Matching handlers implement `NativeMatchingApi`.
