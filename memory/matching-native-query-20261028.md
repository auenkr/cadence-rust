# Matching native query module (2026-10-28)

Status: additive source written, NOT compiled or tested (no local Cargo by project rule). No runtime claim; CI validation pending by parent.

Files:
- `rust/crates/cadence-matching/src/matching_query_native.rs`: byte-preserving twin of `matching_query.rs` (channel, `QueryIndex` with generations, `Registration` guard, `query_response`, `query_outcome`, version checks on `&[u8]`). Keys/IDs are `GoString`; types are `cadence_proto::native::{api,history,matching}`. Not yet declared in `lib.rs` (parent registers it).
- `rust/crates/cadence-proto/build/native_model.rs`: added `.uber.cadence.history.v1.GetMutableStateResponse` as a generator root.

Integration boundary: `tonic::Status` cannot hold arbitrary bytes, so `query_outcome` returns `NativeQueryError`:
`QueryFailed{message: GoString}`, `ClientVersionNotSupported{feature_version, client_impl, supported_versions}` (raw GoStrings), and `Status(Status)` only for pre-existing internal statuses. The native error transport must encode the first two as Go typed errors (QueryFailedError message / ClientVersionNotSupportedError) without lossy conversion. Do not map them to `Status`.

Behaviour kept from legacy: sticky needs configured sticky list + impl in {uber-go,uber-java,""} + version >= 1.0.0; strong query needs impl in {uber-go,uber-java} + >= 1.5.0 (missing info rejected); hashicorp release-version syntax (prerelease rejected, leading v, padded, metadata validated) ported to bytes, so invalid/non-ASCII bytes are rejected rather than aliased via U+FFFD. `EmptyEventID` (-23) omitted.

Follow-ups for parent: add `pub mod matching_query_native;` to lib.rs, confirm generated native field names compile (e.g. `r#impl`, `GetMutableStateResponse` fields), run clippy/tests on CI.
