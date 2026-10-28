# Matching native forward client adapters (2026-10-28) — UNVERIFIED

File: `rust/crates/cadence-matching/src/matching_forward_native.rs` (new; not registered in `lib.rs`, parent must add `pub mod matching_forward_native;`).

- `InProcessNativeMatching(Arc<dyn cadence_proto::native::NativeMatchingApi>)` and `RemoteNativeMatching(tonic Channel)` implement `matching_dispatch_native::NativeMatchingApi` (4 methods).
- Raw requests/replies pass through; remaining deadline -> `Request::set_timeout`; remote adds `matching_yarpc_grpc_metadata` headers.
- Fault injection (`inject_client_call`) inside the adapter for all four: `matching-poll-for-decision-task`, `matching-poll-for-activity-task`, adds use `matching-forward-task`. A `NativeParent::Dispatcher` branch is not covered and needs separate parent wrapping; do not double-wrap adapters.
- Unit test `remote_adapter_preserves_raw_fields_headers_deadline_and_reply`: scripted tonic peer (shutdown-based), checks raw add/poll fields, YARPC headers, grpc-timeout, reply bytes, stalled-poll deadline.
- Open: add-operation name was chosen per brief (`matching-forward-task`), legacy sync-match forward in matching_dispatch.rs uses `matching-add-*-task`; confirm. Assumes native generated client is `MatchingApiClient<Channel>` with `new(channel)`; the test needs `tokio-stream` (already a dependency). Nothing compiled, linted or tested (no CI run).

## Parent source reconciliation

The parent's initial brief incorrectly named the backlog fault-injection operation matching-forward-task. The actual current legacy matching_dispatch.rs uses matching-add-decision-task and matching-add-activity-task for backlog forwarding too. Corrected both remote and in-process adapters to those exact names. Added matching fault injection to NativeParent::Dispatcher backlog forwarding; the Service branch relies on the adapter and is not double-wrapped. Registered the module and runner formatting path. Introduced an alias for the test's nested header capture type. All changes remain unverified pending CI; old operation-name statements above are superseded by this correction.
