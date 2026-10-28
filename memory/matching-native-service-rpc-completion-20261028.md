# Native Matching service RPC wiring (2026-10-28)

Scope: `rust/crates/cadence-matching/src/matching_service_native.rs` only, plus this note. This is the implementation subtask for the native service RPC surface; parent owns `lib.rs` registration, runtime providers and launcher integration.

Implemented:

- Explicit `NativeMatchingApi` implementation for all 13 methods generated from `go/proto/internal/uber/cadence/matching/v1/service.proto`.
- Add decision/activity, decision/activity polling and workflow query dispatch to the native engine. Incoming tonic deadlines are captured before consuming requests and passed to the engine.
- Query errors pass through `NativeQueryError::into_status`, retaining typed/raw status details.
- Mandatory async startup readiness callback, domain guard callback and native semaphore callback. Semaphore receives the original native tonic request and returns a native response; there is no UTF-8-model conversion or permissive default.
- Existing control RPCs forward through their inherent methods, preserving the async partition-list implementation and explicitly qualifying the ambiguous `NativeControlContext::domain_name` calls.
- Added a bounded regression asserting that generated native method metadata contains all 13 service RPC names.

Source review used `matching_engine_native.rs`, `matching_engine_poll_native.rs`, `matching_engine_query_native.rs`, `matching_engine_control_native.rs`, `grpc_matching.rs`, and the matching service proto. The engine paths perform their own registry lookup followed by context `check_queue`; production contexts still must implement that callback with the actual handler ownership check. Add/poll/query domain guard is also an explicit required callback.

Limits / handoff:

- Parent must supply implementations for the newly required async `check_started`, `check_domain`, and native `acquire_semaphore` callbacks; semaphore behavior itself is outside this task.
- No claim is made that the service is runtime complete or that the RPC order fully matches every Go branch. In particular, a focused complete-provider dispatch/order test was not added in this narrow change.
- No Cargo, Go, rustfmt, build, test, check, CI, staging, commit, or push was run/performed. `git diff --check` is the only mechanical validation performed; this is formatting/whitespace validation, not compilation or test evidence.
