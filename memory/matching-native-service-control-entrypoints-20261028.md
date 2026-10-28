# Native Matching control service entrypoints, 2026-10-28

Implemented the seven requested inherent methods in
`rust/crates/cadence-matching/src/matching_service_native.rs`:
`describe_task_list`, `get_task_lists_by_domain`, `cancel_outstanding_poll`,
`respond_query_task_completed`, `list_task_list_partitions`,
`update_task_list_partition_config`, and `refresh_task_list_partition_config`.
Each method delegates to the corresponding native engine control method and
wraps successful output in tonic `Response`. Partition updates and refreshes
retain the request's actual tonic deadline. Query completion validates the
generated mapper condition before lifecycle/admission/request accounting, then
delivers through the engine-wide query task ID map.

The control engine's `control_queue` obtains the manager and calls
`NativeControlContext::check_queue` before reading or mutating queue state;
this is the runtime domain/ownership check and means registry lookup alone does
not authorize control. Domain UUID/name resolution uses the inherited control
context; get-by-domain also invokes the explicit `domain_for_name` integration
callback before engine enumeration. Go handler domain resolution is kept
before handler accounting for UUID-based describe, cancel and partition
configuration requests. List partitions and get-by-domain perform cache
resolution through the engine context at the handler's corresponding stage.

Failures returned after handler accounting pass through `finish` and are
recorded. Query mapper failures happen before handler accounting, matching the
Go gRPC mapper boundary. UUID domain lookup failures that happen before the Go
handler context is constructed are returned directly. No integration callback
is defaulted or synthesized; a runtime must provide concrete lifecycle,
admission, metrics, domain cache, and queue ownership behavior.

Scope is limited to these seven inherent methods and this memory note. The
`NativeMatchingApi` trait implementation, remaining RPCs, module registration,
and production listener remain outside this change. No Cargo, Go, rustfmt,
build, test, or CI command was run; compilation and behavior remain unverified.
Only `git diff --check` is intended for local validation.
