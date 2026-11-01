# Async gRPC workflow hook routing

Implementation slice for the two Matching gRPC async workflow adapters. This records transport routing only; it does not establish asynchronous API parity.

## Change

`cadence-matching/src/grpc.rs` now maps `POST /api/v1/workflows/start-async` to `StartWorkflowExecutionAsync` and `POST /api/v1/workflows/signal-with-start-async` to `SignalWithStartWorkflowExecutionAsync`. When the installed `WorkflowApi` serves the operation, each adapter first validates/converts its nested protobuf request, wraps the complete converted request in the pinned public API shape `{ "request": ... }`, and calls `call_with_headers`. That helper preserves `GRPC_CALLER` original headers and deadline and applies the existing WorkflowApi error mapping. If no hook serves the operation, the prior local validation/enqueue path remains intact.

The Go proto definitions (`go/idls/proto/uber/cadence/api/v1/service_workflow.proto`) confirm both outer wrappers. The public Rust API wrapper types are `cadence_api::types::StartWorkflowExecutionAsyncRequest` and `cadence_api::service_types::SignalWithStartWorkflowExecutionAsyncRequest`; both serialize their nested value as `request`. Go's handlers also validate the nested request before selecting their async producer, consistent with validating the wrapper shape before dispatch.

Tests added to `workflow_hook_tests` cover both operation names, wrapper and nested field preservation (including binary payloads), original caller header/deadline propagation, missing outer/nested requests, hook error mapping, and bypass of local domain lookup/enqueue when a hook serves the request.

## Validation evidence

- Local `git diff --check`: passed.
- Cargo, rustfmt, and Go validation are intentionally CI-only.
- First CI run: https://github.com/auenkr/cadence-rust/actions/runs/41331044962
- First tested head SHA: `9f41dd962251013acd4f2ba4f1f56067ef2a331e`.
- First CI conclusion: failed because the test looked for a `data` child on the serialized memo bytes; the model serializes byte vectors as base64 JSON strings. The second CI attempt confirmed the full values are base64 strings (the only failure was comparing against JSON arrays); assertions now check those full strings. Clippy and formatting completed successfully in both runs. The workspace formatter's patch was inspected, and its `grpc.rs` hunks were applied without taking unrelated hunks.
- Corrected CI run: https://github.com/auenkr/cadence-rust/actions/runs/41332389967
- Corrected tested head SHA: `e2b0fa1093afd7ddeb00158af1389f289b47ceaf`.
- Corrected CI conclusion: success (format check, Clippy, and all `cadence-matching` tests passed). The CI formatter artifact contained only two formatting hunks in this file; both were applied locally and revalidated below.
- Final formatting follow-up CI run: https://github.com/auenkr/cadence-rust/actions/runs/41333008392
- Final tested head SHA: `3fd0266d9020c4b308f9518db304b1a2d792dcf7`.
- Final CI conclusion: success (format check, Clippy, and all `cadence-matching` tests passed); the formatter patch artifact was empty.

## Limitation

The production async queue publisher remains independently absent: `workflow_backend.rs::build_workflow_api` constructs the handler without an `AsyncWorkflowPublisher`. These hooks route to shared handler operations, but until that publisher is implemented and mixed Go Worker consumption/recovery is demonstrated, this does not establish async API parity. No publisher or mixed-cluster behavior is claimed in this slice.
