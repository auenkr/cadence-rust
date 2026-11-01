# Frontend gRPC workflow history hook slice (2026-11-01)

Branch: `t3/6e5c5418` (prepared worktree `t3-6e5c5418`). This is a bounded hook-routing slice and does not establish Frontend service completion.

## Prior CI run inspected

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41327541884
- Tested head: `e059623c4dae11b2f25926c01c51ce3142e8aea1`
- Conclusion: failure in Clippy before crate-specific code was checked. Compiler reported duplicate `get_domain` and `list_domains` implementations in `rust/crates/cadence-persistence/src/matching_domain_metadata.rs`.
- The tested head predates `b967e1442` (`fix: deduplicate merged domain read adapters and apply CI formatting`), which removes the duplicate adapters. Therefore that CI failure is a stale-base failure, not attributed to the history/start implementation. Formatting check and workspace formatting passed on the old run; tests were skipped after Clippy failed.

## Source parity and implementation

- Go behavior source: `go/service/frontend/api/handler.go::WorkflowHandler.GetWorkflowExecutionHistory` validates shutdown/request/domain/execution, resolves the domain, normalizes the page size, handles archival reads unless skipped, then reads history/pages and returns history or raw blobs with next-page token and archived status.
- Go gRPC converter: `go/common/types/mapper/proto/api.go::FromGetWorkflowExecutionHistoryResponse` maps `History`, `RawHistory`, `NextPageToken`, and `Archived`; `ToGetWorkflowExecutionHistoryResponse` maps all four fields back.
- Rust hook source: `rust/crates/cadence-frontend/src/handler_workflow.rs` validates and normalizes `GetWorkflowExecutionHistory`; `rust/crates/cadence-frontend/src/workflow_matching.rs` invokes the history backend and produces the typed response.
- Rust gRPC now dispatches `/api/v1/workflows/history` to `GetWorkflowExecutionHistory` through `call_with_headers` only when the configured `WorkflowApi` serves that operation. The task-local caller context carries lower-case headers and the parsed deadline. Hook errors continue through the common Cadence-to-gRPC error mapper. With no hook or an unserved operation, existing local Matching domain validation and `history_page` behavior remain active.
- Hook responses now preserve event history through `cadence_proto::convert::history`, every raw `DataBlob` (including its proto encoding discriminant and bytes), page token bytes, and archived flag. `GetWorkflowExecutionHistoryResponse.raw_history` is represented in the API type and Thrift codec (field 11), per the existing Go thrift IDL.
- Sync start hook tests now provide structurally valid start requests (domain, workflow ID/type, task list, timeouts, request ID), so they reach hook dispatch after protobuf conversion. Existing header/deadline/run ID and error-conversion assertions remain.
- Added history hook tests for request encoding, header/deadline context, raw binary blob and page-token round trips, archived/history response fields, and error conversion.

## Validation evidence

- No local Cargo, rustfmt, Go test, or Go build commands were run, as required.
- `git diff --check` passed before commit.
- CI run `https://github.com/auenkr/cadence-rust/actions/runs/41328219489` tested head `04421156b6d0c7a2aa64a207a2422f9a1d2057ba` and failed in Clippy on an unrelated-in-tests style lint, `field_reassign_with_default`, in the test fixture helper. It produced no formatter patch; all format checks passed.
- The follow-up run `https://github.com/auenkr/cadence-rust/actions/runs/41328574097` tested head `9cb4cd9c87476b355542dbb26c6d41ae36a98bbc`; the tonic trait import fixed the compilation errors. Clippy then reported `field_reassign_with_default` for the fixture's `AppState::default()` plus `workflow_api` assignment. The fixture now uses a struct initializer with `..Default::default()`. This follow-up correction is committed separately and needs a CI rerun.

## Scope limits

This work does not implement async or visibility RPC routing, and does not change Admin replication models/converters. Matching behavior is unchanged when the hook is missing or does not serve the operation. No broad Frontend parity or mixed-cluster completion claim is made.
