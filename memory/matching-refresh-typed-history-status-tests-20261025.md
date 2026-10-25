# Matching refresh typed History status integration tests (2026-10-25)

## Change

Branch: `t3/matching-refresh-error-tests`  
Final code commit: `4cabb9cecee5a177a2a9462a9e8838ed9fb13324`

In `rust/crates/cadence-matching/tests/matching_engine_go_tests_c.rs`, the two
previously ignored refresh tests now send an encoded `google.rpc.Status` through
`tonic::Status::with_details` to the existing scripted History tonic server.
The fixture encodes `prost_types::Any` with the public
`cadence_proto::api::EntityNotExistsError` and
`cadence_proto::api::WorkflowExecutionAlreadyCompletedError` types. The test
does not use Matching's private `grpc::detailed_status` helper.

- `EntityNotExistsError`: outer `NOT_FOUND` status message is `workflow not
  found`, with the typed detail; Matching treats the refresh as success and
  sends exactly one History request.
- `WorkflowExecutionAlreadyCompletedError`: outer `NOT_FOUND` status message
  and encoded detail are preserved; the test decodes and checks the typed
  detail, and verifies exactly one request (no retry).

The public generated detail structs do not contain the Go error's `Message`
field. As in the Rust production gRPC mapper, the human message is carried by
the outer Google RPC status; the Any contains the generated Cadence detail
message. `EntityNotExistsError` has cluster fields, and the completed error
detail is an empty message.

## Go source mapping and cancellation correction

Source: `go/service/matching/handler/engine_test.go`,
`TestRefreshWorkflowTasks` (starts at line 1712 in this checkout).

- `entity not exists error - returns nil`: mock returns
  `&types.EntityNotExistsError{Message: "workflow not found"}`; `wantErr` is
  false. Covered by
  `refresh_workflow_tasks_entity_not_exists_returns_ok`.
- `workflow execution already completed error`: mock returns
  `&types.WorkflowExecutionAlreadyCompletedError{Message: "workflow already
  completed"}`; Go asserts the typed error and message. Covered by
  `refresh_workflow_tasks_already_completed_is_propagated`.
- `context canceled`: the Go test case is also active. The Rust test
  `refresh_workflow_tasks_canceled_context_is_propagated` already checks a
  History cancellation status and a pre-cancelled caller, including zero
  History requests for the caller-cancellation case. The earlier claim that
  this test was ignored was incorrect; no additional cancellation test was
  needed.

## CI evidence

Final validation run:
https://github.com/auenkr/cadence-rust/actions/runs/40155206531

- Head SHA: `4cabb9cecee5a177a2a9462a9e8838ed9fb13324`
- Conclusion: success
- Workflow: `rust-ci.yml`, package `cadence-matching`, `clean_target=true`
- Fresh target: `/home/runner/work/_temp/cargo-target-final`
- Strict clippy (`cargo clippy -p cadence-matching --all-targets -- -D
  warnings`): exit 0
- Crate tests (`cargo test -p cadence-matching`): exit 0
- The changed status fixture and assertions have no diff in the CI rustfmt
  report. The workflow's full `cargo fmt --all -- --check` recorded exit 1 due
  to existing formatting drift elsewhere in the workspace and in older,
  untouched portions of `matching_engine_go_tests_c.rs` (lines 473, 516, and
  527 at the validated revision). The workflow currently treats that step as
  non-blocking; its output is in the run's `rust-ci-logs` artifact.

An earlier run on `bff92083ba0148ccc0375e46ce26a3d63091a248` failed to compile
because the test incorrectly expected generated detail messages to have a
`message` field. The fixture was corrected to use the actual generated shape,
then the final revision above passed clippy and tests.
