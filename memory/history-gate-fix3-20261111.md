# History gate fix3 (2026-11-11)

Branch `history-gate-fix3`, from master b80c3b716. Failing baseline: gate run 43010408023.

## Root causes and fixes
1. `sdk-api-reset-workflow-execution` ("WorkflowResetter is not wired")
   - Ported Go `reset/resetter.go` as `wiring/assembly/engine_resetter.rs` and wired it in `engine_factory.rs`.
   - A second failure appeared behind it: "workflow request ... has no encoded deduplication data". Go `insertWorkflowRequest` records requests only when EnableStrongIdempotency is on (default off). The resetter now clears the un-encoded `insert_workflow_requests`.
2. `sdk-api-admin-workflow-describe` (tchannel and grpc)
   - `BufferedEvents` was serialized as a number; it is now an event list (`engine_events.rs`).
   - A second failure appeared behind it: the CLI decode failed with "Missing binary encoding version" because the JSON had no VersionHistories. `ev_copy_to_persistence` now emits `versionHistories`.
3. `sdk-driver-exit` was a consequence of 1 and 2.

## Not done
- The NDC `reset_workflow` collaborator in `ndc_adapters.rs` still returns a TODO error.
- Resetter unit tests are minimal (2 tests); the full `resetter_test.go` mirror is still open.

## Evidence
- rust-ci (cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43018294961, head 29f30fd103649f27ad163cb5036b43a8e9ff1aef, success.
- history-mixed-gate (mysql, outbound grpc, two_history=false): https://github.com/auenkr/cadence-rust/actions/runs/43019548924, same head, success.
- Earlier failed gate: https://github.com/auenkr/cadence-rust/actions/runs/43016248645.
