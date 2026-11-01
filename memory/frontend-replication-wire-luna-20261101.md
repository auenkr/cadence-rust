# Frontend replication wire model slice

Implementation branch: `t3/replication-wire-models`.

Commits pushed:

- `63bde423d` Complete replication thrift wire models
- `cc5b3435b` Add replication request response thrift conversions

Current head at dispatch: `cc5b3435b1c4f7555ec0bcc4a16bb79b1e200629`.

## Implemented

- Added API models for sync shard status, replication response map, DLQ response, task info next event/scheduled IDs, and the SyncShardStatus, SyncActivity, HistoryV2, and FailoverMarker task attribute shapes. SyncActivity byte fields use base64 JSON and retain `Option<Vec<u8>>` presence.
- Added Thrift conversion for replication tokens, requests, task info, response map encoding/decoding, DLQ response, replication messages sync timestamp, and newly added task attribute structures; corrected DomainTaskAttributes field IDs to pinned IDL 5,10,20,30,40,50,60,70 and updated the existing wire assertions.
- No History routing, endpoints, or protobuf conversion was changed.

## Validation

- `git diff --check` passed before push. No Cargo or Go command was run locally.
- CI dispatch 1: https://github.com/auenkr/cadence-rust/actions/runs/41327876313 (head `63bde423d02715fc4f7a5c2742957a319256a4cb`), was in progress when superseded by the next commit.
- CI dispatch 2: https://github.com/auenkr/cadence-rust/actions/runs/41327938225 (head `cc5b3435b1c4f7555ec0bcc4a16bb79b1e200629`), queued at record time; conclusion pending. Formatting artifact and compile/test results have not yet been inspected.

## Remaining slice-1 gaps

This is not yet a complete/lossless slice. In particular, there are no independent pinned/golden fixtures covering every variant and field; SyncActivity, HistoryV2, and FailoverMarker Thrift field converters are not yet covered by fixture tests; and response serialization / conversion completeness needs CI confirmation. Several nested model optional/presence semantics and JSON field renames need audit against generated Go types. Add full independent wire fixtures and close these gaps before treating this slice as done. No claims are made about protobuf converters, History routing, or endpoint parity.

## Fixture and wire-audit continuation (branch `t3/replication-wire-fixtures`)

Prepared base: `frontend-integration-20261101` at `71c3c2733`. Cherry-picked the model branch commits in order:

- `63bde423d` -> local `31a77f47e` Complete replication thrift wire models
- `cc5b3435b` -> local `83dc354d5` Add replication request response thrift conversions
- `6ecfb26c4` -> local `cc62193a4` Record replication wire slice progress

Prior CI 41327938225 was queried once and completed FAILURE at head `cc5b3435b1c4f7555ec0bcc4a16bb79b1e200629`; Clippy failed with E0433/E0277 because the new converters referenced non-existent `crate::types::*` paths. Formatting had completed and the `format-all-patch` artifact was downloaded and applied. No Cargo or Go command was run locally.

The current implementation audit also corrected `ReplicationTaskInfo.task_type` to the actual Thrift/Go `i16`, represented workflow and run IDs independently (rather than collapsing partially present IDs into a `WorkflowExecution`), made response map and DLQ task-list presence explicit, rejected malformed map and task-type values, retained simultaneous Thrift attribute structs, preserved optional HistoryV2 version-history-list presence, and emits `hasMore=false` because Go's mapper always populates that pointer. All replication converter names now resolve to the imported `cadence_api::types` types.

Added tests include independent fixed Thrift binary bytes for DomainTaskAttributes (operation #5, ID #10 and versions #50-70) and ReplicationTaskInfo (fields #10-90 including separately populated nextEventID #80 and scheduledID #90). Additional pinned field-ID and conversion assertions cover SyncShardStatus, every SyncActivity field #10-150, HistoryV2 #5-70, FailoverMarker #10-30, task envelope alternatives #10/#11/#20/#40/#50/#70/#80/#90, tokens and both replication request/response shapes; cases cover zero, negative/subsecond nanosecond timestamps, empty and nonempty binary data, and absent versus present-empty maps. Malformed i16/map encodings are explicitly rejected. These tests are authored but await CI; `git diff --check` passed locally, and no local Cargo/Rust formatting or Go validation was run.

Remaining review gate: inspect the CI format-all patch and clippy/test result on the pushed implementation head. This remains a model/converter-only slice; no protobuf converters, History routing, or Frontend endpoint implementation is claimed.

CI follow-up 41328648612 (`46dc938b8b5d6a76d71355a46556a72b9f7c6393`) completed FAILURE in Clippy. The initial namespace/type errors are fixed. The only reported error was `clippy::needless_update` on the fully populated HistoryTaskV2 fixture. Downloaded and applied that run's `format-all.patch`, removed the redundant struct update, and pushed the corrected current head after the failure; a new CI run is required because the first run predates the final nested-domain fixture test and this Clippy fix.
