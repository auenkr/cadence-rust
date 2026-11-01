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
