# History mutable-state JSON parity (2026-11-14)

Branch: `history-mutstate-json`

## Changes

- `EngineMutableState::ev_copy_to_persistence` now emits version histories with
  Go `encoding/json` exported field names (`CurrentVersionHistoryIndex`,
  `Histories`, `BranchToken`, `Items`, `EventID`, `Version`), preserving zero
  values and encoding branch-token bytes as standard base64.
- Added a Rust unit fixture for those version-history JSON bytes.
- Removed `norm()` from `tests/gates/run-history-repl-gate.py`; the replication
  gate now compares the version-history JSON object as returned.

## CI evidence

- Rust History CI (clippy + tests):
  https://github.com/auenkr/cadence-rust/actions/runs/43585479278
  Head `7e92c0e58b9d5d9017f25501b9b9a85e2ecc7f06`, conclusion `success`.
- MySQL Go/Rust history replication gate, Rust History on cluster A:
  https://github.com/auenkr/cadence-rust/actions/runs/43586192457
  Head `7e92c0e58b9d5d9017f25501b9b9a85e2ecc7f06`, conclusion `failure`.
  The event history checks passed. Raw version-history equality failed for six
  cases because Go and Rust clusters have different branch UUIDs embedded in
  their base64 `BranchToken` values. Gate output ended with:
  `GATE SUMMARY failed: ['p1-A-completed-version-histories-equal', 'p1-B-parked-version-histories-equal', 'p1-A-child0-version-histories-equal', 'p2-B-continued-version-histories-equal', 'p2-C-new-on-A-version-histories-equal', 'p2-A-old-completed-still-equal-version-histories-equal']`.
  The run artifact includes Rust History SHA256
  `a2f3a97da68b3db918bd7ff2f25ee5d94ed2fef03b7c8f2dfed0ca6eec40245f`.

## Remaining parity work

The version-history subobject now has Go's JSON representation, but
`ev_copy_to_persistence` still emits a reduced mutable-state view with camelCase
top-level keys (`executionInfo`, `pendingActivities`, etc.). Go marshals the
complete `persistence.WorkflowMutableState` with exported PascalCase field
names, nil/zero fields, and the full nested persistence structs. Therefore the
full `mutableStateInCache` / `mutableStateInDatabase` byte-shape goal is not yet
complete. The cross-cluster raw gate also demonstrates that branch tokens are
cluster-specific, so its direct equality condition cannot pass while comparing
these two distinct persistence branches, even though token encoding is
compatible.

## Implementation follow-up

- Restored normalization only for exact `BranchToken`; all other version-history
  keys, indices, items and values are compared without normalization.
- Normalization-only MySQL repl gate:
  https://github.com/auenkr/cadence-rust/actions/runs/43588255295
  Head `281dba321`, conclusion `success`; log explicitly contains
  `GATE SUMMARY failed: []`.
  Rust History SHA256 `6731948df93d6ada363fcadb1bcdf3f6630699f9c7553adb99843cdbd9c2eb2d`.
- Added output-only serde persistence structs, in Go declaration order, for the
  complete mutable state and its execution/activity/timer/child/cancel/signal/
  semaphore/version-history/replication/checksum records. The nested API view
  follows Go's json tags with a checked-in field schema; transport serializers
  are unchanged. Added hand-built full JSON and focused binary/time/tag fixtures.
- First full-shape History CI:
  https://github.com/auenkr/cadence-rust/actions/runs/43588548302
  Head `ec4f37b24`, conclusion `failure`: the default trait method needed a
  qualified HistoryError import. Fixed before the next CI dispatch.
- Follow-up History CI attempts:
  - https://github.com/auenkr/cadence-rust/actions/runs/43589046708
    (`d74d3a3c3`, failure: Rust Unicode escape syntax, corrected).
  - https://github.com/auenkr/cadence-rust/actions/runs/43589592783
    (`715438627`, failure: clippy type complexity, item ordering and unnecessary
    concat; corrected). CI also generated the rustfmt patch applied only to the
    new JSON module; no formatting command ran locally.
  - https://github.com/auenkr/cadence-rust/actions/runs/43590059755
    (`9e14cf7d8`, pending when recorded), fresh dedicated target,
    `package=cadence-history`, clippy then tests.
