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

## Original remaining parity work (before implementation follow-up)

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


## Full-shape validation and limits

History CI https://github.com/auenkr/cadence-rust/actions/runs/43590059755
(head `9e14cf7d8`, conclusion `success`) ran clippy then tests in a fresh
uncached dedicated target. History reported **916 passed, 0 failed**, including
all four new mutable-state JSON fixtures.

The final MySQL replication gate is
https://github.com/auenkr/cadence-rust/actions/runs/43590735894
(head `388ea849faee413db175522d6adbee4af20989d5`; pending when recorded).

**Remaining exact-value limitation:** the output structs represent null versus
empty maps/slices/bytes explicitly, but `history_contract::WorkflowExecutionInfo`
and its activity/signal records already represent several nullable Go fields as
non-optional Rust `Vec`/`BTreeMap`. The original presence information is not
available to History's output adapter. Current conversion uses null for empty
optional execution maps/retry lists/bytes and explicit empty containers for
initialized pending maps and buffered events. The fixture proves the chosen
Go JSON representation, not recovery of every original nil/empty distinction.
Full parity for those cases requires retaining presence information at the
persistence boundary; that is outside the requested History-only output change.
The replication gate compares events and version histories, not the entire
mutable-state object, so its pass does not close this limitation.

## User-approved scope clarification

- User explicitly selected **Go's actual enum strings**, superseding the initial
  instruction that enums should be integers. Go TaskListKind,
  WorkflowExecutionStatus, ParentClosePolicy, FailureCategory and nested API
  enums implement MarshalText. Numeric output makes the existing Go admin CLI's
  mutable-state unmarshal fail. CronOverlapPolicy has no MarshalText and stays
  numeric.
- Output enum names are transcribed from Go String/MarshalText definitions,
  rather than assuming Rust's enum labels are identical (FailureCategory differs).
- User explicitly selected **keep History-only scope and document the gap** for
  nil/empty presence already lost at the persistence boundary. The limitation
  above is accepted follow-up work, not claimed complete by these tests.
- Gate https://github.com/auenkr/cadence-rust/actions/runs/43590735894
  (`388ea849f`) was cancelled after identifying the enum/CLI incompatibility.
- CI https://github.com/auenkr/cadence-rust/actions/runs/43591716744
  (`007beb9ae`) was cancelled in favor of the Go-derived enum mapping.
- Replacement fresh History CI:
  https://github.com/auenkr/cadence-rust/actions/runs/43591791527
  (head `22c54c847`, pending when recorded).

## Final evidence

- **History CI: success**
  https://github.com/auenkr/cadence-rust/actions/runs/43591791527
  Head `22c54c847377f9d8f83e563a3b8fbca13ac90d28`.
  Fresh uncached dedicated target, clippy followed by History tests:
  **917 passed, 0 failed**, including all five mutable-state JSON fixtures.
  Command: `gh workflow run rust-ci.yml --ref history-mutstate-json -f package=cadence-history -f clean_target=true`.
- **Final MySQL replication gate: success**
  https://github.com/auenkr/cadence-rust/actions/runs/43592469144
  Head `26a23ba784e9950a0668646e46aa139866c99986` (documentation-only
  changes after the tested code head above).
  Log explicitly shows `GATE SUMMARY failed: []`.
  Rust History SHA256:
  `c19947c6548c5c599325353f0c554eed2099a2a3e88a584555d55194fdb424f8`.
  Command: `gh workflow run frontend-mixed-gate.yml --ref history-mutstate-json -f database=mysql -f repl_rust_cluster=A -f kafka=false`.
- Only exact `BranchToken` keys are removed from version-history comparisons;
  index, field names and item EventID/Version values remain raw.
- All Cargo/Go/Docker validation ran on GitHub Actions. No such commands ran
  locally. No branches were merged or deleted.
- The user-approved nil/empty presence limitation remains documented above.
