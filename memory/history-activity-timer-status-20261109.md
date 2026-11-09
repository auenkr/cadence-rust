# History activity timer status slice (2026-11-09)

Branch: `t3/history-activity-timer-status`.

## Changes

- Added `timer_task_status` and `last_heartbeat_timeout_visibility_in_seconds` to History's activity record and the persistence `ActivityInfo` contract.
- Timer status uses Go sqlblobs field 44. Heartbeat timeout visibility stays in memory and resets to zero on database decode, matching Go data_manager_interfaces.go:721. No new sqlblobs field is introduced. Both values are carried by runtime activity conversions and typed snapshots/mutations.
- Activity timer generation now reads and updates the persisted activity row, so a repeated generation sees the emitted timer bit and suppresses a duplicate. Retry clears the timer status and heartbeat visibility, matching the Go retry lifecycle.
- Added focused regression tests for repeated timer generation plus snapshot retention, SQL blob round-trip retention, and retry reset.

## Validation and evidence

- `git diff --check`: passed locally (whitespace-only check; no Cargo or Go command run locally).
- Cargo formatting/build/clippy/tests were not run on the dev host per requirement. Dispatch `.github/workflows/rust-ci.yml` for package `cadence-history` after push; record the resulting Actions URL, head SHA, and conclusion here.

## Remaining gaps

- This is a bounded persistence/timer status slice, not a History service parity audit. The first worker incorrectly added extension field 76 for the heartbeat hint. Review removed it before merge: Go explicitly does not persist this hint. The regression test now verifies absence of field 76 and restart reset to zero.

- CI dispatch: https://github.com/auenkr/cadence-rust/actions/runs/42567893660
- CI head SHA: `8d030b8d46973818ce6d25f859d2a5db10dcacc5`
- CI status at dispatch verification: `in_progress` (conclusion pending).
