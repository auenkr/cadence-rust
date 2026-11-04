# History gap mutstate2 (2026-11-05)
Branch history-gap-mutstate2 (inventory merged from history-gap-mutstate).
Status: IN PROGRESS. Only a few functions ported so far; most of the ~120 todo functions remain.

## Ported (rust/crates/cadence-history/src/mutable_state_builder_ext.rs)
- [x] GetStartVersion
- [x] GetWorkflowStateCloseStatus
- [x] UpdateWorkflowStateCloseStatus (full transition table, Go error text)

## Remaining
All other `todo` rows in memory/history-gap-mutstate-20261105.md.
Blocker: mutable_state.rs stores execution_info and pending maps as serde_json Record, so typed ExecutionInfo is needed for faithful ports.

## CI
Run: https://github.com/auenkr/cadence-rust/actions/runs/41927688381 (before message fix; result not yet checked)
Head SHA: see git log
