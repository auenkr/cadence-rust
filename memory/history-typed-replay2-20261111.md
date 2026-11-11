# Typed replay remaining families — 2026-11-11

## Work

- Added typed `ApplyEvents` dispatch in `execution/state_builder.rs` for timer transitions, child workflow transitions, request-cancel external transitions, signal external transitions, workflow signaled and cancel-requested events, and all requested terminal close events. Existing typed Replicate* implementations are used; dispatch never calls Add*.
- Replay still updates current version/version histories and `last_event_task_id` per event, clears stickiness, suppresses generated active tasks during dispatch, and sets `last_first_event_id` / `next_event_id` at batch end. Missing `event_type` now uses the Go unknown-event error. The legacy JSON builder remains untouched.

## Gaps and decisions

- Typed mutable state has no semaphore state. Semaphore replay returns the existing explicit unsupported typed-replay error; no event payload is discarded.
- Typed mutable state has no marker storage or search-attribute projection, and the typed Replicate* implementations do not exist. Marker and upsert events return an explicit unsupported error to avoid acknowledging data that cannot be retained.
- Continued-as-new replay closes the old typed state through the existing close transition. The API currently accepts one batch and has no optional independently-owned new-run state or source/target run and branch validation inputs. The NDC `apply_events` contract has a `new_events` slice, but `NdcMutableState` has no typed replay capability hook and `OwnedNdcMutableState` has no independent-state factory contract. Therefore `TodoNdcStateBuilder` remains in place; a direct adapter would require downcasting or expanding NDC ownership APIs and would not cleanly solve new-run linkage. This requires a follow-up structural seam before wiring.
- Fixture coverage added in this slice: none yet. Existing 3a tests cover start/decision/activity and unsupported event behavior. Required remaining-family/missing-attribute/unknown/continued-new-run fixture matrix is still open.
- No mixed Go/Rust validation or full service parity claim.

## Validation

- Local `git diff --check` passed. No Cargo, rustfmt, or Go command was run locally, per repository instructions.
- CI command: `gh workflow run rust-ci.yml --ref history-typed-replay2 -f package=cadence-history`
- CI run URL: pending
- Code head SHA: pending
- Conclusion: pending
