# history-ndcrebuild (plan item 6a) — 2026-11-11

Branch `history-ndcrebuild`. CI (rust-ci.yml, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43000239931 — head SHA 0e3f7ec9f84257fe231d9a52e7ed4e3fc4a63fbf — **success** (clippy + tests).

## Done
- `wiring/assembly/ndc_rebuild.rs`: `SqlNdcStateRebuilder` (replaces `TodoNdcStateRebuilder`) drives the generic `StateRebuilder` with `NdcRebuildContext` over `OwnedNdcMutableState` (no JSON conversion).
  - `RebuildPager` trait (`SqlHistoryPager` impl; `UnavailableRebuildPager` errors when no history node store).
  - Typed passive replay (`StateBuilder::apply_events_typed`); events needed by the refresher are captured during replay.
  - Branch switch via `set_current_branch_token`; passive snapshot close + `transaction_committed`; refresh tasks in one `run_task_generator_with` pass (drained once); start timestamp last.
  - Rebuilt CAS is never copied from an old run.
- `mutable_state_methods_common.rs`: new generic `run_task_generator_with`.
- Wired in `ndc_adapters.rs` (`new_state_rebuilder`).
- Tests: multiple pages + size sum; mismatch in middle vs end of batch; empty history; branch switch; fork error propagation; refresh tasks once; update_condition not copied.

## Gaps (explicit)
- `advanced_visibility_writing` is hardcoded `false` (TODO ndc-config; not exposed to EngineCore).
- Signal-external `child_only` not carried by Rust event attributes.
- Retention comes from `state_params(domain_id).retention_days`.
- `NdcStateBuilder` and `NdcTransactionManager` remain TODO placeholders.
