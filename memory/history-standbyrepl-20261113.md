# History standby replication apply fix (2026-11-13)

Branch `history-standbyrepl`. Problem: two-cluster gate (rust_history=true), Rust History standby fed by Go active: 3 `repl-standby-*` rows failed (`version histories are missing`, DLQ).

## Root causes / fixes (each ported from Go; all in rust/crates/cadence-history/src)
1. `wiring/assembly/ndc_adapters.rs` `new_mutable_state`: Go `NewMutableStateBuilderWithVersionHistories` -> one empty VersionHistory (was none -> MissingVersionHistories).
2. `execution/state_builder.rs` (typed replay): Go StateBuilder calls `SetHistoryTree(runID)` right after replicating WorkflowExecutionStarted; Rust did not (empty branch token -> "Missing binary encoding version" on describe).
3. `execution/state_builder.rs`: Go ends ApplyEvents with `SetHistoryBuilder(NewHistoryBuilderFromEvents(events))`; Rust left history_events empty so the replicated batch was never persisted ("Workflow execution history not found"). Now `state.history_events = events`.
4. `execution/mutable_state_builder.rs`: Passive close must not re-advance version history (Go `updateWithLastWriteEvent` returns early for Passive; "already handled in state builder"); added `passive` flag to prepare_close_{mutation,snapshot}. Needed after fix 3 (rebuild tests failed with "lower event id").
5. `is_current_workflow_guaranteed` used execution_info.state; Go uses `stateInDB`. Replayed terminate set state=Completed -> "encounter workflow not rebuilt & current workflow not guaranteed" -> DLQ.

Regression tests: state_builder (history builder kept, branch token set, passive close), ndc_adapters (empty version histories), mutable_state_builder (guaranteed uses stateInDB), updated replay tests.

## Evidence
- Failing iterations: gate 43432268879, 43435767791, 43439769624(cancelled), 43441066012 (2 of 3 fixed, terminate row failed).
- Two-cluster gate (two_cluster=true rust_history=true kafka=false) PASS, `GATE SUMMARY failed: []`: run 43443911055 at head with fix 5 (code commit "IsCurrentWorkflowGuaranteed uses stateInDB (Go)").
- rust-ci cadence-history at same code: 43443905150 success.
- Final head SHA / 12 combo run IDs: see section below.
