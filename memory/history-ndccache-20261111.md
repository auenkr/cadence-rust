# History NDC shared cache checkout — 2026-11-11

Branch: `history-ndccache`

## Done

- Added `EngineExecutionContext::clean_snapshot_for_ndc` in `rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs`. The caller must already own the workflow cache lock. It loads through the concrete engine context, rejects dirty state instead of exporting a stale persisted snapshot, and returns the persisted snapshot unchanged for the clean case.
- Existing `EngineExecutionCache::acquire` uses an owned workflow mutex guard captured by an `Option` slot; taking it on release makes repeated release calls unlock at most once. The release closure clears cached context on error/forced clear.
- Branch is pushed at code head `a1f4ded4a`.

- Added `assembly/ndc_workflow.rs`: `EngineNdcWorkflow` (clear/replace/set_history_size/load, vector clock, happens_after, suppress_by, revive, flush_buffered_events via `execution/workflow.rs` policy through `PolicyAccess`), `EngineNdcExecutionCache` over `NdcCheckoutSource` (implemented for `EngineExecutionCache`: validate = same current-run resolution, then the same `acquire` lock; no second lock table, no recursive acquire). Release fn owned and run exactly once (explicit release or `Drop`). `TodoNdcExecutionCache` removed; `NdcReplicator::new` now takes the shard's `ctx.cache`.
- `NdcMutableState` gained default-erroring policy methods (vector_clock_inputs, has_*decision, update_workflow_state, update_current_version, fail/schedule decision, terminate); `OwnedNdcMutableState` implements vector clock inputs, decision predicates, update_state (non-completed) and update_current_version. `execution/state_builder.rs` untouched.
- Tests (fake `NdcCheckoutSource`, no SQL): concurrent acquire serializes, release exactly once on error/drop, clear then reload sees persisted state, persistence methods error, revive.

## Gaps

- Persistence methods (update/create/conflict-resolve/persist events/reapply) are explicit errors (plan 5a).
- Load does not run `StartTransaction` (active-cluster UpdateCurrentVersion, decision failover); load with task version is an explicit error; snapshots with pending activity/timer/child/cancel/signal/semaphore maps are refused (lossless codec into typed state not ported); non-empty ASCP blob is refused for vector clock.
- terminate/fail-in-flight/schedule decision in `OwnedNdcMutableState` remain explicit errors (need DecisionEnv), so `suppress_by` on a running same-cluster workflow errors.
- Concurrency test uses a fake source mirroring the engine lock pattern, not a real `EngineExecutionCache` (no mock `GoExecutionStore` exists); production path shares `acquire` by construction. Dirty-checkout rejection is covered only by the earlier seam, not a new test.
- No service completion / parity claim.

## CI

- Run: https://github.com/auenkr/cadence-rust/actions/runs/42950102765
- Code head: 99e26aee6c9b8e8946b2d2b1714861f7667a346a (package=cadence-history, clippy + tests)
- Conclusion: success
- Earlier run 42949536421 (head 21afb7fbd) failed: missing cache constructor call and non-Send test; fixed in 99e26aee6.
