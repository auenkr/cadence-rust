# History NDC shared cache checkout — 2026-11-11

Branch: `history-ndccache`

## Done

- Added `EngineExecutionContext::clean_snapshot_for_ndc` in `rust/crates/cadence-history/src/wiring/assembly/engine_cache.rs`. The caller must already own the workflow cache lock. It loads through the concrete engine context, rejects dirty state instead of exporting a stale persisted snapshot, and returns the persisted snapshot unchanged for the clean case.
- Existing `EngineExecutionCache::acquire` uses an owned workflow mutex guard captured by an `Option` slot; taking it on release makes repeated release calls unlock at most once. The release closure clears cached context on error/forced clear.
- Branch is pushed at code head `a1f4ded4a`.

## Gaps

This is only the checkout seam, not completion of plan item 4b. No `assembly/ndc_workflow.rs` exists yet; `TodoNdcExecutionCache` remains in `ndc_adapters.rs`; cache wiring currently constructs `NdcReplicator` from `EngineCore` without the shared `EngineExecutionCache`; NDC mutable-state loading from the clean envelope, workflow vector/suppress/revive methods, and required concurrency/error/reload tests remain unimplemented. No service completion or behavior parity claim is made.

## CI

- Workflow dispatch accepted: https://github.com/auenkr/cadence-rust/actions/runs/42948848721
- Code head: `a1f4ded4a` (full SHA pending capture after CI completes).
- Conclusion: in progress at dispatch; final result not yet available.
- The first dispatch attempt failed because the branch did not yet exist on origin; pushed branch and subsequent dispatch succeeded.
