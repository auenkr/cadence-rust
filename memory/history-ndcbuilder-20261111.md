# history-ndcbuilder — 2026-11-11

Branch `history-ndcbuilder` (from master f5b2c2a18).

## Done
- Part A: `TypedNdcStateBuilder` (ndc_adapters.rs) replaces `TodoNdcStateBuilder`. Downcasts the `&mut dyn NdcMutableState` through new default trait hook `as_any_mut` (only `OwnedNdcMutableState` overrides it), calls `StateBuilder::apply_events_typed_with_new_run`, derives the new run id from the continued-as-new event, and returns the new run as an independent `OwnedNdcMutableState::from_typed`. Unknown/invalid events map to BadRequest (Go), the rest to Internal.
- Part B: `OwnedNdcMutableState::{get_activity_info, replicate_activity_info, create_next_activity_timer, generate_activity_retry_tasks}` over typed state / `TimerSequence` / task generator. `ndc::SyncActivityRequest` gained details, last_failure_reason, last_worker_identity, last_failure_details, last_failure_options (conversion in `to_ndc_sync_activity`). Timer masks reset only when `reset=true`. `activity_replicator.rs` untouched.

## Gaps
- Semaphore events still explicit typed-replay error; new-run state has empty branch token (history_replicator owns branch setup).
- Version/attempt ordering policy stays in activity_replicator; the adapter test only checks values are carried.

## CI
- Run URL / code head SHA / conclusion: pending
