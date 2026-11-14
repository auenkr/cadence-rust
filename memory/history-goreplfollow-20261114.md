# History goreplgate follow-ups (2026-11-14), branch history-goreplfollow

1. DIAG debug logs removed (task generator, timer executor base, engine_tasks, sql_workflow_store).
2. Unit test `timer_visibility_tests` in `wiring/assembly/ndc_context_hooks.rs`: blob decode gives visibility 0, `with_row_visibility` (used by the NDC hook) preserves the row's timestamp.
3. Go `startTransactionHandleDecisionFailover` ported for the engine state: `wiring/assembly/engine_failover.rs`, called from `engine_cache.rs::start_transaction_version` after `UpdateCurrentVersion(active version,false)` (incoming task version = EmptyVersion, as `LoadWorkflowExecution`). Cases 1-5, FailDecision(FailoverCloseDecision)+FlushBufferedEvents, ScheduleDecision follow Go line by line. (TypedMutableState already had its own port.)
4. Mutable-state JSON shape (describe/debug): NOT changed. The engine state keeps a persistence-shaped snapshot (`pc_exec`, `Inner`) rather than Go's `mutableStateBuilder` field layout, so matching Go's field names needs a dedicated serializer over Inner. Gap documented; gates normalise version histories.

## CI
