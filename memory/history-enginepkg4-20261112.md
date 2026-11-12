# history-enginepkg4 (2026-11-12)

Branch history-enginepkg4. Only cfg(test) code changed (harness `cache` field, `EngineExecutionCache::clear_all_for_test`), so no mixed gate was required.

## Ported (real_engine_tests.rs, +17 tests, 48 total)
- Activity canceled: success after cancel request, rejected without cancel request, not-started rejected; RequestCancelActivityTask before start cancels immediately.
- Continue-as-new (old run CONTINUED_AS_NEW, new current run), start child initiated, signal-external initiated, request-cancel-external initiated.
- Retry policy: start persists policy; activity failure with retry policy retries without a failed event.
- Reload after cache clear (activity flow, decision flow).
- record_child_execution_completed: unknown child (EntityNotExists), child not started (stale).

## Deviations (Go-faithful, not Rust bugs)
- Go AddActivityTaskCanceledEvent requires cancel to have been requested; otherwise Internal error "Unable to add ActivityTaskCanceled event". Rust matches.
- Retrying activity failure does not write ActivityTaskStarted (next_event_id stays 6).
- After ScheduleActivityTask no decision is scheduled until a signal/completion.

## Bugs fixed
None found.

## Remaining (not ported)
- Buffered consistent query and sticky query (needs fake matching + query registry).
- history_engine3_eventsv2 (needs MemExecutions.conflict_resolve).
- Child workflow started/complete success path (needs transfer-queue start-child processing), timer fire (timer queue, not engine), cancel-requested heartbeat.

## CI (rust-ci.yml package=cadence-history)
- Batch 3 first run failed on 3 wrong-expectation tests: 43147126695. Fixed run green: https://github.com/auenkr/cadence-rust/actions/runs/43147972188
- Batch 4 green: https://github.com/auenkr/cadence-rust/actions/runs/43148846027
