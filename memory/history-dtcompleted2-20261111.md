# history-dtcompleted2 (2026-11-11)

Branch `history-dtcompleted2`, code head b6cf13e6a.

## Ported (Go line by line)
- Search-attribute validation policy + UpsertSearchAttributes decision
- StartChildWorkflowExecution decision
- RequestCancelExternal / SignalExternal decisions
- ContinueAsNew, cron/retry continue-as-new, last completion result
  (`assembly/engine_continue.rs`, `update_with_new_run` in `engine_decision.rs`)
- New runs start with last_processed_event = EmptyEventID (fixes sdk-complete-child timeout)

## Evidence
- rust-ci (cadence-history), head b6cf13e6a: success
  https://github.com/auenkr/cadence-rust/actions/runs/42999882237
- history-mixed-gate (mysql, grpc, one history), head b6cf13e6a: failure, 166/171 steps ok
  https://github.com/auenkr/cadence-rust/actions/runs/43000919671
  - sdk-start-child, sdk-complete-child, schedule backfill: PASS
  - Remaining failures (all outside the decision port):
    1. sdk-refresh-workflow-tasks: "MutableStateTaskRefresher is not wired" (FIRST failure, next to fix)
    2. sdk-api-poll-activities: deadline exceeded (sdk-driver-exit exit=1 follows)
    3. admin-probe-get-cross-cluster-tasks-deprecated: error text mismatch
    4. admin-probe-reapply-events-unknown-workflow: wants DeserializeBatchEvents

## Still unported (explicit named errors kept)
local activity dispatch, semaphores, cron validation, cross-domain get_domain_by_id,
bad-binary check, auto-reset points, attempt-limit terminate, buffered query handling.
No unit tests added yet for continue-as-new.
