# History soakfix (2026-11-13) — PARTIAL, soak still FAILS

Branch `history-soakfix` (from 3c48ab2b0). Binary SHA256 is in each run's artifact (`rust-binaries.sha256`).

## Fix 1: pending-decision guard (root cause of `invalid history action DecisionTaskScheduled (pending decision)`)
`wiring/assembly/queue_deps_timer_active.rs` `EngineTimerWorkflow::schedule_decision` called `add_decision_task_scheduled_event(false)` unconditionally. Go `workflow.ScheduleDecision` returns early when `HasPendingDecision()`. Guard added. Audit of the other callers of `add_decision_task_scheduled`: engine_cache (829), engine_decision (708, 834, 947), engine_close (145), engine_resetter (203) and engine_start first-decision are already guarded or are first-decision paths. Unit test: `engine_close::double_schedule_errors_so_callers_must_guard_with_has_pending_decision`.
Evidence: soak run 43443701871 (mysql, 45 min) has 0 occurrences of `invalid history action`.

## Fix 2: EntityNotExists classification
`engine_events.rs::read_events` flattened `HistoryReadError::EntityNotExists` into an internal error, so tasks whose history was missing were retried forever (`read history branch: EntityNotExists("Workflow execution history not found.")`, attempt-count growing, seen on rust-history-2 within 4 s of start). Now kept as `HistoryError::entity_not_exists` (the task processor drops the task like Go) and the message carries shard/range/branch context. After this, 6-min soak 43457441255 shows no such errors.

## Validation
- rust-ci: 43443685074 (success), 43455516094 (success, Fix 2).
- history-mixed-gate, 12 combos (3 DB x two_history x outbound) at Fix 1 head: all conclusion success: 43443706660 43443711423 43443717577 43443722891 43443733879 43443739520 43443745148 43443750391 43443764788 43443769519 43443775401 43443782445 (re-run needed at the final head).
- history-soak: **still FAIL**. mysql 45 min 43443701871: 5 completed, 10795 failed; 6-min diagnostics 43453144399 and 43457441255 (mysql) fail the same way. postgres/cassandra 45 min runs 43443727975 / 43443758803 were not read (superseded).

## Open: remaining stall (not a History task error)
After Fix 1+2 History logs no task errors in the first minutes, yet only ~5 workflows complete and the rest sit in query state `init` (no first decision handled). Matching traces show only a few dozen RecordDecisionTaskStarted calls (some `Decision task not found`/`already started`) for ~1440 started workflows, so decision tasks mostly never reach the Go worker. Suspects, not verified: transfer-queue processing stall (silent, no error) or Rust Matching add-task path (another worker owns matching add-task/TTL) under load; two History hosts (`no history hosts in the membership ring to own shard N` appears on start_workflow_execution). Next step: run the soak with `two_history=false` to separate the ring from the stall, and add transfer queue ack/read-level logging. The soak workflow now also uploads `logs/head-*.log` (first 300 lines of each service) for early-run evidence.
