# History P2 gaps (branch history-p2gaps-20261114)

## Changes
1. Mutable-state checksum repair on engine load (engine_cache.rs load_state 3-attempt loop, engine_checksum.rs repair_workflow/repair_via_rebuild, ndc_rebuild.rs rebuild_owned, StateParams corruption_repair_timeout / require_checksum_match_after_rebuild_repair). Go: execution/context.go LoadWorkflowExecutionWithTaskVersion, workflow_repairer.go.
2. NDC notifier classification: ndc_context_hooks.rs write_failed now follows Go task_notifier.go isNotifyTaskNeeded (ambiguous error -> notify with persistence_error=true; definitive -> shadow-mode drop log).
3. NDC schedule_decision: Go guards with HasPendingDecision (workflow.go ScheduleDecision); review3fix claim was wrong. Guard added; also implemented fail_in_flight_decision_for_failover and terminate_workflow_execution in ndc_state.rs.
4. Shard reference cycle: fixed (Weak cells, controller.rs). Failover coordinator: see history-failcoord-20261113.md.

## CI
(pending: run URLs / head SHA to be appended)
