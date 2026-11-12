# history-decfailover (2026-11-12)

Branch `history-decfailover` (from master aabe60505).

## Done
- Ported Go `startTransactionHandleDecisionFailover` (mutable_state_builder.go:1936) as
  `TypedMutableState::start_transaction_handle_decision_failover`
  (`execution/mutable_state_failover.rs`, 9 unit tests). Cases 1-5; returns the flush flag.
- Cluster metadata carried in `TypedMutableState.failover` (`FailoverCluster`), installed by
  `EngineNdcContextHooks::build_state` (can_replicate_events computed at load time).
- `WorkflowContext::load_workflow_execution_with_task_version` now fetches the active failover
  version (`ActiveClusterManager::failover_version_by_workflow`), calls
  `update_current_version_from_active`, then `start_transaction`; flush-as-active on true.
- `start_transaction` stubs on the NDC state replaced.
- Unit tests for `closeTransactionHandleBufferedEventsLimit` in `engine_close.rs` (3 tests).
- `firstScheduleTimeNano`: already present (`first_scheduled_time_nano`, thrift field 62,
  gRPC `first_scheduled_time`). Note: archival JSON tag differs (Go `firstScheduleTimeNano`
  vs Rust `firstScheduledTimeNano`); untouched.

## CI evidence
See the section below (filled in after runs).
