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

Head SHA: 6f4b7003f55e6a127f93785b2750634785f8882f (merged origin/master). Evidence (all conclusion success):
- rust-ci (clippy+tests): https://github.com/auenkr/cadence-rust/actions/runs/43212376018
- gate mysql/grpc/two_history=true: https://github.com/auenkr/cadence-rust/actions/runs/43212359114 (GATE SUMMARY failed: [])
- gate cassandra/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43212365030 (GATE SUMMARY failed: [])
- gate postgres/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43212369767 (GATE SUMMARY failed: [])
Earlier fixes: clippy type_complexity / clone_on_copy, and no-in-flight test setup (EMPTY_EVENT_ID).

## Post-merge master validation

Master e607ced11: full rust-ci + 12 history-mixed-gate runs all success (2026-11-12).
Run IDs: 43214321615 43214316492 43214311541 43214306198 43214300779 43214295989 43214290569 43214285505 43214280302 43214275255 43214269808 43214264060 43214259223 

## Post-merge master validation

Master e607ced11: full rust-ci + 12 history-mixed-gate runs all success (2026-11-12).
Run IDs: 43214321615 43214316492 43214311541 43214306198 43214300779 43214295989 43214290569 43214285505 43214280302 43214275255 43214269808 43214264060 43214259223 
