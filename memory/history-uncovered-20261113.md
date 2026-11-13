# History "not fully covered" audit (2026-11-13)

Branch `history-uncovered`. Code head under test: `4945b08cfae39fa3858b3b334507fe1e7541226d`.
Go vs Rust compared function by function for the four areas the Opus cross-review did not cover.

## Per-function table

| Area | Rust | Go | Status |
|---|---|---|---|
| Parent close policy | `task/transfer_active_task_executor.rs` `process_parent_close_policy`, `apply_parent_close_policy` | `transfer_active_task_executor.go` processCloseExecution / applyParentClosePolicy | MATCH |
| Parent close policy client | `wiring/assembly/parent_close_policy_client.rs` | `parentclosepolicy` client | MATCH |
| Cron schedule | `cadence-common/src/cron.rs` | `common/backoff/cron.go` | MATCH |
| Retry policy | `execution/retry.rs` | `mutable_state_builder` retry helpers | MATCH |
| Workflow retry backoff | `wiring/assembly/engine_timer.rs` `workflow_retry_backoff` | `GetRetryBackoffDuration` | FIXED: jitter coefficient was not applied; result now rounded up to whole seconds. Test `retry_backoff_rounds_up_to_whole_seconds_with_jitter` |
| Cron continue-as-new backoff | `engine_timer.rs` `workflow_cron_backoff` | `GetCronBackoffDuration` | FIXED: now uses start time + FirstDecisionTaskBackoffSeconds, jitterStartSeconds from the start event, and errors when the start event is missing. Test `cron_backoff_uses_first_decision_backoff_and_requires_start_event` |
| Cron backoff caller | `queue_deps_timer_active.rs` `cron_backoff_duration` | timer active executor | FIXED: preloads the start event before computing |
| Failover timer/transfer queue processors | `queue/timer_queue_processor.rs`, `queue/transfer_queue_processor.rs` (`failover_domain_async`, `new_*_queue_failover_processor`) | `timer_queue_processor.go`, `transfer_queue_processor.go` | MATCH |
| Task DLQ processor | `taskdlq/processor.rs` | `taskdlq/processor.go` | MATCH |
| Failover coordinator | `failover/coordinator.rs` | `failover/coordinator.go` | OPEN: not audited in this pass |

## Gate row added
`tests/frontend-gate/scen.go` section 5, workflow `ParentClosePolicyParent`: steps `scen-pcp-worker-start`,
`scen-pcp-start-parent`, `scen-pcp-child-terminate`, `scen-pcp-child-request-cancel`,
`scen-pcp-child-abandon-still-running`. Terminate and Abandon are checked by close status. RequestCancel is checked
through the CancelRequested history event, since `SignalQueryWorkflow` ignores cancellation. The existing retry and
cron rows already covered those SDK paths.

## CI evidence
- rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/43434783763 (c31f3dc), https://github.com/auenkr/cadence-rust/actions/runs/43436285697 (4945b08) — success.
- history-mixed-gate on 4945b08, every `gate.log` shows `GATE SUMMARY failed: []` and 10 `scen-pcp` lines
  (verified from downloaded artifacts; `head-sha.txt` = 4945b08):
  mysql-grpc 43436443238, mysql-tchannel 43436448812, mysql-grpc 43436454611, mysql-tchannel 43436459574,
  postgres-grpc 43436464431, postgres-tchannel 43436469942, postgres-grpc 43436475273, postgres-tchannel 43436479974,
  cassandra-grpc 43436485113, cassandra-tchannel 43436490120, cassandra-grpc 43436495411, cassandra-tchannel 43436500373.
  Base URL: https://github.com/auenkr/cadence-rust/actions/runs/<id>
