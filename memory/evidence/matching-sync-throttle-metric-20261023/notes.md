# Sync-offer forward throttle metric (2026-10-24)

Gap: Go `matcher.Offer` increments `SyncMatchForwardTaskThrottleErrorPerTasklist`
(`go/service/matching/tasklist/matcher.go:230-231`) when `forwarder.ForwardTask`
returns `ErrForwarderSlowDown` (parent `ServiceBusy`). Rust `sync_match_chain`
(`rust/crates/cadence-matching/src/grpc_matching.rs`, `result.ok()`) dropped the
error without the metric.

## Change

- `grpc_matching.rs` `sync_match_chain`: after the remote Add RPC, if the error
  classifies as `ForwardError::SlowDown` (`ForwardError::classify`, which mirrors
  Go `forwarder.handleErr`), call `self.metrics.sync_forward_throttled(&forward_context)`.
  Metric name `sync_forward_task_throttle_errors_per_tl`, Counter, same task-list
  tags as Go (`tm.scope`, child task list). Return value and control flow unchanged.
- `matching_rpc_tests.rs`: `FailingAddParent` fake parent plus helper
  `sync_forward_to_failing_parent`. Tests:
  - `matching_rpc_sync_forward_parent_service_busy_counts_throttle_metric`:
    ServiceBusy parent -> exactly one Counter event, value 1.
  - `matching_rpc_sync_forward_parent_other_error_does_not_count_throttle_metric`:
    internal parent error -> no throttle event.

## CI

- Branch: `matching-sync-throttle-metric-20261023`
- Head sha: `ea4a6ab811a0c85bf464e492f9ffee1e4aa78e24`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40009281832
  (`gh workflow run rust-ci.yml -f package=cadence-matching`)
- Conclusion: **success** (attempt 1 of 3)
- New tests passed:
  - `grpc_matching::durable_tests::matching_rpc_sync_forward_parent_service_busy_counts_throttle_metric ... ok`
  - `grpc_matching::durable_tests::matching_rpc_sync_forward_parent_other_error_does_not_count_throttle_metric ... ok`
- Crate result: `492 passed; 0 failed; 9 ignored`.

No local cargo was run (per instruction); verification is CI only.
