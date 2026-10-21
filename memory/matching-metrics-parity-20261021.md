# Matching metrics parity audit (source-level, 2026-10-21)

Source-only comparison of Go `go/common/metrics/defs.go` (Matching metric block, enum
`PollSuccessPerTaskListCounter`..`PartitionDrained`) and emission sites under
`go/service/matching` against `rust/crates/cadence-server/src/matching_metrics*.rs` and their
call sites, at base commit 864dcdbe9 (branch matching-telemetry-mysql-candidate). **Nothing here
was run or tested.** Rust file paths are relative to `rust/crates/cadence-server/src/`; Go paths to
`go/service/matching/`.

Tag comparison is shallow: Rust helpers in `matching_metrics.rs` build the domain/task_list/
tasklistType/isolation-group tags from `PollMetricContext`; I did not diff every tag set per metric
against Go's `scope.Tagged(...)` chains. Treat tag parity as unverified for all rows.

## Summary

- Go Matching-block metrics: 93.
- **Missing in Rust: 14** (4 with no Rust name at all, 10 with a Rust helper but no call site).
- Name mismatch: 1 (`respond_query_failed_per_tl`; Rust emits `respond_query_task_failed_per_tl`).
- Go defines but never emits: 9 (nothing to port; listed N/A).
- Missing outside the Matching defs block: `cadence_errors_semaphore_context_timeout`
  (Go `handler/handler.go:553`, no Rust equivalent). That makes **15 missing counting it**.
- Not audited in depth: shared scopes Matching uses (`cadence_requests_per_tl`, `cadence_latency_per_tl*`,
  per-error `cadence_errors_*_per_tl` in `handler/context.go`, `cadence_tasklist_request` at
  `handler/engine.go:549,632`, adaptive scaler `CadenceRequests`/`CadenceFailures` at
  `tasklist/adaptive_scaler.go:209,213`). Rust has the corresponding constants and error-class
  names in `matching_metrics.rs` (InboundRequestTimer, `add_task_request`) but emission points
  were not compared one by one.
- Persistence-client and history-client scopes: Go Matching gets these from the shared metered
  wrappers. Rust has history-client events (`history_started`/`history_completed`,
  `matching_history.rs:180,917`). I found no Rust persistence request/latency/error metrics.
  Not verified further; flag as likely gap for the persistence scopes.

## Table (type = Go metric type; counter if unspecified)

| Go metric | Type | Go emission site | Rust emission site or MISSING |
|---|---|---|---|
| `poll_success_per_tl` | Counter | tasklist/matcher.go:529, tasklist/matcher.go:550, tasklist/matcher.go:567, tasklist/matcher.go:619, tasklist/matcher.go:640, tasklist/matcher.go:657, tasklist/matcher.go:682, tasklist/matcher.go:701, tasklist/matcher.go:718, tasklist/matcher.go:748, tasklist/matcher.go:767, tasklist/matcher.go:784 | matching_dispatch.rs:945 (poll_success); grpc_matching.rs:1669 (query) |
| `poll_timeouts_per_tl` | Counter | tasklist/matcher.go:570, tasklist/matcher.go:660 | matching_dispatch.rs:742 |
| `poll_success_sync_per_tl` | Counter | tasklist/matcher.go:525, tasklist/matcher.go:546, tasklist/matcher.go:566, tasklist/matcher.go:615, tasklist/matcher.go:636, tasklist/matcher.go:656, tasklist/matcher.go:680, tasklist/matcher.go:699, tasklist/matcher.go:717, tasklist/matcher.go:746, tasklist/matcher.go:765, tasklist/matcher.go:783 | matching_dispatch.rs:945 (sync flag -> POLL_SUCCESS_SYNC) |
| `lease_requests_per_tl` | Counter | tasklist/task_writer.go:228 | matching_registry.rs:955,973 |
| `lease_failures_per_tl` | Counter | tasklist/task_writer.go:231 | matching_registry.rs:966,973 |
| `condition_failed_errors_per_tl` | Counter | tasklist/task_list_manager.go:358 | MISSING |
| `respond_query_failed_per_tl` | Counter | handler/engine.go:1094 | NAME MISMATCH: grpc_matching.rs:3144 emits "respond_query_task_failed_per_tl"; Go name is "respond_query_failed_per_tl" |
| `sync_throttle_count_per_tl` | Counter | tasklist/matcher.go:155, tasklist/matcher.go:256 | grpc_matching.rs:1368 |
| `buffer_throttle_count_per_tl` | Counter | tasklist/task_reader.go:544 | matching_reader.rs:595 |
| `buffer_unknown_task_dispatch_error_per_tl` | Counter | tasklist/task_reader.go:568 | matching_dispatch.rs:1029 |
| `buffer_isolation_group_redirected_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `buffer_isolation_group_redirect_failure_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `buffer_isolation_group_misconfigured_failure_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `tasks_expired_per_tl` | Counter | tasklist/task_reader.go:365, tasklist/task_reader.go:490 | matching_reader.rs:464,722 |
| `forwarded_per_tl` | Counter | handler/handler.go:167, handler/handler.go:200, handler/handler.go:233, handler/handler.go:273, handler/handler.go:314 | grpc_matching.rs:1570,1787,1987,2182,2368 |
| `forward_task_calls_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `forward_task_errors_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `sync_forward_task_throttle_errors_per_tl` | Counter | tasklist/matcher.go:231 | grpc_matching.rs:1427 |
| `async_forward_task_throttle_errors_per_tl` | Counter | tasklist/matcher.go:380 | MISSING |
| `forward_query_calls_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `forward_query_errors_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `forward_poll_calls_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `forward_poll_errors_per_tl` | Counter | none (defined, never emitted in Go) | N/A - Go defines but never emits; Rust absent too |
| `syncmatch_latency_per_tl` | Timer | handler/engine.go:586, handler/engine.go:662 | MISSING |
| `syncmatch_latency_per_tl_ns` | Histogram | handler/engine.go:587, handler/engine.go:663 | MISSING |
| `asyncmatch_latency_per_tl` | Timer | tasklist/task_reader.go:503, handler/engine.go:1398, handler/engine.go:1432 | matching_reader.rs:614 (async_match_latency) |
| `asyncmatch_latency_per_tl_ns` | Histogram | tasklist/task_reader.go:506, handler/engine.go:1399, handler/engine.go:1433 | matching_reader.rs:614 |
| `asyncmatch_dispatch_timeouts_per_tl` | Counter | tasklist/task_reader.go:537 | matching_reader.rs:601 |
| `forward_task_latency_per_tl` | Timer | tasklist/forwarder.go:134 | grpc_matching.rs:1373-1520 |
| `forward_task_latency_per_tl_ns` | Histogram | tasklist/forwarder.go:137 | grpc_matching.rs:1373-1520 |
| `forward_query_latency_per_tl` | Timer | tasklist/forwarder.go:192 | grpc_matching.rs:2524,2548 |
| `forward_query_latency_per_tl_ns` | Histogram | tasklist/forwarder.go:195 | grpc_matching.rs:2524,2548 |
| `forward_poll_latency_per_tl` | Timer | tasklist/forwarder.go:222 | matching_dispatch.rs:507,565 |
| `forward_poll_latency_per_tl_ns` | Histogram | tasklist/forwarder.go:225 | matching_dispatch.rs:507,565 |
| `local_to_local_matches_per_tl` | Counter | handler/engine.go:1567 | matching_dispatch.rs:952 (poll_match) |
| `local_to_remote_matches_per_tl` | Counter | handler/engine.go:1565 | matching_dispatch.rs:952 (poll_match) |
| `remote_to_local_matches_per_tl` | Counter | handler/engine.go:1563 | matching_dispatch.rs:952 (poll_match) |
| `remote_to_remote_matches_per_tl` | Counter | handler/engine.go:1561 | matching_dispatch.rs:952 (poll_match) |
| `isolation_task_matches_per_tl` | Counter | handler/engine.go:1581 | matching_dispatch.rs:952 (poll_match) |
| `isolation_success_per_tl` | Counter | handler/engine.go:1583 | matching_dispatch.rs:952 (poll_match) |
| `poller_count_per_tl` | Counter | tasklist/task_list_manager.go:213 | matching_dispatch.rs:41,57 |
| `poller_invalid_isolation_group_per_tl` | Counter | tasklist/matcher.go:444 | matching_dispatch.rs:767 |
| `tasklist_partition_update_failed_per_tl` | Counter | tasklist/task_list_manager.go:492 | grpc_matching.rs:2880,2908 |
| `tasklist_managers` | Gauge | tasklist/task_list_registry.go:143 | matching_registry.rs:919,1683,1826,1849 |
| `task_lag_per_tl` | Gauge | tasklist/task_reader.go:395 | matching_reader.rs:824 |
| `task_backlog_per_tl` | Gauge | tasklist/task_reader.go:231 | matching_reader.rs:661,778 |
| `task_count_per_tl` | Gauge | tasklist/task_reader.go:273 | matching_reader.rs:809 |
| `rate_limit_per_tl` | Gauge | tasklist/task_list_limiter.go:103, tasklist/task_list_limiter.go:112 | grpc_matching.rs:1037 |
| `syncmatch_local_poll_latency_per_tl` | Timer | tasklist/matcher.go:175, tasklist/matcher.go:203, tasklist/matcher.go:265 | grpc_matching.rs:1375 (sync_match_latency) |
| `syncmatch_local_poll_latency_per_tl_ns` | Histogram | tasklist/matcher.go:176, tasklist/matcher.go:204, tasklist/matcher.go:266 | grpc_matching.rs:1375 |
| `syncmatch_forward_poll_latency_per_tl` | Timer | tasklist/matcher.go:226 | grpc_matching.rs:1375 (forwarded=true) |
| `syncmatch_forward_poll_latency_per_tl_ns` | Histogram | tasklist/matcher.go:227 | grpc_matching.rs:1375 (forwarded=true) |
| `asyncmatch_local_poll_per_tl` | Counter | tasklist/matcher.go:339, tasklist/matcher.go:366 | matching_reader.rs:626 (async_match_local) |
| `asyncmatch_local_poll_attempt_per_tl` | Timer | tasklist/matcher.go:367 | matching_reader.rs:626 |
| `asyncmatch_local_poll_attempt_per_tl_counts` | Histogram | tasklist/matcher.go:368 | matching_reader.rs:626 |
| `asyncmatch_local_poll_latency_per_tl` | Timer | tasklist/matcher.go:340, tasklist/matcher.go:369 | matching_reader.rs:626 |
| `asyncmatch_local_poll_latency_per_tl_ns` | Histogram | tasklist/matcher.go:341, tasklist/matcher.go:370 | matching_reader.rs:626 |
| `asyncmatch_forward_poll_per_tl` | Counter | tasklist/matcher.go:415 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_forward_poll_attempt_per_tl` | Timer | tasklist/matcher.go:416 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_forward_poll_attempt_per_tl_counts` | Histogram | tasklist/matcher.go:417 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_forward_poll_latency_per_tl` | Timer | tasklist/matcher.go:418 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_forward_poll_latency_per_tl_ns` | Histogram | tasklist/matcher.go:419 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_local_poll_after_forward_failed_per_tl` | Counter | tasklist/matcher.go:399 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_local_poll_after_forward_failed_attempt_per_tl` | Timer | tasklist/matcher.go:400 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_local_poll_after_forward_failed_attempt_per_tl_counts` | Histogram | tasklist/matcher.go:401 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_local_poll_after_forward_failed_latency_per_tl` | Timer | tasklist/matcher.go:402 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `asyncmatch_local_poll_after_forward_failed_latency_per_tl_ns` | Histogram | tasklist/matcher.go:403 | MISSING (NO CALL SITE: helper defined in matching_metrics.rs (async_match_forward / async_match_local_after_forward_failure) but never invoked outside unit tests) |
| `poll_local_match_latency_per_tl` | Timer | tasklist/matcher.go:465, tasklist/matcher.go:494, tasklist/matcher.go:527, tasklist/matcher.go:548 | matching_dispatch.rs:520,946 (poll_match_latency) |
| `poll_local_match_latency_per_tl_ns` | Histogram | tasklist/matcher.go:466, tasklist/matcher.go:495, tasklist/matcher.go:528, tasklist/matcher.go:549 | matching_dispatch.rs:520,946 (poll_match_latency) |
| `poll_forward_match_latency_per_tl` | Timer | tasklist/matcher.go:590 | matching_dispatch.rs:520,946 (poll_match_latency) |
| `poll_forward_match_latency_per_tl_ns` | Histogram | tasklist/matcher.go:591 | matching_dispatch.rs:520,946 (poll_match_latency) |
| `poll_local_match_after_forward_failed_latency_per_tl` | Timer | tasklist/matcher.go:617, tasklist/matcher.go:638 | matching_dispatch.rs:520,946 (poll_match_latency, after-forward-failed branch) - branch selection not verified |
| `poll_local_match_after_forward_failed_latency_per_tl_ns` | Histogram | tasklist/matcher.go:618, tasklist/matcher.go:639 | matching_dispatch.rs:520,946 (poll_match_latency, after-forward-failed branch) - branch selection not verified |
| `poll_decision_task_already_started_per_tl` | Counter | handler/engine.go:835 | matching_dispatch.rs:995 |
| `poll_activity_task_already_started_per_tl` | Counter | handler/engine.go:988 | matching_dispatch.rs:995 |
| `tasklist_read_write_partition_mismatch` | Gauge | tasklist/task_list_manager.go:960 | grpc_matching.rs:1133 |
| `tasklist_poller_partition_mismatch` | Gauge | tasklist/task_list_manager.go:964 | grpc_matching.rs:1133 |
| `estimated_add_task_qps_per_tl` | Gauge | tasklist/task_list_manager.go:567, tasklist/adaptive_scaler.go:238 | matching_scaler.rs:565 (scaler_tick); Go also emits at task_list_manager.go - Rust only scaler path seen |
| `tasklist_partition_upscale_threshold` | Gauge | tasklist/adaptive_scaler.go:239 | matching_scaler.rs:565 |
| `tasklist_partition_downscale_threshold` | Gauge | tasklist/adaptive_scaler.go:240 | matching_scaler.rs:565 |
| `standby_cluster_tasks_completed_per_tl` | Counter | tasklist/task_completer.go:128, tasklist/task_completer.go:150 | matching_standby.rs:114,122 |
| `standby_cluster_tasks_not_started_per_tl` | Counter | tasklist/task_completer.go:155 | matching_standby.rs:114 |
| `standby_cluster_tasks_completion_failure_per_tl` | Counter | tasklist/task_completer.go:163 | matching_standby.rs:139 |
| `task_isolation_leak_per_tl` | Counter | tasklist/task_list_manager.go:917, tasklist/task_list_manager.go:921, tasklist/task_list_manager.go:925, tasklist/task_list_manager.go:929, tasklist/task_list_manager.go:934, tasklist/task_list_manager.go:946 | matching_reader.rs:527,753 |
| `partition_upscale_per_tl` | Counter | tasklist/adaptive_scaler.go:248 | matching_scaler.rs:583 |
| `partition_downscale_per_tl` | Counter | tasklist/adaptive_scaler.go:253 | matching_scaler.rs:587 |
| `partition_drained_per_tl` | Counter | tasklist/adaptive_scaler.go:302 | matching_scaler.rs:655 |
| `isolation_rebalance_per_tl` | Counter | tasklist/adaptive_scaler.go:170 | matching_scaler.rs:606 |
| `ig_started_polling_per_tl` | Counter | tasklist/isolation_balancer.go:152 | matching_scaler.rs:625 |
| `ig_stopped_polling_per_tl` | Counter | tasklist/isolation_balancer.go:146 | matching_scaler.rs:631 |
| `ig_upscale_per_tl` | Counter | tasklist/isolation_balancer.go:190 | matching_scaler.rs:640 |
| `ig_downscale_per_tl` | Counter | tasklist/isolation_balancer.go:192 | matching_scaler.rs:646 |
| `ig_partitions_per_tl` | Gauge | tasklist/isolation_balancer.go:166, tasklist/isolation_balancer.go:200 | matching_scaler.rs:652 || `cadence_errors_semaphore_context_timeout` | Counter | handler/handler.go:553 | MISSING |
