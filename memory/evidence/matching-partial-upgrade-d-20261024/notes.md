# Matching PARTIAL upgrade D (rows 25-32) — evidence, 2026-10-24

Branch `matching-partial-upgrade-d-20261024` (from origin/master `bafd65a77`). Shared map
`memory/matching-go-test-map-20261023.md` is not edited.

## Row selection

PARTIAL rows in document order of the map, skipping Go files `engine_test.go`,
`engine_integration_test.go`, `matcher_test.go`, `forwarder_test.go`, `task_completer_test.go`,
and membership tests (`handler/membership_test.go`, `membership/*`). Numbering follows the per-test
table rows (a table row with several Go functions counts once). Rows 1-24 (batches a/b/c) were
not touched. Rows 25-32:

| # | Map row (line) | Go test (file:line) |
|---|---|---|
| 25 | task_list_registry `TestTaskListRegistry_Filters` (L235) | tasklist/task_list_registry_test.go:102 |
| 26 | task_list_limiter `TestTaskListLimiter` (L243) | tasklist/task_list_limiter_test.go:15 |
| 27 | task_list_manager `DeliverBufferTasks` (L248) | tasklist/task_list_manager_test.go:127 |
| 28 | task_list_manager `DescribeTaskList` (L253) | task_list_manager_test.go:304 |
| 29 | task_list_manager `QueriesPerSecond` (L254) | task_list_manager_test.go:457 |
| 30 | task_list_manager `AddTaskStandby` (L256) | task_list_manager_test.go:590 |
| 31 | task_list_manager `MisconfiguredZoneDoesNotBlock` (L258) | task_list_manager_test.go:671 |
| 32 | task_list_manager `TaskWriterShutdown` (L260) | task_list_manager_test.go:943 |

## Results

| Map row | Go test | Rust test(s) | old -> new | Run |
|---|---|---|---|---|
| 25 | TestTaskListRegistry_Filters | `matching_registry.rs::registry_filters_by_domain_and_exact_name_like_go` (new, in-crate) | PARTIAL -> PORTED | 40035271566 |
| 26 | TestTaskListLimiter (12 table cases; map said ~13) | `tests/matching_partial_upgrade_d.rs::task_list_limiter_matches_go_limit_table` (new, public API, all 12 cases) | PARTIAL -> PORTED | 40035271566 |
| 27 | DeliverBufferTasks (cancel and rate-limiter cases) | `matching_reader.rs::stop_joins_pump_waiting_on_exhausted_dispatch_limiter` (new) + existing `stop_joins_pump_blocked_on_buffered_task_without_poller` | PARTIAL -> PARTIAL (rate-limiter case now asserted; the Go cancel-with-empty-buffer case is not asserted separately) | 40035271566 |
| 28 | DescribeTaskList (6 cases) | `matching_registry.rs::describe_by_domain_reports_partition_config_like_go`, `describe_by_domain_reports_recent_pollers_like_go` (new) | PARTIAL -> PARTIAL (config and poller cases now asserted via the domain description; the status cases, including TaskIDBlock, backlog/read/ack levels and isolation-group metrics, are not reproduced in-crate; RPC-level status is covered by `matching_rpc_durable_describe_tracks_activity_poller_and_queue_status` with a range size of 100, not Go's 100000) | 40035271566 |
| 29 | QueriesPerSecond (42.5 mocked, 0) | `matching_registry.rs::loaded_owner_reports_mocked_tracker_qps_after_arrivals` (new, `#[ignore = "parity gap: ..."]`); existing `loaded_owner_reports_zero_qps_before_arrivals` | PARTIAL -> PARTIAL (ignored gap: the owner tracker is a wall-clock EMA with no injection seam, so Go's exact 42.5 is unreachable) | 40035271566 |
| 30 | AddTaskStandby (errShutdown; ForwardedFrom; standby domain) | `matching_registry.rs::append_after_writer_stop_reports_stopped_and_stays_refused` (new) | PARTIAL -> PARTIAL (writer-stopped refusal with the same status is asserted; the ForwardedFrom and standby-domain branches are not reproduced) | 40035271566 |
| 31 | MisconfiguredZoneDoesNotBlock (dispatch to default group) | `matching_reader.rs::matching_reader_misconfigured_isolation_group_dispatches_to_default` (new) | PARTIAL -> PORTED (the reader dispatches each row to the default group and keeps `original-isolation-group`) | 40035271566 |
| 32 | TaskWriterShutdown (stop, AddTask errors, retry after stop-flag reset) | `matching_registry.rs::append_after_writer_stop_reports_stopped_and_stays_refused` (new) | PARTIAL -> PARTIAL (stopped refusal and retry are asserted; Go's stopped-flag reset race has no Rust equivalent because the stop lives on the queue) | 40035271566 |

Counts for rows 25-32: **PORTED 3** (25, 26, 31), **PARTIAL 5** (27, 28, 29, 30, 32). Row 29 carries
the only `#[ignore]` added in this batch.

## CI runs

- `40035139365` (commit `6b7df4121`): red. Clippy `-D warnings` rejected two deprecated proto
  fields (`num_read_partitions`, `num_write_partitions`) in the new describe test.
- `40035271566` (commit `144cc988e`, fix: read the non-deprecated `read_partitions` and
  `write_partitions` maps): **green**, `gh run view 40035271566` exit 0 for
  `rust-ci.yml -f package=cadence-matching`. `cadence-matching` lib: 606 passed, 0 failed,
  14 ignored (all pre-existing gap markers plus the row-29 gap). `tests/matching_partial_upgrade_d.rs`:
  1 passed. The flaky `matching_rpc_reports_arrival_qps_by_isolation_group` passed in this run.

URL: https://github.com/auenkr/cadence-rust/actions/runs/40035271566

## Not done in this batch

- Row 27: Go's empty-buffer cancel case, asserted directly.
- Row 28: DescribeTaskList status cases (TaskIDBlock, backlog/read/ack levels, isolation metrics,
  NewTasksPerSecond) at the RPC level with Go's 100000 range size.
- Row 30: ForwardedFrom error branch and the standby-domain `errShutdown` branch.
- Row 29: exact non-zero QPS needs an injectable owner tracker (production seam; not changed here).
