# Non-durable DescribeTaskList gaps vs Go (2026-10-24)

Branch: `matching-describe-gaps-20261024` (from origin/master e7285da4f).
Predecessor evidence: `memory/evidence/matching-describe-nondurable-20261024/notes.md`
(its branch is already merged into master; the three `parity gap` tests live on master).

Scope: non-durable `DescribeTaskList` (`queues` is `None`) in
`rust/crates/cadence-matching`, plus the `cadence-service-core` HTTP poll/describe
handlers it forwards to. No Go code changed.

## Gap table

| Gap | Go reference | Rust change | Test | Status |
|---|---|---|---|---|
| Poller `rate_per_second` always 0 | `go/service/matching/tasklist/task_list_manager.go` `getTask` (~714-729): override > poll `maxTasksPerSecond` > `TaskDispatchRPS`; `poller/manager.go` `ListInfo` (`RatePerSecond`) | `handlers.rs` `poller_rate` stores the resolved rate per poller; describe reports it | `matching_rpc_non_durable_describe_reports_poller_rate` un-ignored | CLOSED |
| `isolation_group_metrics` empty | `task_list_manager.go` `DescribeTaskList` (~767-799); `poller/manager.go` `GetCountByIsolationGroup` (outstanding or seen after `now - TaskIsolationPollerWindow`, non-empty group) | Poller record stores isolation group and outstanding flag; `PollerGuard` = Go `EndPoll`; describe emits one entry per `system.allIsolationGroups` with poller counts; matching forwards group as tonic metadata; frontend HTTP reads `cadence-worker-isolation-group` | `matching_rpc_non_durable_describe_reports_isolation_group_metrics` un-ignored | CLOSED for poller counts and group set. `new_tasks_per_second` per group still 0 (see Remaining) |
| `task_id_block` and `empty` unset | `rangeIDToTaskIDBlock` and `Empty = ackLevel == maxReadLevel` (`task_list_manager.go` ~767-799, ~983-988) | Not changed | `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` stays ignored | OPEN (large subsystem) |

## Why these choices

- Rate: Go applies `ReportLimit` to the dispatch limiter as a side effect. The non-durable
  path has no limiter, so only the poller-reported rate is implemented.
- Isolation group: the poll's group arrives on the matching-level request. Go puts it in
  context for both decision and activity polls. Rust forwards it through tonic metadata
  rather than adding a field to the public JSON poll body, so the public route surface is
  unchanged. The header name is Go's `common.IsolationGroupHeaderName`.
- Outstanding polls: Go counts an identity whose poll is still open regardless of age.
  A guard clears the flag on drop, so cancelled futures also end the poll, as in Go.
- `last_access_time` now refreshes at poll end (Go `EndPoll` re-`Put`s the entry). Before,
  it was set at poll start.
- Thrift/yarpc: `dispatch` in `yarpc.rs` takes no headers and the Thrift poll args have no
  isolation group, so yarpc polls record `""` (not counted in group metrics).

## Remaining (documented, not closed)

1. `task_id_block` and `empty` (test stays `#[ignore]`). Go derives both from the task-list
   range ID and the ack manager. The non-durable store has neither a range lease nor ack
   levels, so faking them would not match Go. Estimated >150 lines for a real model.
2. Arrival QPS: `isolation_group_metrics[*].new_tasks_per_second` and
   `TaskListStatus.new_tasks_per_second` are 0. Go reads `qpsTracker`, which needs task
   arrivals recorded per group in the non-durable path. The un-ignored isolation test only
   covers the no-arrivals case, where Go also reports 0.
3. `TaskListStatus.rate_per_second` (limiter) is 0. Go reports `limiter.Limit()`; the
   non-durable path has no limiter.
4. yarpc/Thrift polls record no isolation group (see above).

## Thrift and public-surface changes

- `cadence_api::types::TaskListStatus.isolation_group_metrics` (`isolationGroupMetrics`,
  skipped when empty) and `IsolationGroupMetrics` are additive optional fields.
- Thrift field 50 on `TaskListStatus` (map of struct, fields 10 and 20) is read and written.
  Thrift ids follow the existing `cadence-matching` description (field 50).

## Verification

Not run locally (no cargo on this host). Verified with GitHub Actions `rust-ci.yml`.

- Package run (`-f package=cadence-matching`, code head c460d1d5e): green. Clippy pass; `cadence-matching` tests 649 passed, 0 failed, 15 ignored. https://github.com/auenkr/cadence-rust/actions/runs/40065615302
- Whole-workspace run (no package input): see Runs below.

## Runs

- RUN_PKG_PLACEHOLDER
- RUN_WS_PLACEHOLDER

## Whole-workspace runs (head b9a66bbf3 = code c460d1d5e + notes)

- 40065798391 (first attempt): failure. Test `matching_reader::tests::matching_reader_pump_falls_back_to_default_for_unknown_isolation_group` (matching_reader.rs:2273, left 2 right 1). 648 passed, 1 failed, 15 ignored.
- 40065798391 (rerun, same head): failure. Test `matching_reader::tests::matching_reader_dispatch_success_uses_known_or_default_group` (matching_reader.rs:2232). 648 passed, 1 failed, 15 ignored.
- Control 40066185225 on origin/master e7285da4f: failure with the same two reader tests (645 passed, 2 failed, 17 ignored). The failures are pre-existing on master and are not caused by this branch.

Status: the package gate is green on c460d1d5e. The whole-workspace gate is NOT green on this branch, because of the pre-existing reader failures above. Not fixed here (outside the describe gap scope).
