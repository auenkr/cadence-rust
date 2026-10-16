# Matching arrival QPS tracker and load-balancer hints (2026-10-16)

Ported Go `common/stats`'s EMA fixed-window QPS tracker and wired it into the
durable matcher, closing the `TaskListStatus.newTasksPerSecond` reporting gap
named in `matching-limiter-next.md`, and returned `LoadBalancerHints` on the
durable poll responses (the `IsolationGroupMetrics` half of `DescribeTaskList`
is still open; see limits).

## Go semantics implemented

Go `taskListManagerImpl` owns `stats.NewEmaFixedWindowQPSTracker(timeSource, 0.5,
cfg.QPSTrackerInterval(domain, taskList, type), baseEvent)`:

- `AddTask` reports one arrival **only when `ForwardedFrom == ""`**: into the
  task's isolation group when `TaskInfo.PartitionConfig[isolationgroup.GroupKey]`
  ("isolation-group") is present, otherwise into the root counter. The group
  amount also counts toward the root.
- `DescribeTaskList(includeTaskListStatus=true)` reports `NewTasksPerSecond =
  qpsTracker.QPS()`; `LoadBalancerHints.RatePerSecond` and `GroupQPS(group)` use
  the same tracker state, and the adaptive scaler reads `QueriesPerSecond()`.
- The EMA: the **first** closed window stores `counter/windowSeconds` directly,
  every later window is `qps*(1-exp) + counter*exp/windowSeconds` with `exp=0.5`.
- Window length is the dynamic property `matching.qpsTrackerInterval`
  (filters DomainName, TaskListName, TaskType; default 10s).

Go `taskListManagerImpl.LoadBalancerHints()` returns nil until the manager is 10s
old (`createTime`), then `{BacklogCount: taskAckManager.GetBacklogCount(),
RatePerSecond: qpsTracker.QPS()}` — `GetBacklogCount` is the
delivered-but-unacked counter, the same value `TaskListStatus.backlogCountHint`
uses. The engine attaches the hints to every decision/activity poll response it
returns (a delivered task, an already-started forwarded task, a query task, and
the empty/expired poll).

Rust `cadence_common::qps::EmaQpsTracker` applies the identical EMA steps from an
injectable clock instead of a background ticker: arrivals are recorded against
the window they landed in, and every read folds each window the clock has
completed, one EMA step per missed tick (empty windows only decay, and a long
idle span folds in a single multiplication). For the same arrival times and
window boundaries the reported value matches Go's ticker, without a background
task. Go's own tracker unit tests are mirrored as-is
(`ema_tracker_reports_first_window_then_averages`,
`ema_tracker_groups`, `ema_tracker_multiple_report_cycles`), plus one test for
per-window attribution across an idle span.

## Files

- `rust/crates/cadence-common/src/qps.rs` (new): `EmaQpsTracker`
  (`report_counter`, `report_group`, `qps`, `group_qps`, `report`), `Clock`,
  `system_clock`; 4 unit tests.
- `rust/crates/cadence-server/src/matching_registry.rs`:
  `QueueHandle.qps` / `Owner.qps` (`Arc<EmaQpsTracker>`, `exp = 0.5`),
  `Owner.created` / `QueueHandle.created` (Go `createTime`),
  `QpsIntervalProvider`, `DEFAULT_QPS_TRACKER_INTERVAL_NANOS` (10s),
  `QueueRegistry::with_qps_interval`, `QueueHandle::load_balancer_hints`;
  the tracker is created with the owner.
- `rust/crates/cadence-server/src/grpc_matching.rs`: `report_arrival`
  (`ISOLATION_GROUP_KEY` = Go `isolationgroup.GroupKey`), called from
  `add_decision_task`/`add_activity_task` on the durable path after the sticky
  check; durable `DescribeTaskList` now sets `new_tasks_per_second`; the durable
  decision poll (query, query-failure and delivered paths) and the activity poll
  now set `load_balancer_hints`; `with_durable_queues` installs the
  `matching.qpsTrackerInterval` provider; `domain_name_for` is the shared
  UUID->name lookup used by the providers.
- `rust/crates/cadence-server/src/matching_rpc_tests.rs`:
  `matching_rpc_reports_arrival_qps_by_isolation_group`,
  `matching_rpc_reports_load_balancer_hints_only_for_old_task_lists`.

## Validation

- `cargo test -p cadence-common --lib`: **35 passed** (4 new tracker tests).
- `cargo test -p cadence-server --lib`: **82 passed** (2 new RPC tests: root and
  isolation-group arrivals, nothing reported inside an open window, and
  `DescribeTaskList.new_tasks_per_second` following the tracker; and
  `LoadBalancerHints` withheld for a task list younger than 10s, then reporting
  the backlog and a nonzero arrival rate for a 20s-old one).
- strict all-target Clippy clean.
- **Full 29/29-stage runner PASSED** (exit 0, including `workspace-tests`,
  `workspace-clippy`, `go-sdk-tchannel`, `go-services-matching-delivery` and
  `go-services-rust-matching`):
  `/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-qps-full/results.json`.

## Remaining limits

2. `isolation_group_metrics` in `TaskListStatus` is still empty: it needs the
   isolation group list from the isolation-group state
   (`config.AllIsolationGroups()`), which the Rust matcher does not model yet
   (part of the open "isolation groups" gate item). `GroupQPS` itself is
   implemented and covered by the RPC test through the queue handle.
3. `LoadBalancerHints` is returned on the durable decision and activity poll
   responses only; Go also returns them on the not-started/`PartitionConfig`-only
   paths of its poll loop, which the Rust durable poll does not have separately.
4. The tracker window provider honours only a `domainName`-filtered dynamic
   value, like the existing `matching.numTasklistWritePartitions` provider; Go
   also filters by TaskListName and TaskType.
5. `matching.qpsTrackerInterval` is read when a task list manager is created and
   is not refreshed afterwards, matching the Go note "Changes are not reflected
   until service restart".
