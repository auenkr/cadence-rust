# Matching task-list-scoped dynamic configuration (2026-10-16)

The durable matcher now reads `matching.numTasklistWritePartitions` and
`matching.qpsTrackerInterval` with domainName, taskListName and taskType filters.
Previously the latter two filters were silently ignored. Task type is the
persistence enum (0 decision, 1 activity); queue owners use the physical name,
while ListTaskListPartitions uses the root, as Go does.

Sources: `common/dynamicconfig/config.go` task-list-info property accessors,
`file_based_client.go` ordered subset matching, `configstore/config_store_client.go`
ordered selection, `matching/tasklist/task_list_manager.go:newTaskListConfig`,
and `matching/handler/engine.go:getAllPartitions`.

Implementation adds task-list-context typed reads in
`cadence-persistence/src/dynamic_config.rs`, retaining domain-only behavior for
existing callers. First matching filtered entry wins (not most-specific); last
decodable unfiltered entry is fallback; invalid selected values use the typed
default without trying subsequent entries.

Tests: two persistence unit tests cover scopes, order, fallbacks, malformed and
unknown filters, invalid selected values, duration parsing and physical child
names. RPC test verifies activity-only partition listing and queue limiter
configuration without affecting decisions or other names. Arrival-QPS RPC test
now sets task-list/type filters and verifies activity retains the 10s default.

Initial focused validation passed: 2 persistence tests, 83 server tests, strict
all-target workspace Clippy. Full runner is running in tmux `cadence-rust`:
`/tmp/opencode/cadence-scoped-full.log`, exit marker
`/tmp/opencode/cadence-scoped-full.exit`, artifacts
`/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-scoped-config-full`.
The full runner includes the additional scoped-QPS assertions added after the
initial focused run. CONFIRMED PASS: exit marker = 0, all 29 stages passed,
including Go SDK and mixed Go Frontend/History/Worker + Rust Matching. Isolation
reporting was being developed during later stages; this run's initial workspace
checks cover scoped-config, not the final isolation reporting changes.

Next: isolation-group reporting and poller attribution. Go obtains the available
group names from `system.allIsolationGroups` (NOT the global admin group-state
configuration), maps the entire list to strings or rejects it, and reports
GroupQPS + poller history counts within `matching.taskIsolationPollerWindow`
(2s default, task-list-info filtered). Durable poll adapters currently discard
request.isolation_group when creating PollLifecycle. Existing common poller
manager already supports count_by_group. Attribution/reporting does not itself
implement isolation-aware task selection or the fallback wait/balancer.

## Live override-RPS and membership channel cache (2026-10-17)

Committed 5db03a5eb and 88abc0766 on top of 5f9a13305.

- `Membership::remote_channel` now caches one lazy gRPC channel per owner
  address; `replace` retains only addresses still published in the new ring.
  This addresses the "persistent/cached outgoing Matching channels" gap.
- `Matching::poll_rate` ports Go `taskListManagerImpl.getTask`
  (`task_list_manager.go:715`): `matching.overrideTaskListRps`
  (DomainName/TaskListName/TaskType filtered, default 0) wins when positive,
  else the worker-reported `maxTasksPerSecond`, else the configured
  `TaskDispatchRPS`; a negative result is not reported. Decision polls (no
  worker rate) and activity polls share the helper, so the worker rate is now
  reported to the shared limiter instead of only being stored in poller
  history.

Important defect found and fixed during validation: the first implementation
held the admin dynamic-config mutex while calling `limiter.report_limit`, but
the owner's live partition-count provider (`matching.numTasklistReadPartitions`)
locks that same non-reentrant mutex while applying an accepted report. This
self-deadlocked every activity/decision poll and hung the whole full suite
(the focused filter test passed because it used a fixed partition count).
The override read is now scoped to a block. Regression
`matching_rpc_report_limit_does_not_hold_the_admin_overlay` uses a provider that
`try_lock`s the overlay and fails loudly instead of hanging; it fails with the
buggy form and passes now.

Validation: all 159 cadence-server lib tests and strict workspace/all-target
Clippy pass (`/tmp/ovr-server-lib.log`, `/tmp/ovr-clippy.log`,
`/tmp/ovr-full.status` = SERVER_LIB=0 CLIPPY=0). The broad compatibility runner
has NOT been rerun for these two commits yet. Whole Matching remains
incomplete.

Next candidates from the dynamic-config audit (each needs its real Go call
site checked before wiring): reader/dispatch values
`matching.getTasksBatchSize` (task_reader.go:109,300 with <=0 fallback to 1000),
`matching.readRangeSize` (task_reader.go:333, global int, default 50000),
`matching.asyncTaskDispatchTimeout` (task_reader.go:460, default 3s),
`matching.minTaskThrottlingBurstSize` (task_list_limiter.go:39, default 1,
read once per limiter).
