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
initial focused run. Do not claim a full pass until confirmed.

Next: isolation-group reporting and poller attribution. Go obtains the available
group names from `system.allIsolationGroups` (NOT the global admin group-state
configuration), maps the entire list to strings or rejects it, and reports
GroupQPS + poller history counts within `matching.taskIsolationPollerWindow`
(2s default, task-list-info filtered). Durable poll adapters currently discard
request.isolation_group when creating PollLifecycle. Existing common poller
manager already supports count_by_group. Attribution/reporting does not itself
implement isolation-aware task selection or the fallback wait/balancer.
