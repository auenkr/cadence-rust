# Matching write-partition provider (2026-10-16)

Closes the "partition count is a constant 1" gap noted in
`matching-limiter-next.md`. Go sources of truth:
`go/service/matching/handler/engine.go` `getAllPartitions` +
`listTaskListPartitions`/`getHostInfo`, `go/service/matching/tasklist/
task_list_limiter.go` (`countPartitions`), and dynamic property
`matching.numTasklistWritePartitions` (default 1, `dynamicproperties/constants.go`).

## What changed

- `TaskListLimiter` now owns a live `PartitionCount` provider
  (`Arc<dyn Fn() -> i64 + Send + Sync>`), read at construction and again
  whenever `ReportLimit` re-snapshots, exactly like Go's `countPartitions`
  (`l.partitions = l.countPartitions()` in `newTaskListLimiter` and `tryUpdate`).
  `report_limit` no longer hardcodes `|| 1`. Added `with_partitions` and
  `partitions()`.
- `QueueRegistry` gained `with_partition_count(provider)`; the provider is
  `Fn(domain_id, task_list, task_type) -> i64`. The registry builds the
  limiter's count closure from it and clamps to `max(1, ..)` (Go's
  `task_list_manager.go:1075` clamp), so an owner's dispatch limiter divides the
  reported rate by the configured write-partition count.
- `Matching::with_durable_queues` builds the shared provider from the admin
  dynamic-config overlay (`matching.numTasklistWritePartitions`) and the
  in-memory domain store (UUID -> name), the same source the frontend already
  reads its dynamic properties from.
- `ListTaskListPartitions` now enumerates every write partition like Go's
  `getAllPartitions`: root first, then `/__cadence_sys/<root>/<i>` up to the
  configured count, for both activity (type 1) and decision (type 0).

## Validation

- `cargo check -p cadence-server --all-targets` clean.
- `cargo test -p cadence-server --lib`: 72 passed (two new tests:
  `matching_limiter::tests::report_divides_by_live_partition_count` and
  `matching_rpc_lists_configured_write_partitions`).
- `cargo clippy -p cadence-server --all-targets -- -D warnings` clean.
- Full runner CONFIRMED PASSING (2026-10-16): `tests/run-compatibility.py
  --artifacts-dir .../matching-partitions-full --go-mixed-server-bin
  .../matching-mixed-20261016/cadence-server` exit 0, all 29 stages incl.
  `go-services-matching-delivery` and `go-services-rust-matching`. Results:
  `/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-partitions-full/results.json`.

## Known limit / still open

- The provider reads the admin overlay only; it does not yet load the Go
  `dynamicconfig/development.yaml` file, so non-default values must be set
  through the admin API. This is the same dynamic-config-source limitation the
  frontend typed reads already have, not new.
- The dynamic-config filter support is domainName only; the Go property also
  accepts TaskListName/TaskType filters. Defaults match.
- Not yet done: actually storing/reading partitioned task lists (child queue
  routers), forwarding between partitions (`forwarded_from` still returns
  `unimplemented` for durable sync matching), isolation groups, sticky,
  query-task delivery. Full runner re-run still pending for this change.
