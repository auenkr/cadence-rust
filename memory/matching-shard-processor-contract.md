# Matching shard processor contract

Source comparison for the Matching-side adapter between the shard-manager executor and the existing Rust `QueueRegistry`. This work is source-only; no build, tests, or runtime wiring was run or changed.

## Pinned Go contract

References are the pinned shard-manager module at `github.com/cadence-workflow/shard-manager@v0.0.0-20260610143419-4bef35311802` and Cadence Matching source:

- `service/matching/handler/engine.go:221-316` installs a `tasklist.ShardProcessorFactory` and chooses configured `TTLReport`, defaulting to one minute when zero.
- `service/matching/tasklist/shard_processor.go:38-48` constructs a processor with initial status READY, zero load, and construction-time `reportTime`.
- `shard_processor.go:56-60` makes `Start` a no-op: Matching task-list managers are still started by normal Matching RPC flow.
- `shard_processor.go:64-70` makes `Stop` call `TaskListRegistry.ManagersByTaskListName(shardID)`, then `Stop()` every result.
- `shard_processor.go:72-86` retains initial load zero until `reportTime + TTLReport < now`; `reportTime` is never updated, so after expiry `getShardLoad` runs on every report. The load is the sum of `QueriesPerSecond()` across managers.
- `task_list_registry.go:53-71,130-139` indexes each manager by its exact `Identifier.GetName()` string. The lookup does not normalize root/partition names, and spans all domain IDs, task types, and kinds registered under that exact name.
- `task_list_manager.go:416-418` defines `QueriesPerSecond()` as the manager's QPS tracker value. `Stop` unregisters the manager and synchronously signals its child lifecycle components before returning.
- Pinned shard-manager `client/executorclient/clientimpl.go:248-326` removes processors synchronously on assignment loss, schedules processor Stop asynchronously, and schedules Start asynchronously. `stopShardProcessors` waits Stop completion before the DRAINING heartbeat on executor stop/drain.

## Rust adapter contract

`QueueRegistryShardProcessorFactory` receives the existing registry and signed `report_ttl_nanos`; it creates only the processor callback object. `start` is no-op. Reports preserve READY/0 at construction, use the strict signed-nanosecond TTL comparison, and after expiry sum current QPS for live owners whose `TaskListKey.name` exactly equals the shard key.

`QueueRegistry::stop_task_list_name` detaches existing matching owners before finishing each owner, across domains and task types, without task-list root normalization. It reuses existing `Owner::finish` for writer/reader/queue stop, final acknowledgement and garbage collection, then waits for reader loops through `owner_work`. It continues cleanup for all owners after an individual persistence error and returns the first error. The callback logs cleanup errors because `ShardProcessor::stop` has no error return and does not complete until the registry stop future has finished.

No new queue, writer, reader, dispatcher, lease, or ownership state is created by the factory. After stop, normal registry access can recreate owner state from persistence once the executor's separate ownership gate permits it.

## Source-only regression tests

`matching_shard_processor.rs` now includes tests for (1) strict TTL equality with a controlled monotonic clock and the original construction-time origin remaining in force after an expired report, (2) live AddTask QPS flowing through `grpc_matching::report_arrival` into the shard report, including the isolation-group tracker, (3) Start creating no owners, (4) exact physical-name stop spanning domains and task types while the root and a sibling partition remain loaded, and (5) reassignment retrieving an owner again through the same registry and SQLite store. The stop test observes old queues stopped and their owners detached before recreation; the implementation path additionally calls `Owner::finish` and awaits `owner_work` reader joins.

QPS source audit: Go `task_list_manager.go:553-567` records one arrival for non-forwarded AddTask calls, using `ReportGroup` for the isolation key and `ReportCounter` otherwise; `:416-418` defines `QueriesPerSecond` as that tracker. Rust `grpc_matching.rs:81-95` has the matching `report_arrival` helper, called under `!forwarded` at the decision and activity AddTask paths (`:1600`, `:1735`); `QueueRegistry::qps_for_task_list_name` sums each matching live owner's existing tracker. The tests and source changes are committed without running builds or tests; execute the focused Rust tests before treating this as validated evidence.

## Runtime integration responsibility

The runtime owner supplies the existing `QueueRegistry` and `ShardDistributorMatchingConfig::report_ttl_nanos()` to `QueueRegistryShardProcessorFactory`, passes the factory to `ShardDistributorExecutor`, and retains the existing registry as the single owner of queue state. The executor remains responsible for assignment callbacks; the Matching RPC path remains responsible for consulting that assignment before using the queue registry. This branch does not wire `main.rs`, `matching_runtime.rs`, or config.
