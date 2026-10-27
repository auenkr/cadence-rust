# Matching isolation balancing and persistence path audit — 2026-10-27

Audited current source `9e719ab6e6c96d8c64d4880868a18ea602bad2ef` on
`t3/matching-isolation-parity-audit`. Scope: Go isolation-group assignment /
balancing, task add and poll forwarding, persisted task attributes, and
domain/task-list scoped production configuration. This is a source audit; no
Cargo/Go commands or mixed gates were run.

## Result

No concrete observable Go/Rust mismatch found in the audited isolation path.
Rust has the isolation balancer, production dynamic-config wiring, persistence
and reload path, active/standby add behavior, and both backlog and poll
forwarding paths. The code's own comments identify their Go counterparts.
This closes the bounded source-audit item from
`matching-lane-progress-20261027.md`; it does not close Matching acceptance.

## Production path map

| Behavior | Go source | Rust production source and flow |
|---|---|---|
| Isolation assignment balancing | `go/service/matching/tasklist/isolation_balancer.go`: `adjustWritePartitions`, `refreshGroups`, `calculatePartitionGoal`, `applyGroupChanges`, min groups/partitions constraints and greedy balancing. `adaptive_scaler.go`: `collectPartitionMetrics` / `fetchMetricsFromPartitions`. | `rust/crates/cadence-matching/src/matching_scaler.rs`: `IsolationBalancer::{adjust_write_partitions, partition_goal, apply_changes}`, minimum constraints, deterministic partition selection, add/remove and rebalance. `matching_registry.rs:1817-1900` collects per-partition and per-group metrics then invokes the scaler; `describe_scaled_partition` at 1734 serves local owners or uses the matching membership RPC. `matching_scaler_go_tests.rs` contains Go-oracle cases. |
| Scoped isolation/scaler configuration | `go/service/matching/config/config.go:188-250` binds task-list-info-filtered properties; `task_list_manager.go:1036-1126` captures domain/list/type into `TaskListConfig`; `adaptive_scaler.go:collectPartitionMetrics`. | `grpc_matching.rs:810-900` reads the named scaler and task-isolation properties using domain name, task list, and task type; creates `IsolationProvider` with domain isolation state, group list, poller manager and partition cache. `matching_registry.rs:230-245, 1180-1240` receives the providers per owner and constructs the reader/scaler. |
| Partition config persistence, update, propagation and reload | `task_list_manager.go:1333-1400` validates/equality-checks, persists versioned read/write maps, stops on ambiguous write failure, then refreshes old/new non-root read partitions. Startup reload is in `task_list_manager.go:242-302`; refresh/update methods at 362-497. | `matching_registry.rs:1968-2123` updates and refreshes persisted config/cache; owner initialization loads `PartitionCache` (around 1180) before constructing reader. RPC handlers are `grpc_matching.rs:3244-3450`. `matching_scaler.rs` only returns a proposed map; registry update is the persistence boundary. |
| Active, standby, forwarded add; original task metadata | `task_list_manager.go:1412-1475`: non-forwarded arrival accounting; standby tasks persist without sync match; forwarded standby returns remote-sync-failed; active tasks try sync match; a forwarded miss is not persisted by the parent. `forwarder.go:152-167` copies `PartitionConfig` into forwarded add requests. Backlog retry is `task_reader.go:2516-2550`. | `grpc_matching.rs:2338-2445` and `2562-2670` construct `TaskData` from the incoming `partition_config`; non-forwarded adds report arrival QPS; active domain tries sync match; forwarded miss returns remote-sync-failed; a non-forwarded miss enqueues. Standby follows the same final enqueue path. Queue rows retain original `TaskData`; reader retry uses stored row data. |
| Isolation selection, dispatch copy, and reload | `task_list_manager.go:1769-1810` selects only if isolation enabled, non-sticky, group known/not drained, recent poller exists, assigned to read partition, and task duration remains. `task_reader.go:2473-2505` buffers; `2590-2660` reselects at dispatch and falls back to shared matcher. `newInternalTask` carries dispatch partition config. | `grpc_matching.rs:878-953` wires the live selector; `cadence-common/src/task_isolation.rs:1-95` implements Go policy. `matching_reader.rs:625-690, 941-960` selects on buffer admission and every dispatch attempt, falls back to shared channel, and only mutates the delivered clone through `dispatch_partition_config`; `matching_reader.rs:1047+` checks reload. Persisted queue retry data remains untouched. |
| Poll attribution and forwarding | `task_list_manager.go:711-743` attaches poller group to matcher poll. `forwarder.go:229-267` sends isolation group through parent poll. | `grpc_matching.rs:1830-1900, 2080-2160` creates poll lifecycle with request isolation group. `matching_dispatch.rs:730-760, 940-970` forwards poller ID and group in the Matching envelope. Parent request carries the group in `matching_dispatch.rs:115-205`; remote YARPC metadata and caller deadline are retained. |

## Follow the persisted task data

1. An active non-forwarded add may sync-match. If it does not, Rust enqueues
   `TaskData` containing the incoming workflow/run/schedule and partition
   config. A standby add skips sync match and enqueues the same data. A
   forwarded add is attempted only as sync match at the receiving parent; on
   failure that parent returns remote-sync-failed and leaves persistence to
   the child, as in Go.
2. Queue reader loads the persisted task. It checks expiry, then resolves the
   stored `isolation-group` against current live domain isolation state,
   task-list scoped poller window/duration, and the currently loaded read
   partition assignment. Buffered tasks are reselected at dispatch, so a
   config change while buffered is observed. Unknown, drained, stale-poller,
   removed-assignment or expired-isolation tasks fall back to the shared
   channel.
3. Dispatch constructs a task copy with Go's internal task partition-config
   shape (`isolation-group`, `original-isolation-group`, `wf-id`). The durable
   queue row is not rewritten by selection/leak. If History fails to start a
   backlog task, retry appends the original persisted `TaskData`, preserving
   its isolation config. A fresh task-list owner reloads partition config from
   the matching task store before processing rows.
4. Per-group QPS is reported from the incoming persisted partition config for
   non-forwarded adds. Poll manager tracks outstanding group polls plus recent
   group pollers. Describe per-partition status exports group QPS/poller count;
   scaler aggregates these values, computes assignment goals, persists config,
   and refreshes partition owners.

## Existing focused proof in source

- `matching_scaler_go_tests.rs`: table-driven Go oracle for scaler and
  isolation assignment behavior (including `adjust_write_partitions`, goals,
  min assignment and movement); source fixture is included in this module.
- `matching_reader.rs`: tests cover group buffer isolation, selection/leak
  without storage rewrite, retry data preservation, invalid/unknown group
  fallback, dispatch to known/default group, and repoll after config selector
  changes. See tests around lines 1897-2120 and 2178-2290.
- `matching_registry.rs`: tests cover scaled partition extraction,
  isolation-assignment metrics and config update/refresh behavior; see around
  lines 54-70, 1845 onward, and 1938-2123.
- `matching_dispatch.rs`: forwarding tests cover poll isolation-group
  propagation and forwarded add behavior, including request envelope and
  parent resolution; see around lines 730-760, 940-970 and forwarding tests
  around 2796 onward.
- `grpc_matching.rs`: handler tests cover incoming task partition config,
  standby/add handling, poll group attribution and partition config RPCs.

These are source-level/unit/oracle assertions and do not substitute for
runtime interoperability evidence.

## Remaining evidence (parent-owned; no gate dispatched here)

On the parent’s final immutable Matching pin, retain the existing serial
mixed-gate run URL, head SHA, conclusion and Rust binary SHA256 for scenarios
that exercise:

- group assignment under changing per-group QPS and poller activity, followed
  by config persistence, owner refresh/reload, and task delivery after restart;
- known-group, shared/default, removed/drained group, and task isolation
  timeout fallback with persisted task retry/recovery;
- active versus standby domain add, including a forwarded parent sync-match
  success and failure where only the child persists;
- child poll forwarding to a parent with poller ID and isolation group, then
  History start failure/retry and Matching owner restart.

Use existing matching mixed gates where their scenarios establish these facts;
do not invent another lane solely from this audit. This audit itself provides
no runtime proof and cannot accept Matching.
