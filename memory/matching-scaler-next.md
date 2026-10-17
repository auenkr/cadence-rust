# Matching adaptive scaler + isolation balancer (2026-10-17)

Prior increments: `a9531588d`, `ca062c861`, `d501f3779`, `79960a154`
(single-node matching complete, 31/31 stages). This increment closes the
largest remaining genuine single-process gap from the parity audit: Go
`tasklist/adaptive_scaler.go` and `tasklist/isolation_balancer.go`.

## State

- `matching_scaler.rs` (new) ports Go's partition autoscaler and the
  per-isolation-group balancer as pure functions plus a per-root daemon state
  (`Scaler { over, under, balancer }`):
  - `Sustain` mirrors `clock.Sustain` (continuous hold; `check_and_reset`
    re-arms after firing).
  - `write_partition_count` mirrors `calculateWritePartitionCount`:
    upscale threshold `current * upscaleRPS`, downscale threshold
    `(current-1) * upscaleRPS * downscaleFactor` (hysteresis 0.75), target
    `ceil(qps/upscaleRPS)` clamped to `minWritePartitions` (default 1).
  - `adjust_write_partitions` rebuilds the dense `0..n-1` write map preserving
    existing entries; `adjust_read_partitions` unions old reads with new
    writes then drains trailing read-only **and** empty partitions from the
    top, stopping at the first live partition (a missing describe breaks like
    Go's error branch).
  - `IsolationBalancer` ports group-goal math, `minGroupsPerPartition`
    (`isolationGroupsPerPartition`, default 2), `ensureMinimumGroups`, and the
    greedy remove/add passes with deterministic id ordering.
- `matching_scaler.rs` defaults are Go's `dynamicproperties` defaults:
  `partitionUpscaleRPS=200`, `partitionDownscaleFactor=0.75`,
  upscale/downscale sustain 60s/120s, `adaptiveScalerUpdateInterval=15s`,
  `isolationGroupsPerPartition=2`, group sustains 60s, poller window 2s.
- `QueueRegistry::with_scaler_config(ScalerConfigProvider)` installs a
  per-domain/task-list/type provider. A scaler daemon spawns on each **root
  normal** owner (`matching_registry.rs` `get`), mirroring
  `task_list_manager.go:230-233` (root + `TaskListKindNormal` only).
- `scaler_loop` sleeps `adaptiveScalerUpdateInterval` per tick, rereads live
  config, and calls `scaler_update`. It holds only a **weak** handle on the
  owner state (same pattern as the forward resolver) so it can neither
  resurrect a dead owner through `get` nor pin the registry/task store past
  its lifetime; `Owner::scaler` also stores an `AbortHandle` aborted on
  unload/shutdown/drop.
- `scaler_update`: disabled when either `enableAdaptiveScaler` **or**
  `enableGetNumberOfPartitionsFromCache` is off (Go `adaptive_scaler.go:148`
  gate) and resets the sustains. Metrics come from the existing QPS tracker:
  - isolation-assignment mode (`enablePartitionIsolationGroupAssignment`)
    fans out over read partitions, summing per-partition QPS + group QPS +
    poller presence, and marks each partition read-only iff absent from the
    write map; "empty" uses `TaskListStatus.empty` when
    `enablePartitionEmptyCheck`, else backlog hint `== 0`.
  - otherwise `assumeEvenQPS`: total = root arrival QPS × write-partition
    count.
  Changes publish through the existing versioned `update_partition_config`
  path (`matching_partition.rs`), so roots propagate to non-root partitions
  exactly like manual updates.
- Manual-update guard: `grpc_matching.rs`
  `update_task_list_partition_config` rejects with
  `invalid_argument("Manual update is not allowed because adaptive scaler is
  enabled.")` when the scaler is enabled, matching `handler/engine.go:1254`.
  This check runs before the kind/config checks, like Go.
- Config plumbing: `dynamic_config::task_list_float` added for
  `matching.partitionDownscaleFactor`. The scaler provider reads every
  property from the single admin-lock snapshot; it must not call the
  standalone `cache_enabled` provider (that would re-lock the same
  non-reentrant store mutex).

## Verified

- 7 `matching_scaler` unit tests (sustain hold/re-arm, upscale/downscale
  thresholds + min clamp, dense write rebuild, read-drain stop, single-partition
  reset, new-group spread, unsustained-group exclusion).
- `matching_rpc_adaptive_scaler_upscales_write_partitions_on_load`: live
  config enables the scaler, a 12-add burst drives root QPS above `1*1`, and
  the daemon grows the dense write map to ≥2 with all write ids present in
  reads.
- `matching_rpc_manual_partition_update_rejected_while_scaler_enabled`.
- Full `cadence-server --lib`: 123 passed.
- Strict `cargo clippy -p cadence-server --all-targets -- -D warnings`: clean.

## Remaining matching gaps (from the parity audit, not yet ported)

- Standby task completion (`task_completer.go:92`): no `CompleteTaskIfStarted`
  (History `DescribeWorkflowExecution` started-check, 24h EntityNotExists
  guard, `errDomainIsActive`→`MustOffer` fallback). Only reachable for global
  domains whose active cluster is elsewhere.
- Strong/sticky query version gates (`engine.go:790-800,1064-1090`): needs the
  `versionChecker`/`supportedClients` infra (frontend-level, not yet ported).
- Handler error taxonomy (`handler/context.go:83-139`): 13 typed errors +
  counters collapsed to generic `Status`.
- Semaphore manager/registry lifecycle (`semaphore_registry.go`,
  `semaphore_manager.go`): startup scan, idle unload, hold reconciliation.
- Domain-failover poller disconnect (`engine.go:1707-1785`): no
  `domainChangeCallback`.
- Thrift matching transport (gRPC only today).
- Multi-host: cross-host forwarding transport, ringpop membership/ownership,
  shard-distributor autoscaling (out of single-node scope).
