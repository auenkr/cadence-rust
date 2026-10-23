# ListTaskListPartitions owner lookup: Go vs Rust parity check (2026-10-24)

Branch: `matching-list-partitions-owner-20261024` (from origin/master bafd65a77)
Scope: which resolver picks `OwnerHostName` for each partition key in ListTaskListPartitions.
Code changes: none. No cargo run, no CI run (nothing to verify).

## Verdict

**No divergence in any mode.** Rust's use of the shard router for owner lookup is the
Go-equivalent of `membershipResolver.Lookup`, because Go's membership resolver is itself
shard-distributor (SD) aware for Matching. Rust matches Go in all three modes.

## Go call path

- `handler/engine.go:1158` `ListTaskListPartitions` → `listTaskListPartitions` (1178)
  → `getHostInfo(partition)` (1313) → `e.membershipResolver.Lookup(service.Matching, partitionKey)`.
- Error is discarded: `host, _ := e.getHostInfo(partition)` (1192). Owner becomes `""`, RPC succeeds.
- `cmd/server/cadence/server.go:201-215`: the Matching hash ring is wrapped by
  `wrapHashRingsWithShardDistributor` (`server.go:305-322`) into
  `membership.NewShardDistributorResolver(spectator, excludeShortLived, percentageOnboarded, ring, ...)`
  **before** `membership.NewResolver(...)`. So `membershipResolver.Lookup` for Matching always goes
  through `shardDistributorResolver.Lookup` (`common/membership/sharddistributorresolver.go:86`).
- `shardDistributorResolver.Lookup`:
  - `TaskListExcludedFromShardDistributor(key, percentageOnboarded, excludeShortLived)` → true → `ring.Lookup(key)`.
  - spectator nil → `ring.Lookup(key)`.
  - else → `spectator.GetShardOwner` (1s timeout), address = `JoinHostPort(hostIP, tchannel port)`.
- Spectator is created only when the SD client exists and `ShardDistributorMatchingConfig.Namespaces` is non-empty (`server.go:156-168`).

## Rust call path

- `grpc_matching.rs:2821` `list_task_list_partitions` → `task_list_partition_metadata` (983)
  → `owner_address_for_task_list` (971).
- Router set: `grpc_matching.rs:945 with_shard_router`. Only set from `matching_runtime.rs:293-344`,
  which runs only in SD mode (executor + spectator started).
- With router → `MatchingShardRouter::resolve` (`matching_shard_routing.rs:175`), which is
  exclusion check → spectator, else ring. Without router → `membership.owner(name)` (ring only).
- Error handling: `owner_address_for_task_list(&key).await.ok().flatten().unwrap_or_default()`
  (`grpc_matching.rs:998-1004`). Matches Go's discarded error. Covered by the existing test
  `engine_apis_partition_spectator_failure_is_empty_owner_like_go` (`matching_rpc_tests.rs:4075`).
- Partition count: `grpc_matching.rs:916` sets `matching.partition_count` in production, so the
  Rust default of 1 is not reached in production.

## Per-mode comparison

| Mode | Go resolver path | Rust path | Result |
|---|---|---|---|
| SD disabled (no spectator / no namespaces) | wrapper with nil spectator → `ring.Lookup` for every key | no router → `membership.owner` = `ring.lookup` | Same owner (ring) |
| SD enabled, percentage 0 | all keys excluded → `ring.Lookup` | router, all keys excluded → `resolve_from_ring` | Same owner (ring) |
| SD enabled, percentage 100 / UUID exclusion off | spectator `GetShardOwner` per key | router → spectator `get_shard_owner` | Same owner (SD) |
| SD enabled, partial (e.g. 50) | per-key farmhash `hash%100 < pct` decides SD vs ring | same predicate, `task_list_excluded_from_shard_distributor` (routing.rs:305) with Go vectors in tests | Same per key |

Partial-mode key selection: Go hashes the partition key string (including the
`/__cadence_sys/<name>/<n>` prefix) with farm.Fingerprint64. Rust passes the same key string
(`name.partition_name(partition)`) to the same predicate, and has test vectors
(`matching_shard_routing.rs:466`).

Owner address format: Go SD = `JoinHostPort(hostIP, tchannel)`; Rust
`resolved_from_shard_owner` does the same, including the port-0 fallback for a missing tchannel
entry. Go ring = `HostInfo.addr`; Rust = `Host.address`.

Failure cases, both `""` owner with the RPC succeeding in both languages:
- spectator error or timeout
- missing `hostIP` metadata
- Go ring lookup error (Rust `owner()` returns an error, which is also swallowed by `.ok()`)

## Caveats / not checked

- Only the owner-lookup decision was compared. Not checked: exact `record_onboarding` / routing-path
  metrics, and the spectator `Enabled` gating (`percentage > 0`), which affects the spectator itself
  rather than the per-key choice.
- Not run: no test or build (no code change).
