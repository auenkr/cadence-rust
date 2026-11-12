# History: activecluster manager port (2026-11-12)

Branch `history-acmanager` (from master 62016da50).

## Done
- `rust/crates/cadence-history/src/wiring/assembly/active_cluster_manager.rs`: line-by-line port of `go/common/activecluster/manager.go` (`managerImpl`):
  - `get_active_cluster_info_by_cluster_attribute`, `get_active_cluster_info_by_workflow`,
    `get_active_cluster_selection_policy_for_workflow`, `get_active_cluster_selection_policy_for_current_workflow`.
  - `getClusterSelectionPolicy`: current-execution lookup when run id is empty, `GetActiveClusterSelectionPolicy` persistence read,
    thriftrw decode, nil policy => EntityNotExists (swallowed by the by-workflow lookups, propagated by the current-workflow one).
  - Policy cache: TTL 10s, max 1000 entries, key `domain:wf:run` (`PolicyCache`).
  - Non active-active domains return the domain-level cluster / failover version (policy lookups return None).
- Wired: one shared `DomainActiveClusterManager` in `HistoryEnv.active_cluster_manager` (Go resource-level manager) used by engine factory, task shard context, priority assigner and queue allocator. It implements engine `runtime::ActiveClusterManager` (new `active_cluster_selection_policy_for_workflow` method) and task `ActiveClusterManager`. `EngineStartHost::active_cluster_selection_policy_for_workflow` no longer returns `None`.
- Unit tests (ported from Go manager_test coverage): cluster-attribute table, by-workflow (non-AA, persisted policy, empty run id, missing policy/execution, persistence error, unknown attribute), policy-for-workflow, current-workflow, cache hit per run, cache TTL/capacity.

## Not done / notes
- `ClusterAttributeNotFound` message uses Rust Debug formatting of the attribute (unchanged from existing code), not Go's `%s`.
- Cache eviction at capacity is least-recently-used by access tick, not Go's exact LRU implementation.

## Evidence (head 68f66fb7a5c79810193a121629fe666047097eb8; later commits only touch memory/)
- rust-check (push) + rust-ci workflow on 05a3ad467: success (runs 43197007955, 43197008816)
- rust-ci on 68f66fb7a: https://github.com/auenkr/cadence-rust/actions/runs/43198728277 success
- history-mixed-gate mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43198706174 success, GATE SUMMARY failed: []
- history-mixed-gate cassandra/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43198713637 success, GATE SUMMARY failed: []
- history-mixed-gate postgres/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43198721366 success, GATE SUMMARY failed: []
