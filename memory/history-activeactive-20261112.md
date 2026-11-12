# history-activeactive (2026-11-12)

## Done
- Go `DomainCacheEntry.GetActiveClusterInfoByClusterAttribute` ported as `DomainCacheEntry::get_active_cluster_info_by_cluster_attribute` (engine/deps.rs): nil attribute -> domain-level cluster + domain failover version; unknown attribute / no ActiveClusters -> not found.
- `EngineStartHost::active_cluster_info_by_cluster_attribute` (wiring/assembly/engine_start.rs) now = Go `activecluster.managerImpl.GetActiveClusterInfoByClusterAttribute`: returns `ClusterAttributeNotFound` instead of taking the first entry; failover version is per attribute.
- `HistoryDomainCache::engine_entry` now carries all attribute scopes/clusters/failover versions + domain failover version.
- `query_workflow` (engine/query_workflow.rs) uses the same lookup (Go: GetActiveClusterInfoByClusterAttribute, default active on error).
- Tests: `query_decision_task_dispatch_complete_active_active` (Go TestQueryWorkflow_DecisionTaskDispatch_Complete_ActiveActive), `query_active_active_attribute_active_elsewhere_goes_direct`, `start_unknown_cluster_attribute_is_rejected`, unit table `get_active_cluster_info_by_cluster_attribute` (Go activecluster manager_test lookup cases).
- Other Rust lookups checked: task_allocator/standby (queue) use workflow-based lookups with their own traits; no first-entry shortcut found.

## Not done
- Go manager tests for GetActiveClusterInfoByWorkflow / selection-policy cache (persistence-backed manager) not ported: Rust has no activecluster manager impl for them (engine returns `None` policy in `active_cluster_selection_policy_for_workflow`).

## Evidence
- rust-ci package=cadence-history success, head 5948eca04: https://github.com/auenkr/cadence-rust/actions/runs/43191932040

Final head 1f734f7792569cb5c46ec8653c6161abc5edc8b5 (after merging origin/master):
- mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43193296898 success, GATE SUMMARY failed: []
- cassandra/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43193304548 success, GATE SUMMARY failed: []
- postgres/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43193309875 success, GATE SUMMARY failed: []
- rust-ci full workspace: https://github.com/auenkr/cadence-rust/actions/runs/43193315246 success
(The evidence commit itself only touches memory/.)

## Post-merge master validation (master 62016da50, 2026-11-12)
- Full workspace rust-ci + all 12 history-mixed-gate combos: runs 43195643764..43195708772, all success.
