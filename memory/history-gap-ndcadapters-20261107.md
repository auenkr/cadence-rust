# History gap: NDC adapters (branch history-gap-ndcadapters, 2026-11-07)

Status: PARTIAL. `UnwiredReplicator` is no longer wired; `ndc_adapters::NdcReplicator` is (rust/crates/cadence-history/src/wiring/assembly/ndc_adapters.rs).

## Done
- `EngineNdcClusterMetadata` (NdcClusterMetadata over engine ClusterMetadata; IsVersionFromSameCluster = same cluster name).
- `ThriftBatchDeserializer` (HistoryBatchDeserializer via cadence_thrift::history_blob).
- `EngineNdcEnv: HistoryReplicatorEnv` with real branch manager env (SqlBranchManagerEnv), workflow resetter env (SqlWorkflowResetterEnv), shard `set_current_time`, clock.
- `NdcReplicator: Replicator` converts engine request types to ndc ones, runs `HistoryReplicatorImpl::apply_events` / `ActivityReplicatorImpl::sync_activity` on spawn_blocking, maps NdcError -> HistoryError. Request validation, version-history checks, retry-task hints run for real.

## Still TODO(ndc-adapters) (each returns an internal error "TODO(ndc-adapters): ...")
1. `NdcExecutionCache` + `NdcWorkflow` + `NdcMutableState` over `EngineExecutionCache`/`EngineExecutionContext`. Blocker: engine uses `EngineMutableState` (snapshot-backed, interior mutability, engine_state.rs) whereas `execution::StateBuilder` replays onto `mutable_state::MutableStateBuilder` (JSON-ish); `NdcMutableState::get_version_histories` returns `&VersionHistories`, which `EngineMutableState` (Mutex inner) cannot lend. Needs a decision: replay-capable state shared by engine and ndc.
2. `NdcStateBuilder` over `execution::state_builder::StateBuilder` (needs 1).
3. `NdcStateRebuilder` over `execution::state_rebuilder::StateRebuilder` + `SqlHistoryPager` + `RebuildContext` impl (needs 1).
4. `NdcTransactionManager` = `TransactionManagerImpl` + `SqlTransactionManagerEnv` + `TransactionManagerCollaborators` impl (load_workflow, reset_workflow, active cluster) and an `EventsReapplier` impl (needs 1).
5. `HistoryReplicatorEnv::new_mutable_state` (needs 1).
6. `WorkflowContextHooks` impl for `SqlWorkflowContextStore` (build_state, flush_as_active, notify_*, merge_continue_as_new_replication_tasks, reapply_events).
7. Standby cluster delay is a constant (5 min) instead of dynamic config `StandbyClusterDelay`.
8. `Replicator` trait ignores contexts/deadlines (as before).

## CI
See final line below (filled after the run).
