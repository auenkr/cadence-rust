# history-ndctxn (plan item 6b)

Ported (Rust, wiring/assembly):
- `EngineNdcTransactionManager` (ndc_adapters.rs): real `TransactionManagerImpl` over `SqlTransactionManagerEnv` + `EventsReapplierImpl`; collaborators: current cluster, active cluster (via ActiveClusterManager), load_workflow.
- `reapply_events` (ndc_context_hooks.rs `reapply_via_engine`, wired through NdcWriter/EngineNdcWorkflow): Go `contextImpl.ReapplyEvents` for the locally-active case; pending-active skip.
- `request_type` returns 0 (Start) instead of -1; `verify_and_repair` Ok(false) (Go default verify prob 0).
- Advanced-visibility flag read from config (EngineCore.advanced_visibility_writing).
- Event-reapplied dedup set (in-memory, like Go appliedEvents) and add_workflow_execution_signaled in ndc_state.rs.

Gaps (explicit errors): `reset_workflow` collaborator (Go reset.WorkflowResetter), remote-cluster reapply (admin client), continue-as-new replication task merge, replication category in allocate_task_ids, flush_as_active/start_transaction failover. No new unit tests added beyond the renamed dedup test.

CI: https://github.com/auenkr/cadence-rust/actions/runs/43004907916 (green, package=cadence-history) on code head.
