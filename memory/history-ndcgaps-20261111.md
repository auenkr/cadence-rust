# history-ndcgaps (2026-11-11)

Branch `history-ndcgaps` from master 95a558c16. All 5 items are implemented. CI: run 43052491721 (head f15d5f482, cadence-history clippy+test) concluded SUCCESS (https://github.com/auenkr/cadence-rust/actions/runs/43052491721). Merged master e63ced4ba (concurrency fix) -> head 7673dd934; re-dispatched run 43053825646 concluded SUCCESS (https://github.com/auenkr/cadence-rust/actions/runs/43053825646).

## Done
1. NDC `reset_workflow` collaborator (ndc_adapters.rs): runs `EngineWorkflowResetter`. The engine context comes from the new `NdcWorkflow::engine_context` / `NdcCheckoutContext::into_any`, and the engine state is loaded from it. The NDC workflow is cleared afterwards. Head 5602af413, run 43051922719 (that run was the cancelled push-triggered one; the dispatched run supersedes it).
2. Continue-as-new replication task merge (ndc_context_hooks.rs `merge_continue_as_new_replication_tasks`), using `replication_task_from/to_struct`. Go errors are preserved.
3. Replication category in `allocate_task_ids`: ids allocated last through `allocate_transfer_ids` (Go `allocateTransferIDsLocked`). Head ac3fd3c67, run 43052153743 (items 2+3).
4. `flush_as_active`: `EngineNdcWriter::write_update` with UpdateCurrent / Active / New (Go `UpdateWorkflowExecutionAsActive`). Head c2c06a1ae, run 43052206201.
5. Remote-cluster reapply: new `AdminClient::reapply_events` (deps.rs), implemented by `GrpcAdminClient`. New `RemoteAdminClients` (replication_clients.rs) is held in `EngineCore.remote_admin_clients`. `reapply_via_engine` sends a ThriftRW blob to the remote active cluster.

## Gaps and notes
- No new unit tests.
- Replication tasks allocated here are not announced through `notify_tasks` (transfer and timer only). Go notifies the replication task notifier too; check this.
- The collaborator reset path loads the engine state from the context; this assumes the persisted state is clean when called (true for the transaction manager).
