# history-ndcgaps (2026-11-11)

Branch `history-ndcgaps` from master 95a558c16.

## Item 1: NDC `reset_workflow` collaborator (done, CI pending)
`EngineTransactionCollaborators::reset_workflow` (ndc_adapters.rs) now runs `EngineWorkflowResetter`
(engine_resetter.rs, Go `reset/resetter.go`). The current NDC workflow exposes its engine
context through the new `NdcWorkflow::engine_context` / `NdcCheckoutContext::into_any`. The collaborator
loads the engine state from it and builds a `ResetWorkflow` request. It then clears the NDC workflow,
since the engine write bypasses the NDC in-memory state.

## Remaining
2. continue-as-new replication task merge
3. replication category in allocate_task_ids
4. flush_as_active
5. remote-cluster reapply via admin client

## CI
(see below, updated per push)
