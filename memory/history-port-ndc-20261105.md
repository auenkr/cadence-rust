# History NDC port checklist (branch history-port-ndc)

Go go/service/history/ndc/ -> rust/crates/cadence-history/src/ndc/

| Go file | Rust file | Status |
|---|---|---|
| activity_replicator.go | activity_replicator.rs | ported (unverified) |
| branch_manager.go | branch_manager.rs | ported (unverified) |
| conflict_resolver.go | conflict_resolver.rs | ported (unverified) |
| events_reapplier.go | events_reapplier.rs | ported (unverified) |
| existing_workflow_transaction_manager.go | existing_workflow_transaction_manager.rs | ported (unverified) |
| history_replicator.go | history_replicator.rs | ported (unverified) |
| new_workflow_transaction_manager.go | new_workflow_transaction_manager.rs | ported (unverified) |
| replication_task.go | replication_task.rs | ported (unverified) |
| transaction_manager.go | transaction_manager.rs | ported (unverified) |
| workflow_resetter.go | workflow_resetter.rs | ported (unverified) |

Design: Go Context/MutableState/Workflow/ReleaseFn collapsed into `NdcWorkflow` trait objects (mod.rs); shard/domain deps via Env traits; Go recover -> guarded/finish_guarded.

Last green CI run: none yet.
