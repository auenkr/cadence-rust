# History NDC port checklist (branch history-port-ndc)

Go go/service/history/ndc/ -> rust/crates/cadence-history/src/ndc/

| Go file | Rust file | Status |
|---|---|---|
| activity_replicator.go | activity_replicator.rs | ported (CI green) |
| branch_manager.go | branch_manager.rs | ported (CI green) |
| conflict_resolver.go | conflict_resolver.rs | ported (CI green) |
| events_reapplier.go | events_reapplier.rs | ported (CI green) |
| existing_workflow_transaction_manager.go | existing_workflow_transaction_manager.rs | ported (CI green) |
| history_replicator.go | history_replicator.rs | ported (CI green) |
| new_workflow_transaction_manager.go | new_workflow_transaction_manager.rs | ported (CI green) |
| replication_task.go | replication_task.rs | ported (CI green) |
| transaction_manager.go | transaction_manager.rs | ported (CI green) |
| workflow_resetter.go | workflow_resetter.rs | ported (CI green) |

Design: Go Context/MutableState/Workflow/ReleaseFn collapsed into `NdcWorkflow` trait objects (mod.rs); shard/domain deps via Env traits; Go recover -> guarded/finish_guarded.

Last green CI run: https://github.com/auenkr/cadence-rust/actions/runs/41900708590 (clippy + test, package=cadence-history), head SHA 842ef66ddd1f5b9a0b1d32889a9f603ea19e098d.

Notes: fixed pre-existing clippy suspicious_assignment_formatting in cadence-persistence sql_workflow_store.rs:201; ndc/mod.rs allows clippy::result_large_err (NdcError carries RetryTaskV2Error).
Open: Env traits (HistoryReplicatorEnv etc.) need adapters when shard/engine modules land; no ndc unit tests yet.
