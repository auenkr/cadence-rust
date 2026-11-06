# History gap: engine / reset / decision port (branch history-gap-engine)

Status: PARTIAL. Done = only the rows marked done. Task NOT complete.
CI run (cadence-history clippy+test, green): https://github.com/auenkr/cadence-rust/actions/runs/41907856028 on head before this memory commit (86ff42996eb9f4f267340ecb2bbb3520ca3341f5).

| Go | Rust | Status |
|---|---|---|
| record_decision_task_started / respond_decision_task_completed / failed | engine/decision_replication.rs (pre-existing delegation) | done |
| refresh_workflow_tasks | engine/mutable_state_admin.rs (pre-existing) | done |
| notify_tasks (NotifyNewTransfer/Timer/ReplicationTasks, hydrateReplicationTask) | engine/notify_tasks.rs | done (hydrator via trait ReplicationHydrator::hydrate_immediate, not yet bound) |
| reapply_events | engine/reapply_events.rs | done (EventsReapplier/WorkflowResetter traits unbound) |
| describe_workflow_execution | engine/describe_workflow_execution.rs | done (MutableState trait got new getters, unbound) |
| sync_shard_status NotifyTaskInfo | engine/decision_replication.rs | fixed |
| start_workflow_execution.go (1050) | - | TODO |
| signal_with_start | - | TODO |
| get_replication_messages / dlq_operations / poll_mutable_state / query_workflow / record_activity_task_started / record_child_execution_completed / reset_workflow_execution / signal_workflow_execution | existing Rust files, not re-audited line by line | TODO audit |
| register_domain_failover_callback (282) | - | TODO |
| history_engine.go (NewEngine, Start, Stop, createMutableState) | engine/runtime.rs | TODO |
| engine/interface.go | engine/mod.rs | TODO |
| reset/resetter.go (732) | reset/mod.rs, history.rs | TODO |
| decision/checker.go (958), handler.go (948), task_handler.go (1241) | decision/*.rs | TODO |
| replace TODO(link) facades in engine/deps.rs, types.rs with real types | - | TODO |

Note: new trait methods on MutableState (get_last_write_version, is_resource_duplicated, get_pending_*, get_completion_event) have no implementor yet; the real execution MutableStateBuilder must implement them.

## gap-engine3 (2026-11-06): start_workflow_execution + register_domain_failover_callback
- Ported from Go engineimpl: `engine/start_workflow_execution.rs` (start, signal_with_start, reuse policy, terminate-and-start, create-failure cleanup) and `engine/register_domain_failover_callback.rs`.
- Real Go logic lives in the compiled engine tree; environment access goes through host traits `WorkflowStartHost` and `DomainFailoverHost` (fields `start_host`, `failover_host` on `HistoryEngine`).
- CI: https://github.com/auenkr/cadence-rust/actions/runs/42221532971 head 9c28133faa9f30f016184e8736e28344c7e20bc6 conclusion success (rust-ci.yml, package=cadence-history, branch history-gap-engine3).
- Remaining gaps (UNVERIFIED, assembly/resources/persistence trees are not compiled):
  - No implementors yet for `WorkflowStartHost`, `DomainFailoverHost`; `build_engine` (wiring/assembly/engine_factory.rs) does not set the new fields.
  - `EngineMutableState` lacks add_workflow_execution_started_event / add_first_decision_task_scheduled / add_decision_task_scheduled_event; `EngineExecutionContext` lacks update_workflow_execution_as_active / clear.
  - queues.rs `notify_new_task` signature staleness; domain_cache.rs engine_entry should set failover_end_time.
  - Remaining history_engine.go methods and TODO(link) facades in deps.rs/runtime.rs/types.rs/deps_ms.rs not yet replaced.
