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
