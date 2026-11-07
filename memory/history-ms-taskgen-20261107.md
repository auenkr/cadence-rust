# history-ms-taskgen (2026-11-07)

Go source: go/service/history/execution/mutable_state_task_generator.go.
Rust: rust/crates/cadence-history/src/execution/mutable_state_task_generator.rs (generic generator already existed, audited line by line vs Go; all 14 Generate* funcs match).

Changes:
- Added `TaskKind::UserTimer` / `TaskKind::ActivityTimeout` (Go timer_sequence UserTimerTask/ActivityTimeoutTask).
- Added `TypedTaskGeneratorState` adapter implementing `TaskGeneratorState` + `TimerSequenceState` for `TypedMutableState`, plus `TaskGeneratorDomains` (Go domainCache subset).

Caller notes (for owners of mutable_state_methods_*):
- Build `TypedTaskGeneratorState::new(&mut state, &domains, &mut rng)`; set `decision_schedule_to_start_timeout_nanos` (Go GetDecisionScheduleToStartTimeout), seed `activity_timer_status` and `signal_child_only`; run `MutableStateTaskGenerator { state: &mut adapter }.generate_*`; then drain `transfer_tasks`/`timer_tasks` into task categories (Go AddTransferTasks/AddTimerTasks) and write back `activity_timer_status` (base ActivityInfo lacks TimerTaskStatus).
- GAP: Rust `SignalExternalWorkflowExecutionInitiatedEventAttributes` lacks Go `ChildWorkflowOnly`; hence `signal_child_only` side map.
- GeneratedTask -> persistence `Task` blob encoding is not done here (caller/persistence layer).

CI evidence: see below.
