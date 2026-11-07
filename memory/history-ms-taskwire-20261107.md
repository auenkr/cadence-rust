# history-ms-taskwire (2026-11-07)

Wired Go `e.taskGenerator.Generate*` calls into TypedMutableState methods (rust/crates/cadence-history/src/execution).

Infra: `TypedMutableState` gained `transfer_tasks`, `timer_tasks` (Vec<GeneratedTask>; Go AddTransferTasks/AddTimerTasks targets) and `task_gen_env: TaskGenEnv` (domain cache Arc, deletion jitter range, schedule-to-start config, xorshift rng). `run_task_generator(sched_to_start_nanos, f)` in mutable_state_methods_common.rs builds `TypedTaskGeneratorState`, runs the generator, drains tasks.

Wired:
- completed/failed/timedout/canceled replicate -> GenerateWorkflowCloseTasks
- request-cancel-external initiated -> GenerateRequestCancelExternalTasks
- start-child initiated -> GenerateChildWorkflowTasks
- signal-external initiated -> GenerateSignalExternalTasks
- add_activity_task_scheduled_event -> GenerateActivityTransferTasks; retry_activity -> GenerateActivityRetryTasks
- decision: replicate_decision_task_scheduled_event (unless bypass), replicate_transient_decision_task_scheduled -> GenerateDecisionScheduleTasks; replicate_decision_task_started_event -> GenerateDecisionStartTasks; add_binary_checksum_if_not_exists -> GenerateWorkflowSearchAttrTasks (replaces env.needs_search_attr_tasks flag, now unused)

Not wired / gaps:
- Terminated and continue-as-new close tasks (Go builder.go 1220/1355) and GenerateWorkflowResetTasks, GenerateActivityTimerTasks/UserTimerTasks (closeTransaction, builder.go 2142-2166): not in the listed files / not ported yet.
- methods_timer.go has no generator calls in Go.
- `replicate_activity_task_scheduled_event` has no skipTaskGeneration param (behaves as skip=true); add_ generates. Activity local dispatch skip not wired.
- Signal ChildWorkflowOnly missing from Rust attributes -> child_only always false.
- `activity_timer_status` is discarded (base ActivityInfo lacks TimerTaskStatus).
- GeneratedTask -> persistence Task blob encoding not done (tasks kept as GeneratedTask).
- Close tasks need TaskGenEnv.domains configured for retention (unset => 1 day default); cross-domain lookups error if unset.
- started.rs not touched (other worker).

CI: https://github.com/auenkr/cadence-rust/actions/runs/42339691238 head 07b09b43f73bbdc3fb703938877613704f1d67e4 conclusion success (-p cadence-history, clippy+tests)
