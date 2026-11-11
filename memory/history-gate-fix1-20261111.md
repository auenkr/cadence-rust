# History gate fix 1 (2026-11-11)
Branch history-gate-fix1 from 012fd8095. Lane: mysql, grpc, two_history=false.

## Run 1: https://github.com/auenkr/cadence-rust/actions/runs/42947578895 head 012fd8095 FAILURE
- Rust build + Go build ok; gate step failed; SDK start/echo, child, cancel, terminate, etc all failed.
- Rust History log: `start_workflow_execution: unknown failure detected when creating execution ... invalid thrift data: targetRunID: invalid UUID ""` (evidence/history-gate-fix1/run1-targetRunID.txt).
- Root cause: engine_tasks.rs `transfer_base` left target_run_id/target_domain_id empty; sqlblobs transfer-task encoder requires UUIDs. Go uses TransferTaskTransferTargetRunID 30000000-0000-f000-f000-000000000002.
- Fix: transfer_base sets target_domain_id=domain and target_run_id=dummy sentinel (cancel/signal tasks override).

## Run 2: https://github.com/auenkr/cadence-rust/actions/runs/42950453247 head b75e17937 FAILURE
- rust-ci cadence-history green on b75e17937: https://github.com/auenkr/cadence-rust/actions/runs/42950444095
- sdk-start-echo now passes; first failure sdk-complete-echo (deadline exceeded), every later step times out. Matching log shows zero AddDecisionTask calls.
## Run 3: https://github.com/auenkr/cadence-rust/actions/runs/42953588254 (head = tracing-enabled gate, RUST_LOG info) FAILURE
- History log: queue processors start for 5 shards, no task activity ever logged.
- Root cause: Rust assembly never calls the engine's NotifyNewTransferTasks/NotifyNewTimerTasks after create/update (Go shard/task_notifier.go notifyTasks). Queue wakes only on its slow poll interval so decision tasks were not dispatched within SDK timeouts.
- Fix: EngineCore.engine_slot + EngineCore::notify_tasks, called after successful create_workflow_execution (engine_start.rs) and update_workflow_execution (engine_cache.rs persist).

## Runs 4-9 (RUST_LOG debug traces added; all mysql/grpc/single-history)
| gate run | head | result |
|---|---|---|
| 42963556504 | ec0075e5f | FAIL: tasks reached worker, never finished |
| 42966420278 | 17d8a4d98 | FAIL |
| 42969195220 | 15a9d01e0 | FAIL: task filter hangs |
| 42972231837 | a89fbf977 | FAIL: domain_change_cb never starts |
| 42976236564 | dfd81a40d | FAIL: `executions_visibility` table missing in cadence DB |
| 42979556819 | 1a7410c49 | FAIL: 27/144 steps; decision task now dispatched, Rust Matching gets AddDecisionTask, polls, RecordDecisionTaskStarted ok |
(all under https://github.com/auenkr/cadence-rust/actions/runs/<id>)
rust-ci: 42963552820, 42966415238, 42969198836, 42972236390, 42975315906 (history green); 42979552423 (matching, 1a7410c49 green); history on 1a7410c49: 42979548183 was cancelled by concurrency, re-dispatched alone after final commit.

Fixes:
- Task filter deadlock: transfer and timer processors shared one TaskAllocator so the domain-update write lock deadlocked the read in verify_active_task. Go NewTaskAllocator(shard) per processor -> `task_allocator_factory` (dfd81a40d).
- Visibility writer used default DB; Go uses visibilityStore (cadence_visibility). `ConfiguredMatchingStores.history_visibility_sql_backend` + env.rs (1a7410c49).

## REMAINING FIRST FAILURE (run 42979556819)
`sdk-complete-echo` TimeoutType START_TO_CLOSE. SDK worker error:
`RespondDecisionTaskCompleted is not ported on the engine mutable state yet (decision command processor)`
from `rust/crates/cadence-history/src/wiring/assembly/engine_decision.rs` `handle_decision_task_completed` (explicit TODO).
Port needed: Go `service/history/decision/handler.go HandleDecisionTaskCompleted` + `task_handler.go` (command processor: CompleteWorkflow, ScheduleActivity, StartTimer, StartChild, etc.) + `checker.go` attribute validation, plus EngineMutableState add_* for those events (only decision scheduled/started/failed/timeout exist). ~3000 Go lines; all later SDK steps (complete-child, describe-closed, ...) cascade from this.
