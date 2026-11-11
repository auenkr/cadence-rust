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
