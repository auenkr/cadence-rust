# History gate fix 1 (2026-11-11)
Branch history-gate-fix1 from 012fd8095. Lane: mysql, grpc, two_history=false.

## Run 1: https://github.com/auenkr/cadence-rust/actions/runs/42947578895 head 012fd8095 FAILURE
- Rust build + Go build ok; gate step failed; SDK start/echo, child, cancel, terminate, etc all failed.
- Rust History log: `start_workflow_execution: unknown failure detected when creating execution ... invalid thrift data: targetRunID: invalid UUID ""` (evidence/history-gate-fix1/run1-targetRunID.txt).
- Root cause: engine_tasks.rs `transfer_base` left target_run_id/target_domain_id empty; sqlblobs transfer-task encoder requires UUIDs. Go uses TransferTaskTransferTargetRunID 30000000-0000-f000-f000-000000000002.
- Fix: transfer_base sets target_domain_id=domain and target_run_id=dummy sentinel (cancel/signal tasks override).
