# Original-Go control: SDK endpoint wiring failure

https://github.com/auenkr/cadence-rust/actions/runs/40472428123 failed at source
9a3673d0de3284e3bda434c4f722a412f753eb75. All four Go services were running.
Adaptive config increased12->72->216 partitions, with matching QPS samples
retained in go-scaler-observations.json. This independently shows that hundreds
of partitions under1RPS threshold are not uniquely Rust behavior.

The SDK never started its retry workflow: public WorkflowService calls targeted
Matching's TChannel port, producing `no handler for service cadence-frontend and
method WorkflowService::StartWorkflowExecution`. This is a control fixture
wiring defect, not a Go/Rust retry comparison. Parent corrected the harness to
pass Frontend TChannel separately from the Matching endpoint used for routing
evidence. Retained actual SDK log, source/Go binary hashes, topology and cleanup.
No adaptive completion or SDK deadline assertion was changed.
