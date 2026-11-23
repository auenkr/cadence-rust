# Master validation batch — 2026-11-14d

Branch: `master-validation-20261114d`  
Validated source head: `15e01dd83e47fab1c7ea793c46b82fde80be4448` (origin/master at branch creation)  
Workflow dispatches: 21 total (1 Rust CI, 12 History combinations, 6 Frontend two-cluster cases, 2 History replication directions).

Mixed gates pass only when the Actions run concluded `success` and its log contains `GATE SUMMARY failed: []`. Rust CI passes by workflow conclusion.

| Workflow | Inputs | Run | Conclusion / gate evidence |
|---|---|---|---|
| rust-ci.yml | Full workspace; no package input | [43550173932](https://github.com/auenkr/cadence-rust/actions/runs/43550173932) | success |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=true | [43550174356](https://github.com/auenkr/cadence-rust/actions/runs/43550174356) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=false | [43550173870](https://github.com/auenkr/cadence-rust/actions/runs/43550173870) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=true | [43550174122](https://github.com/auenkr/cadence-rust/actions/runs/43550174122) | **failure**; DLQ bad-type probe plus recovery driver timeout and recovery workflow checks (details below) |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=false | [43550174017](https://github.com/auenkr/cadence-rust/actions/runs/43550174017) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true | [43550174093](https://github.com/auenkr/cadence-rust/actions/runs/43550174093) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=false | [43550174913](https://github.com/auenkr/cadence-rust/actions/runs/43550174913) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=true | [43550174295](https://github.com/auenkr/cadence-rust/actions/runs/43550174295) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=false | [43550174169](https://github.com/auenkr/cadence-rust/actions/runs/43550174169) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=true | [43550174080](https://github.com/auenkr/cadence-rust/actions/runs/43550174080) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=false | [43550174118](https://github.com/auenkr/cadence-rust/actions/runs/43550174118) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=true | [43550174099](https://github.com/auenkr/cadence-rust/actions/runs/43550174099) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=false | [43550173996](https://github.com/auenkr/cadence-rust/actions/runs/43550173996) | **failure**; `GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43550174289](https://github.com/auenkr/cadence-rust/actions/runs/43550174289) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_frontend_active=true, kafka=false | [43550174848](https://github.com/auenkr/cadence-rust/actions/runs/43550174848) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43550174139](https://github.com/auenkr/cadence-rust/actions/runs/43550174139) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_frontend_active=true, kafka=false | [43550174506](https://github.com/auenkr/cadence-rust/actions/runs/43550174506) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43550174481](https://github.com/auenkr/cadence-rust/actions/runs/43550174481) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_frontend_active=true, kafka=false | [43550174260](https://github.com/auenkr/cadence-rust/actions/runs/43550174260) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=A, kafka=false | [43550174141](https://github.com/auenkr/cadence-rust/actions/runs/43550174141) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=B, kafka=false | [43550174380](https://github.com/auenkr/cadence-rust/actions/runs/43550174380) | success; `GATE SUMMARY failed: []` |

## Failure details

All 12 History gate runs concluded `failure` and reported the same failed check:

```text
GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type']
```

The mysql/tchannel/two_history=true lane, [43550174122](https://github.com/auenkr/cadence-rust/actions/runs/43550174122), ran substantially longer and reported additional recovery failures:

```text
GATE SUMMARY failed: ['admin-probe-get-dlq-replication-messages-bad-type', 'sdk-recover-driver-timeout', 'sdk-recover-killprimary-start-activity-timeout', 'sdk-recover-killprimary-start-signal', 'sdk-recover-killprimary-raw-start', 'sdk-recover-killprimary-activities-running', 'sdk-recover-driver-exit']
ERROR cadence_history::handler::metrics_handler: Internal service error operation="start_workflow_execution" error=shard store error: CodecError { IO error: `Broken pipe (os error 32)' }
```

No mixed gate failed for the Frontend two-cluster or the two Go<->Rust History replication directions. All 21 runs used head SHA `15e01dd83e47fab1c7ea793c46b82fde80be4448`. No local Cargo, Go, or Docker commands were run.

## Result

9 of 21 runs passed by the requested criteria: Rust CI plus 8 mixed gates. All 12 History mixed-gate runs failed as detailed above.
