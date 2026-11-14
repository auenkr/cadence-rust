# Master validation batch — 2026-11-14e

Branch: `master-validation-20261114e`  
Validated source head: `96ad81f73b88c3bf010f5f815fe637e59e291492` (`origin/master` when branched; all 22 runs used this head)  
Workflow dispatches: 22 total (1 Rust CI, 12 History combinations, 1 History queue-v2 lane, 6 Frontend two-cluster lanes, 2 Go<->Rust History replication directions).

Mixed gates pass only when the Actions run concluded `success` and its log contains `GATE SUMMARY failed: []`. Rust CI passes by workflow conclusion.

| Workflow | Inputs | Run | Conclusion / gate evidence |
|---|---|---|---|
| rust-ci.yml | Full workspace; no package input | [43569574503](https://github.com/auenkr/cadence-rust/actions/runs/43569574503) | success |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=true, queue_v2=false | [43569577780](https://github.com/auenkr/cadence-rust/actions/runs/43569577780) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=false, queue_v2=false | [43569580550](https://github.com/auenkr/cadence-rust/actions/runs/43569580550) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=true, queue_v2=false | [43569583216](https://github.com/auenkr/cadence-rust/actions/runs/43569583216) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=false, queue_v2=false | [43569586009](https://github.com/auenkr/cadence-rust/actions/runs/43569586009) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true, queue_v2=false | [43569588903](https://github.com/auenkr/cadence-rust/actions/runs/43569588903) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=false, queue_v2=false | [43569591302](https://github.com/auenkr/cadence-rust/actions/runs/43569591302) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=true, queue_v2=false | [43569594526](https://github.com/auenkr/cadence-rust/actions/runs/43569594526) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=false, queue_v2=false | [43569597416](https://github.com/auenkr/cadence-rust/actions/runs/43569597416) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=true, queue_v2=false | [43569599942](https://github.com/auenkr/cadence-rust/actions/runs/43569599942) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=false, queue_v2=false | [43569602499](https://github.com/auenkr/cadence-rust/actions/runs/43569602499) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=true, queue_v2=false | [43569605255](https://github.com/auenkr/cadence-rust/actions/runs/43569605255) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=false, queue_v2=false | [43569607898](https://github.com/auenkr/cadence-rust/actions/runs/43569607898) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true, queue_v2=true | [43569610384](https://github.com/auenkr/cadence-rust/actions/runs/43569610384) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43569613272](https://github.com/auenkr/cadence-rust/actions/runs/43569613272) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_frontend_active=true | [43569615698](https://github.com/auenkr/cadence-rust/actions/runs/43569615698) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43569618032](https://github.com/auenkr/cadence-rust/actions/runs/43569618032) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_frontend_active=true | [43569620440](https://github.com/auenkr/cadence-rust/actions/runs/43569620440) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43569622666](https://github.com/auenkr/cadence-rust/actions/runs/43569622666) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_frontend_active=true | [43569625250](https://github.com/auenkr/cadence-rust/actions/runs/43569625250) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=A (Go active -> Rust standby) | [43569628036](https://github.com/auenkr/cadence-rust/actions/runs/43569628036) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=B (Rust active -> Go standby) | [43569630274](https://github.com/auenkr/cadence-rust/actions/runs/43569630274) | success; `GATE SUMMARY failed: []` |

## Failure details

None. All 22 runs concluded `success`; all 21 mixed gates included `GATE SUMMARY failed: []`. No local Cargo, Go, or Docker commands were run.

## Result

22 of 22 runs passed by the requested criteria.
