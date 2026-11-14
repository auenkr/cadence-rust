# Master validation batch — 2026-11-14f

Branch: `master-validation-20261114f`  
Validated source head: `b8af03b6d04d71082285af928d6551979bc4ec4b` (`origin/master` when branched; all 23 runs used this head)  
Workflow dispatches: 23 total (1 Rust CI, 12 History combinations, 1 History queue-v2 lane, 6 Frontend two-cluster lanes, 2 Go<->Rust History replication directions, 1 authenticated Kafka SASL/PLAIN lane).

Mixed gates pass only when the Actions run concluded `success` and its log contains `GATE SUMMARY failed: []`. Rust CI passes by workflow conclusion.

| Workflow | Inputs | Run | Conclusion / gate evidence |
|---|---|---|---|
| rust-ci.yml | Full workspace; no package input | [43581447970](https://github.com/auenkr/cadence-rust/actions/runs/43581447970) | success |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=true, queue_v2=false | [43581447743](https://github.com/auenkr/cadence-rust/actions/runs/43581447743) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=false, queue_v2=false | [43581447721](https://github.com/auenkr/cadence-rust/actions/runs/43581447721) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=true, queue_v2=false | [43581447698](https://github.com/auenkr/cadence-rust/actions/runs/43581447698) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=false, queue_v2=false | [43581447803](https://github.com/auenkr/cadence-rust/actions/runs/43581447803) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true, queue_v2=false | [43581447804](https://github.com/auenkr/cadence-rust/actions/runs/43581447804) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=false, queue_v2=false | [43581447921](https://github.com/auenkr/cadence-rust/actions/runs/43581447921) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=true, queue_v2=false | [43581447858](https://github.com/auenkr/cadence-rust/actions/runs/43581447858) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=false, queue_v2=false | [43581447741](https://github.com/auenkr/cadence-rust/actions/runs/43581447741) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=true, queue_v2=false | [43581447786](https://github.com/auenkr/cadence-rust/actions/runs/43581447786) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=false, queue_v2=false | [43581447691](https://github.com/auenkr/cadence-rust/actions/runs/43581447691) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=true, queue_v2=false | [43581447846](https://github.com/auenkr/cadence-rust/actions/runs/43581447846) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=false, queue_v2=false | [43581447783](https://github.com/auenkr/cadence-rust/actions/runs/43581447783) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true, queue_v2=true | [43581447855](https://github.com/auenkr/cadence-rust/actions/runs/43581447855) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43581447740](https://github.com/auenkr/cadence-rust/actions/runs/43581447740) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_frontend_active=true | [43581447780](https://github.com/auenkr/cadence-rust/actions/runs/43581447780) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43581447862](https://github.com/auenkr/cadence-rust/actions/runs/43581447862) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_frontend_active=true | [43581447695](https://github.com/auenkr/cadence-rust/actions/runs/43581447695) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true | [43581447751](https://github.com/auenkr/cadence-rust/actions/runs/43581447751) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_frontend_active=true | [43581447785](https://github.com/auenkr/cadence-rust/actions/runs/43581447785) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=A (Go active -> Rust standby) | [43581447798](https://github.com/auenkr/cadence-rust/actions/runs/43581447798) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=B (Rust active -> Go standby) | [43581447968](https://github.com/auenkr/cadence-rust/actions/runs/43581447968) | success; `GATE SUMMARY failed: []` |
| history-es-visibility.yml | es_flavor=elasticsearch, two_history=false, kafka_auth=true (SASL/PLAIN) | [43581447681](https://github.com/auenkr/cadence-rust/actions/runs/43581447681) | success; `GATE SUMMARY failed: []` |

## Failure details

None. All 23 runs concluded `success`; all 22 mixed gates included `GATE SUMMARY failed: []`. No reruns were needed. No local Cargo, Go, or Docker commands were run.

## Result

23 of 23 runs passed by the requested criteria.
