# Master validation batch — 2026-11-14

Branch: `master-validation-20261114c`  
Validated source head: `938b770058d837b185c643c7bd8cbe26f43e32ce` (origin/master at branch creation)  
Workflow dispatches: 21 total (1 Rust CI, 12 History combinations, 6 Frontend two-cluster cases, 2 History replication directions).

Mixed gates count as passing only when the Actions run concluded `success` and its log contained `GATE SUMMARY failed: []`. Rust CI is reported by its workflow conclusion (it has no gate summary).

| Workflow | Inputs | Run | Conclusion / gate evidence |
|---|---|---|---|
| rust-ci.yml | Full workspace; no package input | [43537737292](https://github.com/auenkr/cadence-rust/actions/runs/43537737292) | success |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=true | [43537737685](https://github.com/auenkr/cadence-rust/actions/runs/43537737685) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=grpc, two_history=false | [43537737427](https://github.com/auenkr/cadence-rust/actions/runs/43537737427) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=true | [43537737295](https://github.com/auenkr/cadence-rust/actions/runs/43537737295) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=mysql, outbound=tchannel, two_history=false | [43537737424](https://github.com/auenkr/cadence-rust/actions/runs/43537737424) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=true | [43537737568](https://github.com/auenkr/cadence-rust/actions/runs/43537737568) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=grpc, two_history=false | [43537737705](https://github.com/auenkr/cadence-rust/actions/runs/43537737705) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=true | [43537737636](https://github.com/auenkr/cadence-rust/actions/runs/43537737636) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=postgres, outbound=tchannel, two_history=false | [43537737489](https://github.com/auenkr/cadence-rust/actions/runs/43537737489) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=true | [43537737889](https://github.com/auenkr/cadence-rust/actions/runs/43537737889) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=grpc, two_history=false | [43537738327](https://github.com/auenkr/cadence-rust/actions/runs/43537738327) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=true | [43537738493](https://github.com/auenkr/cadence-rust/actions/runs/43537738493) | success; `GATE SUMMARY failed: []` |
| history-mixed-gate.yml | database=cassandra, outbound=tchannel, two_history=false | [43537737652](https://github.com/auenkr/cadence-rust/actions/runs/43537737652) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43537737559](https://github.com/auenkr/cadence-rust/actions/runs/43537737559) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, two_cluster=true, rust_frontend_active=true, kafka=false | [43537737756](https://github.com/auenkr/cadence-rust/actions/runs/43537737756) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43537737659](https://github.com/auenkr/cadence-rust/actions/runs/43537737659) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=postgres, two_cluster=true, rust_frontend_active=true, kafka=false | [43537737724](https://github.com/auenkr/cadence-rust/actions/runs/43537737724) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_history=true, rust_history_side=active, two_history_graceful=true, kafka=false | [43537737846](https://github.com/auenkr/cadence-rust/actions/runs/43537737846) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=cassandra, two_cluster=true, rust_frontend_active=true, kafka=false | [43537738073](https://github.com/auenkr/cadence-rust/actions/runs/43537738073) | **failure**; `GATE SUMMARY failed: ['set-isolation-groups-on-rust-frontend-B', 'isolation-groups-replicated-to-A']` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=A, kafka=false | [43537738386](https://github.com/auenkr/cadence-rust/actions/runs/43537738386) | success; `GATE SUMMARY failed: []` |
| frontend-mixed-gate.yml | database=mysql, repl_rust_cluster=B, kafka=false | [43537737783](https://github.com/auenkr/cadence-rust/actions/runs/43537737783) | success; `GATE SUMMARY failed: []` |

## Failure details

Run [43537738073](https://github.com/auenkr/cadence-rust/actions/runs/43537738073), Frontend mixed gate with Cassandra and `rust_frontend_active=true`:

- `GATE FAIL set-isolation-groups-on-rust-frontend-B Error: failed to update isolation-groups`
- `GATE FAIL isolation-groups-replicated-to-A -- No groups found --`
- The run concluded `failure` and did not satisfy the required empty-failure summary.

## Result

20 of 21 workflow runs passed by conclusion. Among the 20 mixed gates, 19 passed the required summary-marker check; one failed as recorded above. Every run used head SHA `938b770058d837b185c643c7bd8cbe26f43e32ce`. No local Cargo, Go, or Docker commands were run.
