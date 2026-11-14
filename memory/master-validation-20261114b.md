# Master validation 2026-11-14b

Validation target: `master` at `d9eb3d7fc31c1cca229ab90a26ff3f657e1ef979`.

The requested set was 19 workflow-dispatch runs: one `rust-ci`, 12 History mixed gates, and six Frontend mixed gates. All runs completed. The `rust-ci` run and 17 of 18 mixed gates passed. The failing Cassandra Frontend gate had a successful process conclusion from the harness but does not count as a gate pass because its `GATE SUMMARY` lists failures.

| Run ID | URL | Workflow | Inputs | Conclusion | GATE SUMMARY |
|---:|---|---|---|---|---|
| 43521529204 | https://github.com/auenkr/cadence-rust/actions/runs/43521529204 | rust-ci | `package` omitted (whole workspace) | success | n/a |
| 43521533476 | https://github.com/auenkr/cadence-rust/actions/runs/43521533476 | History mixed gate | mysql; grpc; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521539036 | https://github.com/auenkr/cadence-rust/actions/runs/43521539036 | History mixed gate | mysql; grpc; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521543278 | https://github.com/auenkr/cadence-rust/actions/runs/43521543278 | History mixed gate | mysql; tchannel; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521547428 | https://github.com/auenkr/cadence-rust/actions/runs/43521547428 | History mixed gate | mysql; tchannel; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521560583 | https://github.com/auenkr/cadence-rust/actions/runs/43521560583 | History mixed gate | postgres; grpc; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521564944 | https://github.com/auenkr/cadence-rust/actions/runs/43521564944 | History mixed gate | postgres; grpc; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521569915 | https://github.com/auenkr/cadence-rust/actions/runs/43521569915 | History mixed gate | postgres; tchannel; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521575001 | https://github.com/auenkr/cadence-rust/actions/runs/43521575001 | History mixed gate | postgres; tchannel; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521588980 | https://github.com/auenkr/cadence-rust/actions/runs/43521588980 | History mixed gate | cassandra; grpc; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521593154 | https://github.com/auenkr/cadence-rust/actions/runs/43521593154 | History mixed gate | cassandra; grpc; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521597635 | https://github.com/auenkr/cadence-rust/actions/runs/43521597635 | History mixed gate | cassandra; tchannel; `two_history=true` | success | `GATE SUMMARY failed: []` |
| 43521601750 | https://github.com/auenkr/cadence-rust/actions/runs/43521601750 | History mixed gate | cassandra; tchannel; `two_history=false` | success | `GATE SUMMARY failed: []` |
| 43521551838 | https://github.com/auenkr/cadence-rust/actions/runs/43521551838 | Frontend mixed gate | mysql; `two_cluster=true`; `rust_history=true`; `rust_history_side=active`; `two_history_graceful=true` | success | `GATE SUMMARY failed: []` |
| 43521556416 | https://github.com/auenkr/cadence-rust/actions/runs/43521556416 | Frontend mixed gate | mysql; `two_cluster=true`; `rust_frontend_active=true` | success | `GATE SUMMARY failed: []` |
| 43521579453 | https://github.com/auenkr/cadence-rust/actions/runs/43521579453 | Frontend mixed gate | postgres; `two_cluster=true`; `rust_history=true`; `rust_history_side=active`; `two_history_graceful=true` | success | `GATE SUMMARY failed: []` |
| 43521584636 | https://github.com/auenkr/cadence-rust/actions/runs/43521584636 | Frontend mixed gate | postgres; `two_cluster=true`; `rust_frontend_active=true` | success | `GATE SUMMARY failed: []` |
| 43521606210 | https://github.com/auenkr/cadence-rust/actions/runs/43521606210 | Frontend mixed gate | cassandra; `two_cluster=true`; `rust_history=true`; `rust_history_side=active`; `two_history_graceful=true` | failure | `GATE SUMMARY failed: ['grace-marker-send-cross-host', 'grace-marker-recv-on-other-host']` |
| 43521611638 | https://github.com/auenkr/cadence-rust/actions/runs/43521611638 | Frontend mixed gate | cassandra; `two_cluster=true`; `rust_frontend_active=true` | success | `GATE SUMMARY failed: []` |

## Failure details

Run [43521606210](https://github.com/auenkr/cadence-rust/actions/runs/43521606210) failed the cross-host graceful failover marker checks. The gate reported two sends with no cross-host send, and both received markers were observed on `rust-history-2`. The excerpt is from the uploaded `gate.log` artifact:

```text
2026-11-13T22:59:18.418582Z  INFO cadence_history::queue::timer_queue_processor: Timer Failover Triggered domain_ids={"01a1d722-8f3c-7d84-907b-30bb49c63bd4"} min_level=1000000000 max_level=1794610710418000000
2026-11-13T22:59:18.418607Z  INFO cadence_history::queue::timer_queue_processor: creating timer failover processor cluster=cluster1 domain_ids={"01a1d722-8f3c-7d84-907b-30bb49c63bd4"} failover=from: cluster0
2026-11-13T22:59:18.418647Z  INFO cadence_history::queue::timer_queue_processor_base: Timer queue processor state changed: starting
2026-11-13T22:59:18.418689Z  INFO cadence_history::queue::timer_queue_processor_base: Timer queue processor state changed: started
2026-11-13T22:59:19.393405Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=1 markers=1
2026-11-13T22:59:19.394602Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=0 markers=1
2026-11-13T22:59:19.394652Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=2 markers=1
2026-11-13T22:59:19.395746Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=3 markers=1
2026-11-13T22:59:21.464475Z  INFO cadence_history::wiring::assembly::failover_client: failover-marker-send: NotifyFailoverMarkers to owning history host target_host=127.0.0.1:34765 target_grpc=127.0.0.1:53803 tokens=1 domains=["01a1d722-8f3c-7d84-907b-30bb49c63bd4"]
2026-11-13T22:59:21.465758Z  INFO cadence_history::handler::handler_impl: failover-marker-recv: NotifyFailoverMarkers applied on this history host shard_ids=[1, 0, 2, 3] domain_id=Some("01a1d722-8f3c-7d84-907b-30bb49c63bd4") failover_version=Some(1)
2026-11-13T22:59:21.465785Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-receive shard_ids=[1, 0, 2, 3] domain_id=Some("01a1d722-8f3c-7d84-907b-30bb49c63bd4")
2026-11-13T22:59:21.477361Z  INFO cadence_history::failover::coordinator: Updated domain from pending-active to active domain_name=tc-act-94bc15 failover_version=Some(1) failover_duration_seconds=4.048608046 marker_pipeline_duration_seconds=0.011404831 clean_pending_active_duration_seconds=0.011312695
2026-11-13T22:59:24.393360Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=1 markers=1
2026-11-13T22:59:24.394573Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=0 markers=1
2026-11-13T22:59:24.395715Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=3 markers=1
2026-11-13T22:59:24.395746Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-notify shard_id=2 markers=1
2026-11-13T22:59:27.158351Z  INFO cadence_history::wiring::assembly::failover_client: failover-marker-send: NotifyFailoverMarkers to owning history host target_host=127.0.0.1:34765 target_grpc=127.0.0.1:53803 tokens=1 domains=["01a1d722-8f3c-7d84-907b-30bb49c63bd4"]
2026-11-13T22:59:27.159463Z  INFO cadence_history::handler::handler_impl: failover-marker-recv: NotifyFailoverMarkers applied on this history host shard_ids=[1, 0, 3, 2] domain_id=Some("01a1d722-8f3c-7d84-907b-30bb49c63bd4") failover_version=Some(1)
2026-11-13T22:59:27.159491Z  INFO cadence_history::failover::coordinator: failover-marker-coordinator-receive shard_ids=[1, 0, 3, 2] domain_id=Some("01a1d722-8f3c-7d84-907b-30bb49c63bd4")
2026-11-13T22:59:29.393765Z  INFO cadence_history::wiring::assembly::failover: Dropped pending failover marker domain=tc-act-94bc15 reason="domain is active in current cluster"
GATE FAIL grace-marker-send-cross-host sends=2 cross=0 e.g. 2026-11-13T22:59:21.464475Z INFO cadence_history::wiring::assembly::failover_client: failover-marker-send: NotifyFailoverMarkers to owning history host target_host=127.0.0.1:34765 target_grpc=127.0.0.1:53803 tokens=1 domains=["01a1d722-8f3c-7d84-907b-30bb49c63bd4"]
GATE FAIL grace-marker-recv-on-other-host recv hosts=['rust-history-2'] count=2
GATE SUMMARY failed: ['grace-marker-send-cross-host', 'grace-marker-recv-on-other-host']
```

There was also a separate push-triggered `rust-ci` run on the same SHA, [43521512036](https://github.com/auenkr/cadence-rust/actions/runs/43521512036), which succeeded. It is not included in the 19 requested workflow-dispatch runs.
