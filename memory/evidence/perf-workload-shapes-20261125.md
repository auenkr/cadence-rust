# Workload-shape benchmarks, Go vs Rust (2026-11-25)

Branch `perf-workload-shapes`. The driver and `paired-run.py` take a new `--shape` flag. The workflow input `capacity_only` was replaced by a `shape` choice, because the benchmark workflow is at the 25-input cap and a new workflow file cannot be dispatched until it exists on master.

Shapes (`tests/frontend-gate/benchmark/main.go`):
- `activities`: 5 sequential plus 5 parallel activities per workflow (about 10 activities).
- `signal-timer`: client sends 2 signals, workflow waits for both, then a 200 ms timer.
- `child`: 3 parallel child workflows, each the echo flow with one activity.
- `history`: 4 batches of 100 parallel activities, about 1200+ events, with a query handler. After completion the client queries the workflow, which forces a full-history replay. The execution timeout is 10 min.

Setup, same for all paired runs:
- Arms: 3-arm mode (go, control, rust candidate), rotated order, fresh DB per arm, MySQL 8.0, `rust_services=frontend,matching,history`, `payload_bytes=256`, 3 repetitions.
- Control: `origin/master`. Its rust, go and go-overlay trees are identical to the branch, so control and rust are the same binaries (A/A, `control-same-tree.txt` = 1).
- Runner: GitHub `ubuntu-latest` (ubuntu26 image 20261116.202.1), Intel Xeon Platinum 8573C, 4 vCPU (2 cores × 2 threads), 15 GiB RAM, kernel 7.0.0-1020-azure.
- Each service container is capped at 1 CPU and 1400 MiB, and the DB at 1.5 CPU and 1800 MiB. Each run's artifact has the Go and Rust binary SHA256 files.

Columns are medians over 3 reps. Peak anon MiB and CPU-s are the sums of the frontend, matching and history containers, taken from the per-rep `resources-*.json` files. Closed loop at concurrency 16, except `history` at concurrency 4.

| Shape | Run | Arm | wf/s | p50 ms | p95 ms | p99 ms | peak anon MiB | CPU-s | failed |
|---|---|---|---|---|---|---|---|---|---|
| activities (300 req) | [45622485832](https://github.com/auenkr/cadence-rust/actions/runs/45622485832) | go | 13.5 | 1155.7 | 1585.6 | 1830.8 | 139.2 | 25.6 | 0 |
| | | control (master) | 13.4 | 1154.8 | 1707.4 | 1918.6 | 106.5 | 16.5 | 0 |
| | | rust | 12.9 | 1225.0 | 1766.7 | 1906.7 | 107.2 | 16.8 | 0 |
| signal-timer (300 req) | [45622494242](https://github.com/auenkr/cadence-rust/actions/runs/45622494242) | go | 14.0 | 1071.7 | 1296.0 | 1506.5 | 114.7 | 13.5 | 0 |
| | | control (master) | 14.8 | 1040.0 | 1163.3 | 1269.4 | 72.5 | 5.7 | 0 |
| | | rust | 14.8 | 1042.3 | 1141.2 | 1253.9 | 72.3 | 5.8 | 0 |
| child (300 req) | [45622509371](https://github.com/auenkr/cadence-rust/actions/runs/45622509371) | go | 14.1 | 1123.2 | 1464.9 | 1527.1 | 165.9 | 24.0 | 0 |
| | | control (master) | 12.8 | 1325.3 | 1775.9 | 1912.7 | 130.8 | 15.7 | 0 |
| | | rust | 11.8 | 1414.2 | 1798.4 | 1895.5 | 130.6 | 16.3 | 0 |
| history ≥1200 events + replay query (20 req, c4) | [45640614166](https://github.com/auenkr/cadence-rust/actions/runs/45640614166) | go | 0.2 | 18494 | 18743 | 18743 | 150.0 | 88.2 | 0 |
| | | control (master) | 0.3 | 12935 | 14012 | 14012 | 189.5 | 59.9 | 0 |
| | | rust | 0.3 | 12990 | 13271 | 13271 | 182.2 | 58.4 | 0 |

Per-rep wf/s:
- activities: go 13.4/13.5/13.6, control 13.2/13.6/13.4, rust 12.5/13.0/12.9.
- signal-timer: go 14.0/14.0/14.2, control and rust 14.8 in every rep.
- child: go 14.1/14.4/12.5, control 12.8/12.9/11.2, rust 11.1/20.7/11.8.
- history: go 0.2 in all reps, control and rust 0.3 in all reps (the 1-decimal rounding hides the gap; see the p50 row).

## A/A (control vs rust, same binaries) and Go vs Rust

Ratios are rust ÷ other arm, from medians.

| Shape | A/A wf/s | A/A p50 | A/A p99 | A/A CPU | Rust/Go wf/s | Rust/Go p50 | Rust/Go p99 | Rust/Go anon | Rust/Go CPU |
|---|---|---|---|---|---|---|---|---|---|
| activities | 0.962 | 1.061 | 0.994 | 1.02 | 0.952 | 1.060 | 1.041 | 0.77 | 0.66 |
| signal-timer | 1.000 | 1.002 | 0.988 | 1.02 | 1.056 | 0.973 | 0.832 | 0.63 | 0.43 |
| child | 0.919 | 1.067 | 0.991 | 1.04 | 0.835 | 1.259 | 1.241 | 0.79 | 0.68 |
| history | 0.997 | 1.004 | 0.947 | 0.98 | 1.417 | 0.702 | 0.708 | 1.21 | 0.66 |

The runner's own three-arm report gives cand/control throughput in `three-arm-report.md` of each artifact. Medians [min,max]:
- activities: 0.952 [0.947, 0.962].
- signal-timer: 0.999 [0.995, 1.004].
- child: 1.054 [0.864, 1.599].
- history: 0.992 [0.971, 1.011].

## Findings

- **Resources.** Rust uses less CPU than Go in every shape (0.43–0.68×). It uses less peak anon memory in the three small shapes (0.63–0.79×). In the `history` shape it uses more (1.21×, 182 vs 150 MiB). That is the one place Rust memory is higher than Go.
- **signal-timer.** Rust is equal to or better than Go: 1.056× wf/s, p99 0.83×.
- **history.** Rust is faster than Go: 1.42× wf/s, p50 0.70×. This covers a 1200+ event history, parallel activity batches and a full-replay query. All 20×3 queries returned the correct count.
- **activities.** Rust is 4.8% below Go on throughput and 6% above on p50. The A/A (control vs rust, identical binaries) already shows 3.8% and 6.1% from noise, so this is not a resolvable regression.
- **child.** Rust is 16.5% below Go on throughput, p50 1.26× and p99 1.24×. The control, which has the same binaries as Rust, shows the same gap to Go (12.8 vs 14.1 wf/s, p50 1.18×). The A/A spread is huge: one rust rep ran at 20.7 wf/s, and the three-arm ratio range is [0.864, 1.599]. This is **not resolvable from 3 reps.** It is the only shape where Rust trails Go beyond the A/A deltas. It needs more reps or a longer run before it can be called a regression. I did not investigate or fix it. No profile was captured: `profile=1`/`trace_decision` were not enabled. Per-workflow history gaps are in `paired-benchmark/result-*-tail-history.json` in each artifact (slow-5% and 20 random fast workflows) and can be used to see where the child path loses time.
- **Throughput caveat.** At concurrency 16 the closed loop is latency-bound (about 1 s p50 per workflow), so wf/s mostly reflects p50 and not capacity.

## Cold start and failover latency

No existing gate reports process-restart timing. `history-restartgate.yml` is a correctness gate, with no timing output. The only timed recovery measurement is the SDK suite `recovery` stage, a 3 s MySQL TCP outage while loading 5 starts/s. It was run with `service=all` (Go vs all-Rust cluster), 3 reps, together with the `fixed` and `capacity` stages. Run [45644853675](https://github.com/auenkr/cadence-rust/actions/runs/45644853675), artifact `sdk-all-benchmark`, head `397d7fd1781192f12affa8c7c1210c12b041e060`.

Medians of 3 reps. CPU-s is the sum over all containers including MySQL. The 25 wf/s capacity row has a single-service `go`/`rust` label.

| Stage | Arm | completions/s | completion p50 ms | p95 ms | p99 ms | CPU-s | errors | seconds to stable after restore |
|---|---|---|---|---|---|---|---|---|
| fixed 5/s | go | 5.00 | 40.1 | 46.2 | 49.3 | 7.3 | 0 | n/a |
| fixed 5/s | rust | 5.00 | 27.5 | 29.0 | 33.6 | 4.2 | 0 | n/a |
| capacity 25/s | go | 25.00 | 36.2 | 84.4 | 108.2 | 26.7 | 0 | n/a |
| capacity 25/s | rust | 25.00 | 25.4 | 67.6 | 99.1 | 13.9 | 0 | n/a |
| recovery 5/s (3 s DB outage) | go | 5.00 | 39.9 | 2123 | 3377 | 7.1 | 0 | 5 (5, 5, 5) |
| recovery 5/s (3 s DB outage) | rust | 5.00 | 27.7 | 2075 | 3138 | 4.2 | 0 | 5 (5, 5, 5) |

Rust recovers in the same 5 s window as Go (the minimum the harness measures), and its p99 during the outage is slightly lower. **Gap:** true cold-start time and kill -9 failover latency are not measured by any harness. That needs a new timed restart stage.

## Reproduction

```
gh workflow run frontend-matching-benchmark.yml --ref perf-workload-shapes -f shape=<activities|signal-timer|child|history> \
  -f rust_services=frontend,matching,history -f control_ref=origin/master -f requests=300 -f repetitions=3 -f concurrency=16 -f payload_bytes=256
# history: -f requests=20 -f warmup=2 -f concurrency=4
# recovery suite: -f sdk_openloop=true -f sdk_suite=true -f service=all -f capacity_rates=25 -f sdk_repetitions=3
```

Failed attempts, not used for the results:
- Runs 45621268879, 45621275047, 45621280967 and 45621287160 failed at `go vet` (a driver wiring bug, fixed).
- History run 45622516859 (300 req, c16) hit the 90 min job timeout.
- History run 45638858443 failed. The 1 min execution timeout was too short for 400 activities, even on Go.
