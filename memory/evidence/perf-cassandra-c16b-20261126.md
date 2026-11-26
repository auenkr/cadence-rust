# Cassandra c=16 re-measurement with CIs (2026-11-26)

Branch `perf-cassandra-c16b` (from origin/master 82d393d2f). Six independent dispatches of `frontend-matching-benchmark.yml` on source head `82d393d2fd6755b1e5390231fd207c2701d06209`: database=cassandra, rust_services=frontend,matching,history, control_ref=origin/master (control = same tree/binary as Rust, `control-same-tree.txt`=1, the A/A arm), shape=echo, 1000 requests, warmup 10, 1 KiB payload, concurrency 1,16,64 (default), repetitions=4 (alternating Go/Rust/control, fresh DB per arm/rep). All 6 runs success, 216 cells, **0 failed workflows**. Runners: 2 cores/4 threads, 15 GiB, Ubuntu 26.04.1; service containers capped per machine-spec.md.

Rust cadence-server SHA256 (rust and control, all 6 runs): `1b01a920abefbba65f87092bb2840975c5c9dd3728bdeb79932b58f13c3906a3`; Rust cadence-history `a9a4e8552e70ea9cd3147bcea104f4db9e1dbc552aa535d9973e5d734ec0d709`.

| run | runner CPU | Go cadence-history SHA256 |
|--|--|--|
| 45759957156 | Intel(R) Xeon(R) 6975P-C | 69d2227b523e98d46464862fcf05f39d09c4f759b83d8bd222e849f7574e49da |
| 45759961051 | AMD EPYC 9V45 96-Core Processor | 2cb588e5d904d9c83c473fcf911b57bd60b994d8502d894cc0dbe293705cfcdc |
| 45759965058 | AMD EPYC 9V45 96-Core Processor | 6b9c60612365fd39b100664035f294cd33f790e336a9d90141b213a31aa79f77 |
| 45759968954 | AMD EPYC 9V45 96-Core Processor | 9fe4d8058e8040cac1e25a8f10c067dcdb9fa25949d52fe91a7f99c358318c91 |
| 45759973060 | Intel(R) Xeon(R) 6975P-C | 0437e7e259a3a26749958eb29ee0c32b40d421b28598745026c2b6239d2dcbe2 |
| 45759976681 | AMD EPYC 9V74 80-Core Processor | 740fb7e539966ed9605730ee91bec01bec98ce6ec3279c1b70a0e6c5503b02d8 |
Run URLs: [45759957156](https://github.com/auenkr/cadence-rust/actions/runs/45759957156) [45759961051](https://github.com/auenkr/cadence-rust/actions/runs/45759961051) [45759965058](https://github.com/auenkr/cadence-rust/actions/runs/45759965058) [45759968954](https://github.com/auenkr/cadence-rust/actions/runs/45759968954) [45759973060](https://github.com/auenkr/cadence-rust/actions/runs/45759973060) [45759976681](https://github.com/auenkr/cadence-rust/actions/runs/45759976681) 

## Method
Per run and cell: d = mean over the 4 reps of (arm A − arm B), paired by rep. The 6 per-run d values are the clusters; 95% CI = percentile cluster bootstrap (10000 resamples of the 6 runs, seed 1) of their mean. wf/s = successful/elapsed. p95/p99 are per-rep percentiles of successful latency samples. CPU-s = cgroup CPU seconds in the measurement window summed over FE+MT+HI. Runner CPU varies (Intel Xeon 6975P ×2, EPYC 9V45 ×3, EPYC 9V74 ×1), hence runs as clusters. Raw data: the run artifacts above (`result-*`, `resources-*`, `cass-*-{before,after}.log`).


### Rust − Go (mean of per-run mean-paired-rep diffs; 95% cluster bootstrap over 6 runs)

| c | metric | diff | 95% CI | excludes 0 | per-run diffs |
|--|--|--|--|--|--|
| 1 | wfs (wf/s) | +1.54 | [+0.97, +2.06] | YES | +0.8, +1.9, +2.5, +1.7, +0.4, +1.8 |
| 1 | p95 (ms) | +14.98 | [+8.25, +23.20] | YES | +10.3, +33.6, +12.5, +13.3, +2.6, +17.5 |
| 1 | p99 (ms) | +0.67 | [-4.52, +6.87] | no | -5.8, +4.5, +15.2, -3.0, -5.9, -1.0 |
| 1 | cpu (cpu-s) | -8.08 | [-9.30, -6.75] | YES | -6.2, -10.0, -9.8, -7.4, -5.9, -9.2 |
| 16 | wfs (wf/s) | -8.82 | [-11.22, -6.96] | YES | -14.0, -8.5, -9.1, -5.2, -7.6, -8.4 |
| 16 | p95 (ms) | +55.99 | [+47.42, +63.92] | YES | +59.8, +63.1, +70.5, +47.6, +37.7, +57.2 |
| 16 | p99 (ms) | +84.94 | [+58.61, +112.01] | YES | +123.7, +91.4, +132.6, +61.4, +41.6, +58.9 |
| 16 | cpu (cpu-s) | -0.57 | [-0.92, -0.09] | YES | -0.8, -0.6, +0.6, -0.6, -1.1, -0.9 |
| 64 | wfs (wf/s) | +1.36 | [-2.33, +3.98] | no | +2.7, +5.7, -7.5, +1.0, +3.1, +3.1 |
| 64 | p95 (ms) | +36.49 | [-7.74, +95.58] | no | +21.4, +5.5, +176.2, +11.8, +42.8, -38.7 |
| 64 | p99 (ms) | +97.85 | [+25.43, +174.59] | YES | +130.1, +62.7, +259.5, +4.8, +152.5, -22.5 |
| 64 | cpu (cpu-s) | -0.66 | [-1.15, -0.03] | YES | -1.0, -1.5, +0.8, -0.4, -1.0, -0.8 |

### A/A: Rust − Control (same binary) (mean of per-run mean-paired-rep diffs; 95% cluster bootstrap over 6 runs)

| c | metric | diff | 95% CI | excludes 0 | per-run diffs |
|--|--|--|--|--|--|
| 1 | wfs (wf/s) | -0.24 | [-0.76, +0.21] | no | -0.4, +0.2, +0.6, -0.4, -1.4, +0.1 |
| 1 | p95 (ms) | -0.07 | [-0.29, +0.16] | no | +0.4, -0.0, -0.5, -0.1, -0.1, -0.1 |
| 1 | p99 (ms) | +0.38 | [-0.03, +0.74] | no | +0.7, -0.5, +0.2, +1.0, +0.4, +0.6 |
| 1 | cpu (cpu-s) | -0.31 | [-0.66, +0.03] | no | +0.3, -0.9, -0.7, +0.0, -0.0, -0.5 |
| 16 | wfs (wf/s) | -1.78 | [-2.95, -0.66] | YES | -4.1, -1.8, -0.5, -3.3, -0.2, -0.9 |
| 16 | p95 (ms) | +8.27 | [+2.58, +16.78] | YES | +2.6, +28.3, +3.0, +10.6, +1.8, +3.3 |
| 16 | p99 (ms) | +22.47 | [+14.30, +30.57] | YES | +33.9, +34.9, +23.4, +22.9, +8.2, +11.5 |
| 16 | cpu (cpu-s) | +0.27 | [+0.14, +0.41] | YES | +0.4, +0.1, +0.5, +0.3, +0.0, +0.2 |
| 64 | wfs (wf/s) | -1.06 | [-4.43, +1.50] | no | +1.6, +1.7, -2.3, -9.2, +2.1, -0.4 |
| 64 | p95 (ms) | +3.16 | [-48.20, +57.44] | no | -69.5, -48.1, +88.4, +105.9, -26.9, -30.7 |
| 64 | p99 (ms) | +28.19 | [-5.00, +63.13] | no | +9.6, +2.1, +84.5, +93.7, -24.3, +3.4 |
| 64 | cpu (cpu-s) | +0.27 | [-0.06, +0.63] | no | +0.0, -0.3, +0.4, +1.0, -0.1, +0.5 |

### Absolute means over all 24 reps

| c | arm | wf/s | p95 ms | p99 ms | CPU-s (FE+MT+HI) |
|--|--|--|--|--|--|
| 1 | go | 22.4 | 84 | 109 | 22.7 |
| 1 | rust | 23.9 | 99 | 110 | 14.6 |
| 1 | control | 24.2 | 99 | 109 | 14.9 |
| 16 | go | 82.4 | 311 | 389 | 14.4 |
| 16 | rust | 73.6 | 367 | 474 | 13.8 |
| 16 | control | 75.3 | 359 | 451 | 13.6 |
| 64 | go | 90.3 | 1117 | 1250 | 14.3 |
| 64 | rust | 91.7 | 1153 | 1347 | 13.6 |
| 64 | control | 92.7 | 1150 | 1319 | 13.4 |

Total cells 216; failed workflows total 0

## Per-table Cassandra ops per workflow
Delta of nodetool tablestats, all 24 cells per arm/concurrency, divided by 1010 workflows. At c=1 every table is identical across arms (executions 7.02/6.01, history_node 3.07/6.00, paxos W 18.03 vs 18.02).

| c | table | Go R/W | Rust R/W | Control R/W |
|--|--|--|--|--|
| 16 | cadence.tasks | 1.50/0.76 | 1.71/0.87 | 1.73/0.88 |
| 16 | system.paxos (LWT writes) | 0.01/20.38 | 0.01/20.77 | 0.01/20.80 |
| 16 | cadence.executions | 7.16/6.05 | 7.17/6.08 | 7.15/6.08 |
| 16 | cadence.history_node | 3.07/6.04 | 3.07/6.07 | 3.07/6.07 |
| 64 | cadence.tasks | 1.47/0.75 | 1.82/0.93 | 1.87/0.95 |
| 64 | system.paxos (LWT writes) | 0.00/20.34 | 0.00/20.96 | 0.00/21.04 |
| 64 | cadence.executions | 7.61/6.05 | 7.63/6.08 | 7.67/6.08 |

Visibility tables (8, ~1.05–1.10 W/wf) and history_tree (1.00) are the same in both arms.

## Findings
- **The c=16 deficit reproduces and is statistically real.** Rust − Go at c=16: **−8.8 wf/s [−11.2, −7.0]** (82.4 → 73.6, −11%), p95 **+56 ms [+47, +64]**, p99 **+85 ms [+59, +112]**. All 6 runs, on all three runner CPU types, show Rust slower at c=16 (−5 to −14 wf/s). The earlier 2-rep run (perf-cassandra-c16) that suggested no gap was underpowered.
- **A/A spread** (same binary, Rust − Control): c=16 −1.8 wf/s [−3.0, −0.7], p99 +22 ms [+14, +31]. Small but nonzero (arm-order effect), about 5x smaller than the Rust−Go gap, so it does not explain it. c=1 and c=64 A/A CIs include 0.
- **c=1**: Rust throughput +1.5 wf/s [+1.0, +2.1], p95 +15 ms [+8, +23], p99 not different, 36% less CPU (14.6 vs 22.7 CPU-s). **c=64**: throughput not different (+1.4 [−2.3, +4.0]); p99 +98 ms [+25, +175]; p95 CI includes 0.
- **Rust is not CPU-bound**: total CPU is lower than Go at every concurrency (c=16 −0.6 CPU-s [−0.9, −0.1]); the deficit is latency, not compute.
- **Most likely cause (supported by op counts, not proven by a profile):** per-workflow persistence is identical for executions, history_node, history_tree and visibility, but Rust issues more matching task-table ops under concurrency (cadence.tasks reads +14% / writes +14% at c=16; +24% / +24% at c=64) and 2–3% more LWT (paxos) phases (20.77 vs 20.38 at c=16; 20.96 vs 20.34 at c=64). That points to a lower sync-match rate in Rust matching, so more tasks are persisted and re-read instead of handed to a waiting poller. It fits the shape: no extra ops at c=1 (poller always waiting), growing at c=16/64. The control arm (same binary) shows the same extra ops, so it is binary behaviour, not noise.
- **Not isolated**: the exact Rust matching path that loses sync matches, and no `perf` profile was taken (needs `profile=1`, which changes the image). Next step: run with `trace_decision=true`/matching metrics to compare sync-match rate Go vs Rust at c=16, then diff Rust task-list dispatch/poll code against Go `service/matching/tasklist` (sync-match handoff, `UpdateTaskList` frequency).
- No Rust code change in this branch (evidence only).
