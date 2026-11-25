# Default-config tail replication (2026-11-25)

Branch `perf-default-tails`. **No product code change.** Source/head `7a1a3ced4da4663aa3df4b7cf5a5bf2077815590` (master), `control_ref=origin/master` (A/A arm), `rust_services=frontend,matching,history`, workflow `frontend-matching-benchmark.yml` (paired legacy mode), 5 reps x 1000 req. Rust cadence-server sha256 `a3d4752e1489ee03...` in all runs (Go binary hash differs per run: Go image rebuilt each time).
A first dispatch set (45624362437..45624392917) wrongly used `sdk_openloop=true` (fixed-arrival harness); discarded.

| id | run | config | runner CPU | conclusion |
|---|---|---|---|---|
| a | [45633803361](https://github.com/auenkr/cadence-rust/actions/runs/45633803361) | MySQL default c=1,16 | EPYC 7763 | success |
| b | [45633808354](https://github.com/auenkr/cadence-rust/actions/runs/45633808354) | MySQL default c=1,16 (repeat) | EPYC 7763 | success |
| c | [45633814918](https://github.com/auenkr/cadence-rust/actions/runs/45633814918) | PostgreSQL default c=1 | EPYC 9V74 | success |
| d | [45633822180](https://github.com/auenkr/cadence-rust/actions/runs/45633822180) | MySQL `transferProcessorMaxPollRPS:1000` c=64 | EPYC 9V74 | success |
| e | [45633827327](https://github.com/auenkr/cadence-rust/actions/runs/45633827327) | MySQL default c=1, `trace_decision=true` | EPYC 9V45 | success |
| f | [45633832466](https://github.com/auenkr/cadence-rust/actions/runs/45633832466) | d + `heap_profile=1`, 3 reps | - | cancelled (not needed, see 3) |

All runs: 4 vCPU (2c x 2t), 15 GiB, Ubuntu 26.04.1, kernel 7.0.0-1020-azure; service containers 1 cpu/1.4 GiB, DB 1.5 cpu/1.8 GiB. Compare within a run only. Tables are medians (latency ms) computed from `result-*-samples.json` / `resources-*.json`.

## 1. Replication

MySQL default, a+b pooled (10 reps):

| c | arm | wf/s | p50 | p95 | p99 |
|--|--|--|--|--|--|
| 1 | go | 21.8 | 44.1 | 56.4 | 96.6 |
| 1 | control | 24.9 | 27.7 | 100.0 | 104.5 |
| 1 | rust | 25.3 | 27.5 | 99.8 | 103.2 |
| 16 | go | 40.2 | 405.8 | 543.2 | 596.6 |
| 16 | control | 61.9 | 260.2 | 353.3 | 386.0 |
| 16 | rust | 62.1 | 259.9 | 348.6 | 387.2 |

A/A spread (control vs rust, per-run medians): c1 p95 -0.4..0.3 ms, c16 p95 0.0 / -4.1 ms (a / b), c16 p99 -0.3 / +0.2 ms. Between-run (a vs b) Go c1 p95 54.8 vs 61.1, c16 p95 517 vs 548: runner noise on Go is larger than any Rust delta.
**c16: Rust p95/p99 are 0..-1.3% vs A/A and 36%/35% below Go; the earlier +1.8%/+0.7% over Go (396/435 vs 389/432) did not replicate** (Go was 543/597 here).

c1 p95 is run-dependent for Go: Go 54.8/61.1 (a/b, EPYC 7763), 106.7 (c, Postgres), 99.8 (e). Rust is a constant ~100 (two 50 ms limiter waits). So the "Rust 99.8 vs Go 89.2" c1 p95 gap is Go-side variance in how often its slower pace stays under the 20 rps / burst-20 limiter, not a Rust drift (Rust p50 is 27 ms vs Go 44 ms, throughput +16%).

PostgreSQL default c=1 (run c, 5 reps): go 23.7 wf/s p50 23.3 p95 106.7 p99 196.9; control 25.4 / 19.9 / 100.8 / 131.4; rust 25.2 / 19.9 / 101.2 / 170.6 (per-rep p99 range rust 100.6-113.1 p95 only; p99 control 131 vs rust 171 = A/A spread 30%). **Rust p99 170.6 < Go 196.9; the earlier 105.3 vs 103.2 gap did not replicate and A/A spread exceeds it.**

## 2. c1 avoidable work (trace run e)

Transfer-queue-v2 `DTRACE queuev2 load wait_ms` summed per workflow over the measurement window, mean latency split into limiter wait and the remainder (median of 5 reps; Go also logs the same trace, EPYC 9V45):

| arm | mean lat ms | limiter wait / wf | everything else / wf | permits/wf | permits that waited / wf |
|--|--|--|--|--|--|
| go | 41.3 | 21.9 | 18.2 | 2.97 | 0.53 |
| control | 38.5 | 33.8 | 4.7 | 2.92 | 0.76 |
| rust | 39.4 | 33.1 | 5.0 | 2.89 | 0.76 |

Non-limiter time: Rust 5.0 ms vs Go 18.2 ms (3.6x smaller). The condition "non-limiter >= Go" is never met, so no code fix, rust-ci, history-mixed-gate or re-benchmark was done. Rust's whole c1 tail is limiter wait (faster completion => more permits/s/shard hit the 20 rps/burst-20 limiter, as in perf-c1-burst-20261125.md). Changing it would alter the Go-parity limiter, which is not allowed. Per-run c1 for e: go 24.2 wf/s p95 99.8; rust 25.4 wf/s p95 100.6.

## 3. Raised-limit c64 History memory (run d)

Peak cgroup anon MiB, median (min-max) of 5 reps: History go 88.3 (88.0-90.4), control 84.1 (81.2-86.7), rust 85.1 (83.0-87.6). Rust is 3.6% **below** Go (earlier 105 vs 102 did not replicate); A/A control-vs-rust 1.0 MiB. Other memory at the same c: FE go 40.7 / rust 24.3, MT go 30.6 / rust 20.2. Also a/b/c1/c16: History go 71.7-73.0 / rust 68.6-72.0 (c1), go 92.4-92.6 / rust 88.6-89.2 (c16).
Because Rust is not above Go beyond A/A, the heap-profile run f was cancelled and no allocation fix was made. Run d throughput (wf/s) go 53.0 / ctl 65.7 / rust 68.3, p95 2292 / 1819 / 2039 (A/A spread 12%, c64 highly noisy).

## Conclusion
All four council open items were runner/Go variance; none reproduced as a Rust regression, and the c1 breakdown shows no avoidable Rust work. No code changed; History queue v2 limiter stays 20 rps / burst 20.
