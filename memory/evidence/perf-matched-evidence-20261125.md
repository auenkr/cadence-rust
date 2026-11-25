# Matched-rate and long-run benchmark evidence (2026-11-25)

Branch `perf-matched-evidence`. No product code change. Source `38594e592902d292410bfe5b3a9019391b3ff825` (master HEAD),
`control_ref=origin/master` (same tree, so control vs rust is an A/A pair), `rust_services=frontend,matching,history`, workflow
`frontend-matching-benchmark.yml`, all arms interleaved on one runner per run. Rust binary sha256 (cadence-server) `a3d4752e1489ee03...ccb91f`,
cadence-history `1280bea951bcba09...3584b5` (identical in all four runs). Runner (all runs): AMD EPYC 7763, 4 vCPU, 15 GiB, Ubuntu 26.04.1,
kernel 7.0.0-1020-azure; service containers 1 CPU / 1.4 GiB, MySQL (or Postgres) 1.5 CPU / 1.8 GiB. Values are medians over repetitions.
Tables were computed from the run artifacts (result-*-samples.json, resources-*.json, services-*.log) with a throwaway script.

| id | run | config | conclusion |
|---|---|---|---|
| A | [45614942819](https://github.com/auenkr/cadence-rust/actions/runs/45614942819) | MySQL c1, 5 reps x 1000, target_rate=15, trace_decision=true | success |
| B | [45614947836](https://github.com/auenkr/cadence-rust/actions/runs/45614947836) | MySQL c1, 5 reps x 1000, target_rate=15, trace off | success |
| C | [45614953288](https://github.com/auenkr/cadence-rust/actions/runs/45614953288) | MySQL c16, 12000 requests, 1 rep, target_rate=25 | success |
| D | [45614959504](https://github.com/auenkr/cadence-rust/actions/runs/45614959504) | PostgreSQL c16, 12000 requests, 1 rep, unpaced | success |

Go sustained the requested rate in A and B (15.00 / 15.01 wf/s) and in C by throughput (24.98), but see C's latency drift.

## A: c1, rate 15, traced

| arm | wf/s | p50 | p95 | p99 | per-rep p95 | FE/MT/HI peak anon MiB | FE/MT/HI CPU-s |
|---|---|---|---|---|---|---|---|
| go | 15.00 | 43.5 | 52.3 | 58.5 | 52,51,52,53,53 | 34/32/73 | 8/5/23 |
| control | 15.01 | 27.3 | 32.1 | 34.9 | 32,32,32,32,32 | 18/21/67 | 4/2/8 |
| rust | 15.01 | 27.3 | 32.4 | 35.1 | 33,33,32,32,31 | 18/21/66 | 4/2/8 |

Transfer-queue v2 permits (measured window, request time = log time - wait):

| arm | permits/s/shard (4 shards) | permits/wf | waited >1 ms | max permits in any 1 s window (one shard) |
|---|---|---|---|---|
| go | 11.3 | 3.00 | 0.0% | 30 |
| control | 11.3 | 3.00 | 0.0% | 30 |
| rust | 11.3 | 3.00 | 0.0% | 31 |

At 15 wf/s every arm uses 11.3 permits/s/shard, well under the 20 rps limiter, and no permit waits in any arm. The 50 ms wait clusters seen in the
unpaced c1 run (perf-c1-burst-20261125.md) disappear, which confirms they came from throughput, not from extra Rust permits.
Rust p50 is 37% lower than Go, p95 38% lower, p99 40% lower, with about half to a third of the CPU-s and half the FE/MT memory.
A/A delta (rust vs control): wf/s +0.1%, p50 0%, p95 +1%, p99 +0.6%, History -1 MiB.

Drift (first vs last third, p50 ms): go 43.4 -> 43.0, control 27.4 -> 27.3, rust 26.7 -> 27.3.

## B: c1, rate 15, trace off

| arm | wf/s | p50 | p95 | p99 | per-rep p95 | FE/MT/HI peak anon MiB | FE/MT/HI CPU-s |
|---|---|---|---|---|---|---|---|
| go | 15.01 | 33.7 | 45.6 | 64.7 | 46,51,42,44,53 | 34/32/71 | 7/4/16 |
| control | 15.01 | 22.2 | 26.8 | 46.1 | 26,27,27,25,27 | 18/21/65 | 4/2/7 |
| rust | 15.01 | 22.8 | 27.2 | 46.8 | 29,26,27,26,28 | 18/21/67 | 4/2/7 |

Rust is 32-34% lower p50, 40-41% lower p95 and 28-29% lower p99 than Go. A/A delta (rust vs control): p50 +2.7%, p95 +1.5%, p99 +1.5%
(all within the per-rep spread). Trace on (A) costs Go about 10 ms p50 and Rust about 5 ms.

## C: MySQL c16, 12000 requests, rate 25

| arm | wf/s | p50 | p95 | p99 | FE/MT/HI peak anon MiB | FE/MT/HI CPU-s |
|---|---|---|---|---|---|---|
| go | 24.98 | 253.5 | 1259.3 | 1637.6 | 38/34/102 | 79/54/381 |
| control | 25.00 | 25.9 | 83.7 | 106.0 | 19/22/81 | 41/24/94 |
| rust | 25.00 | 25.6 | 87.5 | 106.6 | 19/22/82 | 42/24/93 |

| arm | first-third p50 | last-third p50 | first-third p99 | last-third p99 | failed |
|---|---|---|---|---|---|
| go | 58.2 | 302.9 | 1434.4 | 1659.6 | 0 |
| control | 26.0 | 25.8 | 106.1 | 106.0 | 0 |
| rust | 25.8 | 25.3 | 105.2 | 106.1 | 0 |

Go reaches 25 wf/s on average but is saturated: p50 grows 58 -> 303 ms across the run (queue backlog), History used 381 CPU-s.
Rust holds a flat 25-26 ms p50 with no drift, 0 failures, and 4x less History CPU. A/A delta: wf/s 0.0%, p50 -1%, p95 +4.5%, p99 +0.6%,
memory within 1 MiB.

## D: PostgreSQL c16, 12000 requests, unpaced

| arm | wf/s | p50 | p95 | p99 | FE/MT/HI peak anon MiB | FE/MT/HI CPU-s |
|---|---|---|---|---|---|---|
| go | 64.5 | 230.6 | 436.2 | 697.0 | 39/34/141 | 55/34/141 |
| control | 89.6 | 165.9 | 295.7 | 462.8 | 24/23/120 | 37/28/122 |
| rust | 85.9 | 167.5 | 344.3 | 564.1 | 24/35/120 | 37/28/125 |

| arm | first-third p50 | last-third p50 | first-third p99 | last-third p99 | failed |
|---|---|---|---|---|---|
| go | 219.5 | 238.8 | 809.4 | 659.2 | 0 |
| control | 154.1 | 176.2 | 449.8 | 478.6 | 0 |
| rust | 151.6 | 179.4 | 384.3 | 701.3 | 0 |

Both Rust arms beat Go on Postgres (+33% / +38% wf/s, p50 -27% / -28%). The A/A pair differs by 4% in wf/s (candidate/control 0.959), 16% in p95 and
22% in p99, so single-repetition Postgres tail numbers have roughly that noise. Both Rust arms drift about +15% in p50 from the first to last third
(Go +9%). Rust matching peak anon is 35 MiB in the rust arm against 23 in control: a one-sample difference between identical binaries, not a regression
signal (it was 22 MiB in the MySQL c16 run C).

## Conclusion

With a rate cap that Go can sustain, Rust is 32-40% faster at p50/p95 at c1 with no permit waits, matching the burst analysis. At 12000-request c16
the Rust cluster is stable (no drift on MySQL, mild drift on Postgres, 0 failures), while Go degrades under the same rate on MySQL. No parity defect found.
