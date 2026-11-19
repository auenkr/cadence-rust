# All-Rust cluster load stability (2026-11-19)

Branch `eff-allrust-load` (base master 56b58c6c1). No Rust code change; harness-only change.

## Question
perf-history-tail.md all-25 lane was very noisy (Rust p50 43–3899 ms, Go up to 11 s). Runner overload or Rust degradation mode?

## Diagnosis of the noisy run 44417379825 (old artifacts)
- Collapses are pair-correlated: pair 01 collapsed in BOTH arms (Go p50 11 s, Rust 3.9 s).
- MySQL avg commit latency jumped 0.6 ms -> 10–27 ms, InnoDB log write wait x5 during collapses = runner disk fsync stalls.
- Services CPU < 0.5 cores each, MySQL ~0.5; commit rate unchanged in Rust during collapse (no retry amplification); no Rust errors/warnings in logs; matching task backlog drains.
=> runner disk noise, not a Rust degradation mode.

## New runs (service=all, 5 alternating pairs, 30 s measure, 10 s warmup, 0 errors everywhere)
p50/p99 completion ms per pair, avg MySQL commit ms.

| run | rate | mysql | Go p50/p99 | Rust p50/p99 | commit Go/Rust |
|---|---|---|---|---|---|
| 44427214387 | 25 | disk | 79–81 / 163–182 | 46–51 / 195–210 | 1.0 / 0.5 |
| 44427194305 | 30 | disk | 1171–2567 / 2857–5186 (Go saturates) | 86–97 / 267–275 | 1.9 / 0.7 |
| 44427358333 | 25 | tmpfs | 68–69 / 122–161 | 36 / 183–201 | 0.3 / 0.1 |
| 44427363948 | 30 | tmpfs | 134–288 / 258–1464 | 68–73 / 226–240 | 0.5 / 0.2 |

Host health (new timeline fields, tmpfs runs): iowait 0%, steal 0%, IO PSI 0%, load 1.3–4.4.

## Conclusions
- Rust all-service cluster is stable at 25 and 30/s in all 20 Rust arms (p50 spread <= 11 ms). At 30/s on disk Go saturates while Rust does not.
- Rust issues ~3.5x more COMMIT statements but lower MySQL commit latency; not a load problem.
- Remaining Rust gap: fixed p99 ~190–240 ms (Go ~125–180 ms on tmpfs) — same tail as perf-history-tail.md (~200 ms period), open.
- Transfer_tasks growth within the 40 s window is the 60 s complete-transfer interval (same as Go), not a leak.

## Harness change (Benchmark/)
- `sdk-matching-benchmark.yml` input `mysql_tmpfs` (default false) -> MYSQL_TMPFS, MySQL datadir on tmpfs; removes runner fsync variance. Recommended `true` for load comparisons.
- `collect_resources.py` timeline adds `host`: loadavg, iowait/steal jiffies, IO/CPU PSI, to flag overloaded runners.
- `Benchmark/load_stability_report.py <artifact dir>`: per-arm p50/p99, commit ms, host health.
- Benchmark python tests: test_pipeline 11/11 OK. No Rust code touched, so rust-ci not required.
Temp refs tmp/eal-25, tmp/eal-25-tmpfs, tmp/eal-30-tmpfs hold only benchmark runs.
