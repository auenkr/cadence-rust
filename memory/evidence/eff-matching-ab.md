# eff-matching-ab: same-CPU A/B of eff-matching-v2 (2026-11-21)

Workflow `sdk-matching-benchmark.yml` on master, service=matching rate=25 duration=60 warmup=10 repetitions=3 mysql_tmpfs=true.
before = 119df57fce5c439854b53d2bf6662df94a6524ab, after = 10c29bcb8f3ee054f872a891c40d9ebdcad912d3 (merge 760dea66f lineage).
Aggregated with `Benchmark/agg.py` (medians of 3 pairs per run). 0 errors in all runs. Runs 4/4 rounds dispatched in parallel.

| Round | Arm | Run | Runner CPU | Rust CPU s | Go CPU s | Rust/Go CPU | Rust p99 ms | Go p99 ms | Rust/Go p99 | Used |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| 1 | before | [44795749203](https://github.com/auenkr/cadence-rust/actions/runs/44795749203) | EPYC 9V74 | 4.37 | 4.89 | 0.89 | 114.5 | 113.4 | 1.01 | yes |
| 1 | after  | [44795754031](https://github.com/auenkr/cadence-rust/actions/runs/44795754031) | EPYC 9V74 | 3.84 | 4.90 | 0.78 | 113.6 | 116.3 | 0.98 | yes (pair with R1 before) |
| 2 | before | [44795758646](https://github.com/auenkr/cadence-rust/actions/runs/44795758646) | EPYC 7763 | 4.91 | 5.79 | 0.85 | 168.7 | 170.3 | 0.99 | rejected (7763) |
| 2 | after  | [44795764175](https://github.com/auenkr/cadence-rust/actions/runs/44795764175) | EPYC 9V45 | 5.01 | 5.31 | 0.94 | 114.2 | 112.6 | 1.01 | unpaired (CPU mismatch) |
| 3 | before | [44795770626](https://github.com/auenkr/cadence-rust/actions/runs/44795770626) | EPYC 7763 | 4.60 | 5.57 | 0.82 | 170.1 | 167.8 | 1.01 | rejected (7763) |
| 3 | after  | [44795775380](https://github.com/auenkr/cadence-rust/actions/runs/44795775380) | EPYC 9V74 | 4.46 | 5.55 | 0.80 | 151.9 | 167.1 | 0.91 | extra 9V74 sample |
| 4 | before | [44795780358](https://github.com/auenkr/cadence-rust/actions/runs/44795780358) | EPYC 7763 | 7.53 | 7.70 | 0.98 | 3224.0 | 2306.6 | 1.40 | rejected (7763, saturated) |
| 4 | after  | [44795786071](https://github.com/auenkr/cadence-rust/actions/runs/44795786071) | EPYC 9V74 | 4.39 | 5.48 | 0.80 | 154.5 | 162.3 | 0.95 | extra 9V74 sample |

## Valid same-CPU comparison (EPYC 9V74 only)
Only one round landed both arms on the same CPU model (round 1). Rounds 3/4 "after" also hit 9V74 but their "before" hit 7763, so
the only 9V74 "before" sample is round 1. Using all 9V74 samples:

| | Rust CPU s | Rust/Go CPU | Rust p99 ms | Rust/Go p99 |
|---|---:|---:|---:|---:|
| before (n=1) | 4.37 | 0.89 | 114.5 | 1.01 |
| after (n=3: 3.84 / 4.46 / 4.39) | 4.39 median | 0.80 median (0.78/0.80/0.80) | 113.6 / 151.9 / 154.5 | 0.98 / 0.91 / 0.95 |

Strict same-run pair (round 1): Rust CPU 4.37 -> 3.84 s (-12%), Rust/Go CPU 0.89 -> 0.78.

## Verdict
Directionally real but small and not statistically established: Rust/Go CPU ratio improves 0.89 -> ~0.78-0.80 on every 9V74 "after"
sample, which fits the removed spawn_blocking hop. Absolute Rust CPU is noisy (3.84-4.46 s on the same CPU model, Go itself moves
4.89-5.55 s), and the before side has only n=1 valid sample. p99 is unchanged (Rust/Go ~0.91-1.01, within noise).
Expect roughly 10% less matching CPU; a firm claim needs more 9V74 "before" samples (runner CPU is not selectable).
