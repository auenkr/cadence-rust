# Perf: Rust Frontend+Matching c=64 p99 tail (2026-11-19)

Question: benchmark rerun 44395943619 (source 9592b349a) showed Rust FE+Matching c=64 p99 +798 ms vs Go (throughput −1.5%). Real or runner noise?

Re-run: frontend-matching-benchmark.yml, `repetitions=3`, defaults (1000 req, 10 warmup, c=1,16,64, 1 KiB, tchannel), `source_ref=56b58c6c1a32bce1bd914c29dbc5a4766887967c` (master; no FE/Matching code change since 9592b349a beyond History block_in_place).
Run: https://github.com/auenkr/cadence-rust/actions/runs/44426943529 — success. (44426926435 was cancelled by workflow concurrency, a duplicate dispatch.)

## Medians (Rust − Go)

| Conc | Run | Go thr | Rust thr | Δthr | Δp50 ms | Δp99 ms |
|---|---|---:|---:|---:|---:|---:|
| 64 | 44395943619 | 19.695 | 19.410 | −1.5% | −53 | **+798** |
| 64 | 44426943529 | 25.851 | 25.987 | +0.5% | −198 | **−132** |
| 16 | 44426943529 | | | −3.96% | −0.8 | −221 |
| 1 | 44426943529 | | | −1.56% | −1.8 | −25 |

## c=64 per-pair p99 (44426943529)

| Pair | Go p99 ms | Rust p99 ms | Go thr | Rust thr |
|---|---:|---:|---:|---:|
| 0 | 5056 | 5435 | 24.41 | 23.49 |
| 1 | 4547 | 4199 | 25.85 | 27.07 |
| 2 | 3837 | 4415 | 27.05 | 25.99 |

Within-arm p99 spread is 1.2–1.6 s at c=64 (Go 3837–5056, Rust 4199–5435); the per-pair sign flips and the median delta flipped from +798 to −132 ms between identical-code runs. Absolute Go throughput also moved 19.7 → 25.9 wf/s between days.

## Verdict

**Noise, not a regression.** The c=64 p99 delta is well inside run-to-run variance (saturation is in Go History/MySQL: History CPU ≈ 36 s vs FE/Matching ≈ 3.5 s per run). No code change made; no FE/Matching fix warranted, so rust-ci/mixed gates were not re-run for this branch (no diff). If a stricter answer is needed, use ≥5 repetitions and compare p99 CIs, not single medians.
