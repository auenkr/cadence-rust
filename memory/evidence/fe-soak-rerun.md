# Frontend soak rerun, Go vs Rust (2026-11-20/21)

Run: https://github.com/auenkr/cadence-rust/actions/runs/44813614531 (workflow `frontend-soak.yml`, dispatched on branch `fe-soak-rerun` = master `4af33f095ed6c1133488b0ed3695de12123791aa`, which includes the Benchmark/run.py Worker-wait fix). Workflow conclusion **success**; soak-verdict **RUST OVERALL: FAIL** on one criterion (below).
Inputs: rate=10/s, duration_minutes=70, lanes=["go-baseline","rust-fe"], restart of the Frontend at 2100 s (same as perf-fe-soak-start / replacement pairs).
Rust binary SHA256: `ee30e5b608ba937aba7537dddc170397de0613d6d1127bdb42a8a93de2901b63`.

CPU models: go-baseline lane Intel Xeon Platinum 8573C; rust-fe lane **AMD EPYC 7763** (different CPUs, so ms-level comparisons are weak).

| Metric | Go | Rust-FE |
|---|---:|---:|
| started / completed / failed | 42000 / 42000 / 0 | 42000 / 41999 / 1 |
| Start p50 / p95 / p99 (ms) | 9 / 12 / 19 | 9 / 15 / 22 |
| Start max | not recorded in summary.json | not recorded |
| Query p50 / p95 / p99 (ms) | 12 / 32 / 44 | 12 / 34 / 46 |
| E2E p50 / p99 / max (ms) | 3087 / 4112 / 7742 | 3089 / 4125 / 8573 |
| query "init" attempts / workflows | 28488 / 21054 | 28859 / 21104 |
| FE RSS mean MB (peak) | 115.1 (117.4) | 79.6 (83.1) |
| FE CPU mean % | 24.7 | 11.8 |
| MT RSS mean MB / CPU mean % | 108.5 / 15.8 | 114.2 / 18.1 |
| FE RSS slope MB/h | 5.29 | 1.34 |

Ratios Rust/Go: FE CPU 0.48, FE RSS 0.69 (Matching, which is Go in both lanes, 1.14 CPU / 1.05 RSS = runner noise).

## Errors
Rust lane: 1 failure, `fe-soak-...-0020994 query query state "init"` (query stage), the soak driver's 90 s retry gave up with state still `init` near the restart. Go lane in this run: 0 failures. The same single `init` failure has been seen in the Go lane in earlier soaks (44366018885, 44681388177, 44682455258: Go 41,999/42,000), so it is a driver/Cadence query-semantics artefact around the restart, not Rust-specific (see fe-soak-query-init.md). Note it is the same workflow index 0020994 as in 44366018885.

## Tail flake (Start p99)
Start p99 was 22 ms Rust vs 19 ms Go, within the 1.5x criterion (PASS). Start p99 criterion did not fail; the historical 123 ms Rust Start p99 did **not recur** (also absent in replacement pairs 44690902433/44690920852, 9 ms each). Verdict: gone in this run (one more clean sample; the Rust lane even ran on the slower EPYC 7763).

## Comparison with previous soaks
- Previous replacement pair 1 (44690902433): Start p99 Go 9 / Rust 9 ms, both 42000/42000. Pair 2 (44690920852): 8 / 22 ms (Go EPYC 9V45, Rust EPYC 7763), both 42000/42000, zero failures.
- Earlier failing run (44426979723): Start p99 Rust 123 ms vs Go 35 ms.
- Now: 19 / 22 ms. Matches pair 2 on EPYC 7763 (22 ms), i.e. consistent with a slower CPU, not a tail spike. The sole failing criterion is the one `init` query (1 of 42000), which previously appeared on the Go side.
