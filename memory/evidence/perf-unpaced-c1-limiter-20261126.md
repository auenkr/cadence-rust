# Unpaced c1 with limiter parity fix: 16-run pooled study (2026-11-26)

Status: PREDECLARATION (committed before any dispatch). Results are appended below by a later commit.

## Predeclared criteria
- Same criterion as `perf-c1-knee-20261126.md`: run-cluster bootstrap 95% CI upper bound of (Rust-Go)/Go <= +5% for BOTH p90 and p95 of pooled request latencies.
- 16 total unpaced runs = the 4 existing runs (45750371177 45750374664 45750377978 45750381921, source 0cadb6b05) + 12 new runs with identical inputs. No further reruns, whatever the outcome.
- New runs use `control_ref=d9d31badb` (parent 1 of the "Merge perf-limiter-parity" commit, i.e. old limiter) for old-vs-new attribution over the 12 runs.
- Analysis: `perf-c1-knee2-analysis.py` approach (10000 draws, seed 1) plus CPU-model-stratified sensitivity.
