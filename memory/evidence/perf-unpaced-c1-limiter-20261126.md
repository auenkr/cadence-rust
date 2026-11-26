# Unpaced c1 with limiter parity fix: 16-run pooled study (2026-11-26)

Status: PREDECLARATION (committed before any dispatch). Results are appended below by a later commit.

## Predeclared criteria
- Same criterion as `perf-c1-knee-20261126.md`: run-cluster bootstrap 95% CI upper bound of (Rust-Go)/Go <= +5% for BOTH p90 and p95 of pooled request latencies.
- 16 total unpaced runs = the 4 existing runs (45750371177 45750374664 45750377978 45750381921, source 0cadb6b05) + 12 new runs with identical inputs. No further reruns, whatever the outcome.
- New runs use `control_ref=d9d31badb` (parent 1 of the "Merge perf-limiter-parity" commit, i.e. old limiter) for old-vs-new attribution over the 12 runs.
- Analysis: `perf-c1-knee2-analysis.py` approach (10000 draws, seed 1) plus CPU-model-stratified sensitivity.

## Execution
- 12 new dispatches of `frontend-matching-benchmark.yml` on master: `source_ref=0cadb6b057036bbc7fe4903ad284cab6995113d2`, `control_ref=d9d31badb7503a505be0c455607a9186a1a8f08f`, mysql, `rust_services=frontend,matching,history`, `sdk_openloop=false`, echo, c1, 5 reps x 1000, warmup 10, no target_rate. All 12 success. URL prefix `https://github.com/auenkr/cadence-rust/actions/runs/`:
  45753023726 45753036563 45753049571 45753063352 45753075894 45753087894 45753099285 45753110514 45753122592 45753133971 45753147808 45753161257
- Earlier 4 (two-arm, same tree, no control arm): 45750371177 45750374664 45750377978 45750381921.
- Analysis: `perf-unpaced-c1-limiter-analysis.py` (= knee2 script, arms selectable via `ARMS`), 10000 draws, seed 1. Outputs: `...-analysis-16.txt` (go vs rust, 16 runs), `...-analysis-12.txt` (three arms, 12 runs).
- Binaries: Rust candidate `cadence-server` `e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725` (all 16 runs); `cadence-history` `9bda3c570ee7fb4e222d5b1bcb33d1ed113fddf92f840f6ee942dd2b4db43804` (new runs; the earlier 4 recorded `d86114586c14...` in the parity file). Control (old limiter, d9d31badb) `cadence-server` `220d897089f35934ce81ccbdcae4fbf39b73bb1d9ec20c07db766bc21b180c16`, `cadence-history` `41c8e503cc2ed37261777a5ad8809a3035cb8124841cb025afc39878bcbc1b3e`.
  **Caveat:** in run 45753161257 the control binary SHA is e778... (identical to candidate), i.e. that run's control arm is NOT the old binary. The other 11 runs have the old one. Its control arm is therefore an A/A pair; it was not excluded (effect on the old-vs-new result is nil since the difference is already ~0).
- Go `cadence-server`: a different SHA in nearly every run (Go build is not reproducible byte-for-byte); source identical.
- Runner HW (all 4 vCPU, 15 GiB, Ubuntu 26.04, kernel 7.0.0 aws): CPU mix over the 16 runs: EPYC 9V74 x6, EPYC 9V45 x4, EPYC 7763 x3, Xeon 6975P-C x2, Xeon 6973P-C x1.

## Verdict (criterion: CI upper bound <= +5% for p90 AND p95)
| comparison | runs | metric | point | 95% CI | result |
|--|--|--|--|--|--|
| Rust - Go | 16 | p90 | +27.0% | [+10.2, +46.0] | FAIL |
| Rust - Go | 16 | p95 | +0.6% | [-3.1, +2.8] | ok |
| Rust - Go, CPU-stratified | 16 | p90 / p95 | | [+16.3, +35.5] / [-0.6, +1.4] | FAIL (p90) |
| Rust - Go | 12 | p90 / p95 | +31.8 / +1.0 | [+16.4, +68.2] / [+0.2, +13.0] | FAIL |
| Rust(new) - control(old) | 12 | p90 / p95 | +0.1 / +0.1 | [-1.3, +1.1] / [-0.1, +0.2] | no difference |

**Overall: FAIL on p90** (p95 passes in the 16-run pool). The earlier 4-run p90 +5.1% was a low draw; with 16 runs the gap is +27%.
Pooled 16 runs (80 000 per arm): Go p50/p90/p95/p99 36.2/77.4/100.3/172.6 ms; Rust 24.6/98.2/100.9/134.4. Rust is faster at p50 and p99, but its p90 sits on a ~98-100 ms plateau in every run.

## Interpretation
- Old-vs-new limiter: no measurable effect (p90 +0.1%, CI [-1.3, +1.1]). The parity fix does not change unpaced c1 latency; the earlier "improvement" was Go variance.
- Go p90 depends strongly on runner CPU: EPYC 7763 runs show Go p90 ~50-52 ms (Rust 94), EPYC 9V45/Xeon ~85-88, so the p90 gap is +84% on 7763, +12-17% elsewhere (Sensitivity B). On the slower-per-core 7763 Go finishes under the 100 ms plateau, Rust does not. Go's p95 is similarly CPU dependent.
- Rust's ~98-100 ms p90/p95 plateau is constant across runners and arms (old and new), which points at a fixed ~100 ms wait in the unpaced path (not limiter token accrual, which is unchanged by the fix); Go avoids it in ~10% fewer requests (share >=75 ms: Go 0.109 vs Rust 0.182). Not investigated here; no further reruns per predeclaration.
