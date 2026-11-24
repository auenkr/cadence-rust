# CI speedup (branch ci-speedup, 2026-11-24)

Changes: Swatinem/rust-cache (v2.7.8) in 6 gate workflows (`fresh=true` disables it; saves only on master/ci-speedup);
`release_profile` input (gate = thin LTO + cgu16 default, release = fat/1) for history-soak/restartgate, logged in out/profile.txt;
cargo-chef dependency layer + `binaries` stage in rust/Dockerfile, built with buildx GHA layer cache in the paired benchmark (fat LTO kept);
`GATE SUMMARY failed: [...]` added to tests/run-go-service-baseline.py (Matching gate) and asserted in matching-mixed-gate.
Already done before this work (documented only): rust-ci caches (clean_target input), benchmark arms alternate order per repetition on one runner, CPU model recorded (lscpu).

## Wall times (minutes; cold = first run with empty cache on branch, warm = second)
| Workflow | Before (master) | Cold | Warm | Cold run | Warm run |
|---|---|---|---|---|---|
| matching-mixed-gate | 8 | 8 | 7 | 45369537925 | 45381123578 |
| history-swap | 8 | 8 | 8 | 45369541927 | 45381128410 |
| history-mixed-gate | 15 | 14 | 14 | 45369545709 | 45381133988 |
| frontend-mixed-gate | 12 | 12 | 13 | 45369550229 | 45381140190 |
| history-restartgate | 50 | 49 | 44 (build 21 -> 16) | 45369660430 | 45381144976 |
| history-soak | 70 | 70 | 71 (build 24 -> 23) | 45369563094 | 45381149156 |
| benchmark (paired) image build | 35-42 | 33.7 (cold, fat LTO) | not measured | 45392233444 (success, total 55 min) | - |

All six gates passed cold and warm. Benchmark failed twice first (chef `-j` flag, missing vendor/ for [patch]; fixed in c332ac5b4).
Not run (time-box): warm benchmark run, rust-ci run (workflow unchanged).

## Findings
- rust-cache stores third-party deps only; workspace crates dominate builds, so debug gate builds stay about 3-5 min. Only the restartgate release build improved (21 -> 16 min).
- Thin LTO/cgu16 did not shorten the cold release build (21-24 min) noticeably; cause not investigated.
- `GATE SUMMARY failed: []` was not re-checked in every warm log; history-mixed and matching assert it in the workflow, the others print it from their harness.

## Cache usage
`gh cache list` showed 10.5 GB (mostly setup-go caches duplicated on ~10 old perf-* branches). Deleted all caches not on master/ci-speedup -> 4.5 GB; 5.9 GB after the benchmark run (< 10 GB).
