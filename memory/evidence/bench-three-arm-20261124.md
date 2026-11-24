# Three-arm paired benchmark (A/A validation) 2026-11-24

Run: https://github.com/auenkr/cadence-rust/actions/runs/45388343227 (success, ~2h incl. image build; runner CPU: INTEL(R) XEON(R) PLATINUM 8573C)
Dispatch: `control_ref=origin/master repetitions=6 requests=500` on branch bench-three-arm (rust/ tree identical to master).

## Usage
`gh workflow run frontend-matching-benchmark.yml --ref <candidate> -f control_ref=origin/master`. Empty `control_ref` keeps the two-arm mode.
Arms go/control/rust run in one job, rotated order per repetition (go,control,cand / control,cand,go / cand,go,control...), fresh DB each.
Outputs: `paired-benchmark/three-arm-report.{md,json}`, `control-binary.sha256`, `rust-binary.sha256`, `cpu-model.txt`, `control-head.txt`, `control-same-tree.txt`.
Strict two-arm `verify_artifacts.py` is skipped in three-arm mode (it only knows go/rust).

## A/A result (candidate/control throughput ratio, 6 reps)
| conc | median | min | max |
|---|---|---|---|
| 1 | 1.014 | 0.965 | 1.037 |
| 16 | 1.006 | 0.994 | 1.015 |
| 64 | 1.006 | 0.963 | 1.025 |

Medians within +/-1.4% of 1.00; per-rep spread up to +/-3.7% (c1, c64), c16 within +/-1.5%. p50 latency ratio medians 0.987-1.020; c64 p50 has one outlier rep (range 0.76-1.28), so use throughput ratio / p99 not c64 p50 for fine decisions. Conclusion: differences below ~3% are not resolvable at 6 reps; >5% median shifts outside the min/max band are.
Binary SHA256 (both arms, identical): cadence-server-real b74167ee..., cadence-history f02fe48f...

## Limitations
- Two uncached fat-LTO Rust builds take ~45 min each; a true A/B with different Rust trees (~90 min of builds + ~80 min bench) does NOT fit one 90-minute job. The A/A used the shortcut: when rust/, go, go-overlay trees are identical the control image is a retag of the candidate (same binaries). A real A/B needs the control image built in a separate job (docker save artifact) or reduced reps/concurrency; not implemented here.
- The first attempt (run 45378671966) built both images and was cancelled at ~64 min inside the control build because it would exceed 90 min.
- Benchmark itself took ~80 min for 6 reps x 3 arms x 3 concurrency levels at requests=500; reduce `repetitions`/`concurrency` for faster runs.
