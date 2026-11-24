# Three-arm benchmark: separate control build job (2026-11-24)

Branch `bench-three-arm` (rebased on master with ci-speedup), head cc16b219a.
Change: control Rust binaries are built in a `build-control` job (buildx, gha scope `rust-image-fat-ctrl`, `binaries` target, tarball artifact retention 1 day); `paired-benchmark` needs it, downloads, assembles the control image. Same-tree shortcut kept (retag).

## Runs
- 45403316953: failed in 37s, control_ref b1180fbd4 predates the Dockerfile `binaries` stage (bad ref choice).
- 45403569898: build-control 35m15s (cold, success); paired-benchmark failed at 32m: tar extract path bug (fixed in cc16b219a).
- **45415715195 (SUCCESS)**: https://github.com/auenkr/cadence-rust/actions/runs/45415715195
  head cc16b219a, control_ref abc4942b2 (rust tree differs; control sha256 26194c33... vs candidate c041f5c3...), rust_services=frontend,matching,history, repetitions=2.
  - build-control 1m15s (warm cache; 35m cold), paired-benchmark 16m02s. All jobs < 90 min.

## Results (median of 2 interleaved reps, same runner; workflows/s)
| conc | go | control | candidate | cand/control tput | cand/control p50 |
|---|---|---|---|---|---|
| 1 | 23.4 | 25.0 | 25.1 | 1.007 | 0.988 |
| 16 | 45.1 | 64.9 | 64.6 | 0.995 | 1.002 |
| 64 | 22.6 | 60.1 | 60.8 | 1.012 | 0.944 |

## Runner spec
AMD EPYC 7763, 2 cores x 2 threads (nproc 4), 15Gi RAM, kernel 7.0.0-1020-azure, Ubuntu 26.04.1 (image ubuntu26 20261116.202.1), mysql:8.0; containers: 4x 1 cpu/1400MiB + 1x 1.5 cpu/1800MiB.
Only 2 reps: ratios within ~3% spread, so no regression resolvable. Full report in artifact `frontend-matching-paired-benchmark` (3 days).
