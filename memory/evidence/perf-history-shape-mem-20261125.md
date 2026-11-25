# History-shape peak memory, Rust vs Go (2026-11-25)

Branch `perf-history-shape-mem`, head `f07a5ad3e82e29b738e504c6cb314013d9e0bb95`. Not merged.

## Finding

The 1.21x gap (182 vs 150 MiB) in `perf-workload-shapes-20261125.md` is the **sum of the frontend, matching and history containers**. The History container was not the cause:

- Per-container peak anon (median of 3 reps, baseline heap-profile run [45649738867](https://github.com/auenkr/cadence-rust/actions/runs/45649738867)): Rust history 50.4–52.5 MiB vs Go 56.3–58.6 MiB. History was already at or below Go.
- The excess was in the Frontend (`cadence-server` binary): about 97 MiB for Rust vs about 57 MiB for Go.
- Cause: `cadence-server` exported its jemalloc config as the unprefixed `malloc_conf`. tikv-jemalloc-sys builds jemalloc with the `_rjem_` symbol prefix, so the setting was never read and jemalloc ran with its defaults (many arenas, 10 s decay). `cadence-history` already used `_rjem_malloc_conf` (fixed earlier, see `eff-history-rss.md`).
- The History heap profile showed only about 20 MB live heap (jeprof, 20.4 MB total), so there was no large live allocation to remove. The difference was allocator retention, not decoded-history or blob buffering.

## Change

`rust/crates/cadence-server/src/main.rs`: export `_rjem_malloc_conf` with `background_thread:true,dirty_decay_ms:250,muzzy_decay_ms:0,narenas:2,tcache_max:4096`, the same string cadence-history uses. `_RJEM_MALLOC_CONF` still overrides it. No API, logic, cache size or default-config change. Behavior is identical; only allocator retention changes.

## Results

Setup: `frontend-matching-benchmark.yml`, 3-arm mode, `rust_services=frontend,matching,history`, `sdk_openloop=false`, payload 256 B, 3 reps, control = `origin/master` (old config), rust = this branch. MySQL 8.0. Medians over 3 reps. Peak anon is the sum of the frontend, matching and history containers.

Runner: GitHub `ubuntu-latest` (ubuntu26 image 20261116.202.1), AMD EPYC 7763, 4 vCPU (2 cores × 2 threads), 15 GiB RAM, kernel 7.0.0-1020-azure. Service containers 1 CPU / 1400 MiB, DB 1.5 CPU / 1800 MiB. This run landed on EPYC, while the earlier shapes report used a Xeon 8573C, so compare ratios, not absolute numbers.

### history shape (20 req, c4, 1200+ events + replay query)

Run [45655697333](https://github.com/auenkr/cadence-rust/actions/runs/45655697333), conclusion success.

| Arm | peak anon MiB (fe+match+hist) | frontend | matching | history | wf/s | p50 ms | p99 ms | failed |
|---|---|---|---|---|---|---|---|---|
| go | 149.6 | 57.9 | 36.1 | 57.3 | 0.23 | 17112 | 17494 | 0 |
| control (master) | 182.3 | 96.7 | 34.0 | 50.7 | 0.32 | 12259 | 12478 | 0 |
| rust (this branch) | **142.8** | 58.9 | 32.1 | 52.3 | 0.32 | 12239 | 12580 | 0 |

Rust/Go peak anon: 0.95 (was 1.22 for control). Target (≤ Go) met. Rust/Go CPU-s 0.71 (58.1 vs 81.3). Three-arm report: cand/control throughput 0.997 [0.984, 1.013], p50 0.998.

### activities shape (300 req, c16), default-shape regression check

Run [45655718708](https://github.com/auenkr/cadence-rust/actions/runs/45655718708), conclusion success.

| Arm | peak anon MiB | frontend | matching | history | wf/s | p50 ms | p99 ms | failed |
|---|---|---|---|---|---|---|---|---|
| go | 140.9 | 37.1 | 31.6 | 72.2 | 11.25 | 1375 | 2786 | 0 |
| control (master) | 106.6 | 23.1 | 22.9 | 60.5 | 14.82 | 1036 | 1928 | 0 |
| rust (this branch) | 98.1 | 18.2 | 19.8 | 60.2 | 14.37 | 996 | 2065 | 0 |

No regression: cand/control throughput 1.010 [0.825, 1.311] (spread is noise, as in the earlier A/A), CPU-s 20.1 vs 20.9.

## Not done

- c16/c64 default-shape checks were covered by the activities shape at c16 only. No c64 run was dispatched.
- The History heap profile was only taken on the baseline run (45649738867); the symbols did not resolve (`jeprof` showed raw addresses), so no per-function breakdown is recorded.

## CI

| Check | Run | Head | Conclusion |
|---|---|---|---|
| rust-ci `package=cadence-server` (clippy + test) | [45655673902](https://github.com/auenkr/cadence-rust/actions/runs/45655673902) | f07a5ad3e | success |
| history benchmark (rust arm) | [45655697333](https://github.com/auenkr/cadence-rust/actions/runs/45655697333) | f07a5ad3e | success |
| activities benchmark | [45655718708](https://github.com/auenkr/cadence-rust/actions/runs/45655718708) | f07a5ad3e | success |
| baseline heap profile | [45649738867](https://github.com/auenkr/cadence-rust/actions/runs/45649738867) | 4219d987b | success |

Rust binary SHA256 (this branch): cadence-server `f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74`, cadence-history `1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5`. Control: cadence-server `ce91665eea96414ff5ebcf043c4012daf1ad79f2866c530435f6f463f81dc7bd`, cadence-history `c999c5c9e51ea721ce774dc123151b676f29e1c7b8896add03512e9723bd2069`.

## Reproduction

```
gh workflow run frontend-matching-benchmark.yml --ref perf-history-shape-mem -f shape=history \
  -f control_ref=origin/master -f rust_services=frontend,matching,history -f sdk_openloop=false \
  -f requests=20 -f warmup=2 -f concurrency=4 -f repetitions=3 -f payload_bytes=256
```
