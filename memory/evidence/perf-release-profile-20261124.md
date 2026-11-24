# Release profile: fat LTO + codegen-units=1 (2026-11-24)

Branch `perf-release-profile`. Build settings only; no code or behaviour change.

## Settings
| | Before | After |
|---|---|---|
| `rust/Cargo.toml` `[profile.release]` | none | `lto = "fat"`, `codegen-units = 1` (panic untouched, no target-cpu) |
| `rust/Dockerfile`, `Benchmark/Dockerfile` | forced `LTO=off`, `CODEGEN_UNITS=16` | build ARGs `RELEASE_LTO=fat`, `RELEASE_CODEGEN_UNITS=1` |
| Old A/B | | `--build-arg RELEASE_LTO=off --build-arg RELEASE_CODEGEN_UNITS=16` |

The 90-minute job limit was not hit, so no fallback to thin LTO was needed.

## Runs
| Check | Run | Result |
|---|---|---|
| rust-ci (full workspace) | https://github.com/auenkr/cadence-rust/actions/runs/45334522594 | success (~10 min) |
| History mixed gate, mysql | https://github.com/auenkr/cadence-rust/actions/runs/45334570481 | success, log has `GATE SUMMARY failed: []` (~15 min) |
| Paired benchmark, `rust_services=frontend,matching,history`, new | https://github.com/auenkr/cadence-rust/actions/runs/45334544627 | success, strict artifact verification valid, job ~69 min |
| Paired benchmark, baseline (LTO off, cu=16) | https://github.com/auenkr/cadence-rust/actions/runs/45291978847 | success |

Both benchmark runs used the same inputs (1000 requests, 10 warmup, 5 repetitions, c1/c16/c64, 1024 B payload).

## Build time and size
| | Baseline 45291978847 | New 45334544627 |
|---|---|---|
| Rust image `cargo build --release` (cadence-server + cadence-history, `-j2`) | 34m03s | 41m47s (+23%) |
| cadence-server sha256 | 3d39074f89e0... | 3f2d061200487af9... |
| cadence-history sha256 | f549c3c9eb83... | c075740b4fc7... |

Binary size was not captured by the harness, so it is not reported here.

## Hardware
Both runs landed on `AMD EPYC 7763 64-Core Processor` (2 cores x 2 threads, same runner image), so cross-run comparison is reasonable. Run-to-run variance still applies (the Go arm moved by up to 7% between the runs).

## Results (5-run medians; Rust = Frontend+Matching+History in Rust, Go Worker)
CPU = cgroup CPU seconds per container per 1000 workflows.

| Conc | Metric | Baseline Go | Baseline Rust | New Go | New Rust |
|---:|---|---:|---:|---:|---:|
| 1 | wf/s | 22.669 | 25.173 | 22.958 | 24.955 |
| 1 | frontend CPU s | 7.724 | 4.584 | 7.368 | 3.442 |
| 1 | history CPU s | 22.381 | 9.150 | 21.684 | 7.244 |
| 1 | matching CPU s | 4.658 | 3.024 | 4.427 | 2.270 |
| 16 | wf/s | 41.902 | 60.615 | 42.902 | 63.844 |
| 16 | frontend CPU s | 6.573 | 4.811 | 6.273 | 3.591 |
| 16 | history CPU s | 20.220 | 13.024 | 19.531 | 10.844 |
| 16 | matching CPU s | 4.449 | 4.096 | 4.236 | 3.087 |
| 64 | wf/s | 20.674 | 57.076 | 21.570 | 58.635 |
| 64 | frontend CPU s | 6.871 | 4.866 | 6.527 | 3.739 |
| 64 | history CPU s | 46.022 | 14.043 | 44.078 | 11.916 |
| 64 | matching CPU s | 4.923 | 4.085 | 4.651 | 3.252 |

Rust CPU change, new vs baseline Rust arm:
| Conc | frontend | history | matching | wf/s |
|---:|---:|---:|---:|---:|
| 1 | -24.9% | -20.8% | -24.9% | -0.9% |
| 16 | -25.4% | -16.7% | -24.6% | +5.3% |
| 64 | -23.2% | -15.1% | -20.4% | +2.7% |

Worker CPU is Go in both arms and is unchanged within noise (0.09-0.26 s).

Rust peak cgroup memory is essentially unchanged (history c64 136.4 MiB vs 137.3 MiB).

Conclusion: fat LTO + codegen-units=1 cuts Rust CPU per request by about 15-25% across the three services. Throughput is flat at c1 and about +3-5% at c16/c64. The cost is about 8 more minutes of release build time. Latency tables are in the downloaded run artifacts (`comparison-c*.md`).
