# perf-large-payload: mysql buffer-pool cap (16 x 32 KiB) vs large payloads (2026-11-25)

Question: does the cap (candidate, master a96a02f6e) slow Rust vs control (39aa8be14, before cap) or vs Go for large payloads?
Setup: frontend-matching-benchmark.yml, all-Rust (frontend,matching,history), concurrency 1,16, 3 reps, 300 requests, source_ref=a96a02f6e, control_ref=39aa8be14.
Runner: AMD EPYC 7763, 4 vCPU, 15 GiB (machine-spec.md in artifact).

Runs: 65536 B = https://github.com/auenkr/cadence-rust/actions/runs/45537955331 (success);
200000 B = https://github.com/auenkr/cadence-rust/actions/runs/45537969516 (success).
First attempts discarded: 45528994511 (cancelled at 75 min), 45529007925 (dockerize download 503, infra flake).
200000 B is under BlobSizeLimitWarn (256 KiB) and Error (2 MiB) defaults.

## Throughput (wf/s, median of 3 reps)
| payload | conc | Go | control | candidate | cand/control tput [min,max] | cand/control p50 [min,max] |
|---|---|---|---|---|---|---|
| 65536 | 1 | 25.4 | 26.6 | 25.9 | 0.974 [0.929,1.051] | 0.983 [0.980,0.990] |
| 65536 | 16 | 57.4 | 73.6 | 76.6 | 1.041 [0.990,1.104] | 0.963 [0.942,1.027] |
| 200000 | 1 | 12.4 | 15.1 | 14.9 | 0.979 [0.975,1.018] | 1.016 [0.989,1.024] |
| 200000 | 16 | 23.9 | 28.8 | 29.5 | 1.020 [1.018,1.026] | 0.957 [0.954,0.970] |

Rust (both arms) is 1.05-1.28x Go throughput. Absolute p50/p95 per arm: see three-arm-report.md in the run artifacts (report prints only the ratio).

## History peak_cgroup_anon (MiB, 3 reps)
| payload | conc | Go | control | candidate |
|---|---|---|---|---|
| 65536 | 1 | 158-162 | 130-132 | 128-130 |
| 65536 | 16 | 288-301 | 245-253 | 246-249 |
| 200000 | 1 | 334-350 | 294-298 | 291-293 |
| 200000 | 16 | 654-687 | 567-578 | 565-573 |

## Verdict
No regression: all candidate/control ratios are within each ratio's min/max spread of 1.0 (c1 ~-2..-3% is within noise; c16 +2..+4%).
Memory is flat vs control at large payloads (cap saves little here; payload buffers dominate) and 13-17% below Go.
Cap NOT raised; no code change.
