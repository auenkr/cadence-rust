# History peak memory at c1, matched start rate (2026-11-25)

Branch perf-c1-rate-mem adds optional `target_rate` (wf/s) to frontend-matching-benchmark.yml -> paired-run.py `--target-rate` -> benchmark `-target-rate` (paces workflow starts; empty = unchanged).
Both runs: source_ref=control_ref=e8acf1eaa10aa9c5334e1de0fea75022ee9ef34a, rust_services=frontend,matching,history, concurrency=1, repetitions=5, dynamic_config_extra `history.transferProcessorMaxPollRPS: 1000`. Runner: GitHub-hosted standard `ubuntu-latest` (exact HW not captured here; see the run's own artifacts).

| Run | target_rate | conclusion |
|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/45592929283 | 24 | success |
| https://github.com/auenkr/cadence-rust/actions/runs/45592934891 | none | success |

Median of 5, History `peak_cgroup_anon_bytes` (MiB) and throughput (wf/s):

| Arm | rate=24 mem | rate=24 wf/s | unpaced mem | unpaced wf/s |
|---|---|---|---|---|
| go | 70.4 | 24.0 | 77.4 | 31.1 |
| control (rust) | 67.1 | 24.0 | 77.5 | 46.1 |
| rust | 67.5 | 24.0 | 75.8 | 49.5 |

A/A (control vs rust, same binary): 0.4 MiB at rate 24; 1.7 MiB unpaced.

Conclusion: at matched rate Rust History (67.5 MiB) is below Go (70.4 MiB); the earlier 76 vs 73 gap came from Rust running faster, not from a heap regression. Task 3 (heap investigation) not needed; no code change to History.
