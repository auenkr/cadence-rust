# perf-history-c1mem (2026-11-25) — RESULT: NOT ACHIEVED

Change tested (commit 1f08fc17d): History jemalloc `narenas:2->1`, `dirty_decay_ms:250->100`. Nothing else.
Control = master 801390f4a. Candidate = 1f08fc17d.

Runs (all on branch perf-history-c1mem):
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/45544542886 (success)
- history-mixed-gate: https://github.com/auenkr/cadence-rust/actions/runs/45544547615 (success)
- frontend-matching-benchmark (rust_services=frontend,matching,history; c 1,16,64; 3 reps): https://github.com/auenkr/cadence-rust/actions/runs/45544552694 (success)

History peak_cgroup_anon (MiB, 3 reps mean):
| conc | Go | control | candidate |
|---|---|---|---|
| 1 | 70.9 | 76.1 | 78.8 |
| 16 | 95.6 | 91.1 | 90.4 |
| 64 | 110.1 | 98.6 | 100.6 |

Throughput candidate/control (median): c1 0.986, c16 0.999, c64 1.064; p50 latency ratio 0.989 / 0.999 / 0.901. No regression beyond noise.

Conclusion: narenas:1 / faster decay does NOT lower c1 peak anon (+2.7 MiB, within noise/slightly worse); c1 is still ~8 MiB above Go. Do not merge this change for the memory goal.
Untried: tokio worker thread count / thread_stack_size, blocking pool, startup preallocations, BENCH_MEM_REPORT / BENCH_HEAP_PROFILE attribution of the c1 baseline (next step: heap-profile c1 to see what dominates).
Runner: GitHub-hosted ubuntu-latest (standard 4 vCPU / 16 GB); containers capped 1.5 CPU per benchmark compose.
