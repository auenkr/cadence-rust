# Recovery driver timeout fix (2026-11-26)

Problem: see perf-frontend-kill-loss-20261126.md. The driver set the activity StartToClose (1 min) equal to the 60 s workflow timeout, so one retried activity could not finish (~1/80 lost in Go and Rust on kill_frontend).

Fix (tests/frontend-gate/recovery/main.go, head accdb23e2): workflow ExecutionStartToClose 60s -> 90s; activity ScheduleToStart 20s, ScheduleToClose 40s, StartToClose 10s, RetryPolicy{1s, coeff 1, max 5 attempts}; decision task timeout unchanged (10s); `-lost-dir` dump kept.

Runs (branch recovery-driver-timeouts, head accdb23e2, workflow recovery-timing.yml, mysql, scenarios kill_frontend,kill_matching,kill_history, repetitions=10; cassandra not run):
- Run 1: https://github.com/auenkr/cadence-rust/actions/runs/45748823682 (success)
- Run 2: https://github.com/auenkr/cadence-rust/actions/runs/45748828274 (success)

SHA256 (same in both runs unless noted):
- Rust cadence-server: e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725
- Rust cadence-history: d86114586c140074794f75d57f6ec28c23c1af4699a8cb63f7589dcff3badeb5
- Driver: 1d0ca352ec0bbb1d61427c06e3378722d81e1aa6f655af5ebcd16ac3f499108b
- Go cadence-server: run1 ce92716be12f91d68aaea2cf6ee4edd30fba84f5b52f5ef907bd39678dc38d9c; run2 244cbf401f21cf1db0f8d9e6497c4e5cf976c2d78b29546782459c1d952f9700 (differ between runs; Go image is rebuilt in each run)

## Lost workflows (the point of the fix)
| run | arm | started | completed | lost |
|---|---|---|---|---|
| 1 | go | 2480 | 2480 | 0 |
| 1 | rust | 2103 | 2103 | 0 |
| 2 | go | 2459 | 2459 | 0 |
| 2 | rust | 2053 | 2053 | 0 |

Total 9095 workflows, lost = 0 in every arm. Previously ~1/80 per kill_frontend scenario. No lost-dir dumps were needed.

## Stable time medians (ms, 10 reps each), Go vs Rust
| scenario | run1 go | run1 rust | run2 go | run2 rust |
|---|---|---|---|---|
| kill_frontend | 2810 | 1876 | 2617 | 1771 |
| kill_matching | 2456 | 2330 | 2576 | 2368 |
| kill_history | 1521 | 1233 | 1842 | 1445 |

"Not stable" = 0 in all rows. Rust is equal to or faster than Go at the median in every scenario/run. Full per-run tables are in each run's recovery/summary.md artifact.
