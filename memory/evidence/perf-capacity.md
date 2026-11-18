# All-Rust vs all-Go capacity (2026-11-18)

Source under test: 36e8a1e3175ad76ed77bc9ec9bddadf3241a4d78 (master). Harness branch: perf-capacity-harness (c0cfec9da8539a70ff25c01a366639e493e246bd),
based on t3/matching-benchmark-harness; adds `capacity_rates`, `capacity_only`, `sdk_repetitions` for capacity,
`worker_pollers` (driver `-worker-pollers`) and per-arm SDK worker process CPU (`*-sdkworker.json`).
Same protocol as reports/jemalloc-master-20261115.md: one-activity, 1 KiB, 10ms activity, non-sticky, 30s measure,
services 1 CPU/1400MiB, MySQL 1.5 CPU; 2 fresh-DB alternating pairs per step. Pass = zero errors, p99 <=1000ms, <=max(2,1%) outstanding.

## Runs
| Run | Rates | Pollers | Conclusion |
|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/44321592381 | 40,60,80 | SDK default (2) | success (all steps miss target) |
| https://github.com/auenkr/cadence-rust/actions/runs/44335243991 | 25,40,60 | 16 | success |
| https://github.com/auenkr/cadence-rust/actions/runs/44347203701 | 30,35,40 | 16 | success |

## Max passing tested rate (starts/s)
| Config | before (report 20261115) | this study | completions/s at 40 offered (16 pollers, 4 arms) |
|---|---:|---:|---|
| All Go | 25 | 25 (30 fails) | 17.4, 17.0, 15.1, 21.2 |
| All Rust (Go Worker) | 25 | 25 (30 fails) | 24.5, 24.8, 21.0, 30.5 |

No Rust code was changed, so there is no "after" Rust number. Both systems pass 25/s and fail at 30/s. At 30/s Rust
came close in one arm: 29.6/s with 13 outstanding and p99 1765ms. At 30/s one Go arm collapsed to 1.7/s.
Above 30/s Rust degrades more gracefully: higher completions, and at 60/s Rust has 0 errors vs ~1,690 timeouts for Go.

## Which resource saturates: none measured
At 30–40/s, average CPU cores used (cap in parentheses):
- Rust: FE 0.12–0.15 (1.0), History 0.24–0.31 (1.0), Matching ~0.10 (1.0), MySQL 0.41–0.55 (1.5).
- Go: FE 0.12–0.20, History 0.31–0.75, Matching 0.06–0.10, MySQL 0.43–0.71.
- SDK worker process: 0.02–0.05 cores (own CPU). The generator was valid (lag ok) in every arm.
- The backlog is in **decision** tasks: decision schedule-to-start p99 is 1–25s, while activity schedule-to-start stays <200ms.
  Both implementations show it, with large run-to-run variance (Go 30/s: 27.5 vs 1.7 completions/s).

Conclusion: at this cluster configuration the limit is not service CPU, MySQL CPU, the generator or the SDK worker.
The ceiling is environmental or configuration-wide and hits Go and Rust at the same rate. Rust is not the earlier saturator,
so there was no Go-parity Rust fix to make (step 2 not triggered). The 2-poller SDK default in the harness was a
real limit for Rust at 40/s: 16–19 → ~24.6 completions/s with 16 pollers. Go did not improve with 16 pollers.

## Proposed harness fixes / next diagnostics
1. Default `worker_pollers=16` for capacity steps; the 2-poller default under-feeds non-sticky decisions.
2. Suspects for the shared ceiling, not yet measured:
   - MySQL commit/fsync latency on the runner disk: ~0.5 GB written per arm, CPU idle.
   - The decision task list backlog path: a single partition, and the DB backlog read once sync-match misses.
   - History's 4 shards (development.yaml numHistoryShards).
   Add MySQL performance_schema wait-by-event and InnoDB log-fsync timings, Matching backlog counters, and a variant
   with MySQL datadir on tmpfs or innodb_flush_log_at_trx_commit=2. That variant separates disk from service logic.
3. Use 3+ pairs at 30/35/40; the 2-pair variance at the knee is large.

Caveat: the SDK worker CPU is from /proc utime+stime over generate (warmup+measure+drain). It is low enough that
this is not the limiting factor even if it underestimates.
