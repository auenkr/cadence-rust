# History queue v2 transfer load counts (2026-11-25)

Base `f3479cb2b8c47b581fc184b30dae29b7045d366b`, with Go submodule
`bb4833c865311f2e22eb6dfe73cad52b53dbaf4b`. The branch is
`perf-queuev2-loads`.

## Source audit

- Go and Rust cached queues inject one batch before forwarding its notification.
  The immediate queue coalesces notifications in a one-slot channel in both
  implementations. It creates a new slice only if the exclusive shard read
  level splits the remaining range. Fallback polling uses the same configured
  interval, jitter and backoff comparison.
- Both managers notify only the root queue when appending or merging a new
  transfer slice. They share a single limiter across the virtual queues of each
  shard/category. Each virtual queue coalesces load notifications in a one-slot
  channel. Both reserve the permit before checking whether a slice is available
  or the queue is paused.
- Both slice readers follow persistence or cache progress up to the page size or
  end of range. Both virtual queue loops re-notify if the slice still has progress,
  or if another slice follows it. In particular, moving to the next slice
  without testing its progress is copied from Go's linked-list traversal.
- In the running Rust queue v2 implementation, `V2RateLimiter` uses the
  History `wiring/assembly/queue_exec.rs` bucket. Its 20 rps steady-state rate
  and burst of 20 match Go's default. Rust reads dynamic config on every
  reservation. Go's `common/quotas/dynamicratelimiter.go` refreshes at most
  every five seconds. The configured rate is constant in this benchmark.
- Go reads the immediate max-read-level under a shard read lock. Rust waits
  for a current writer via the task-write gate, then snapshots the level.
  The two steps use separate locks in Rust. No deterministic extra-notify
  path was found in the audited source.

## Prior c1 trace

The [previous History-only trace](https://github.com/auenkr/cadence-rust/actions/runs/45549320491)
contains 6,000 decision transfer task reads for 3,000 measured Rust workflows.
Joining both Rust stages by workflow, run and task ID yields 1,045 decision
task waits above 5 ms (0.348 per workflow) and 892 above 30 ms (0.297 per
workflow). The corresponding Rust `c1_queue_rate_limiter_wait` stage
contains 2,186 records across all transfer task types (0.729 per workflow).
A batch can contribute multiple records, and empty attempts contribute zero.
Go did not emit per-permit traces in that run.

## New trace and validation

| Run | Head | Configuration |
|---|---|---|
| [Paired c1 trace](https://github.com/auenkr/cadence-rust/actions/runs/45556131815) | `127b63a153cce98a1632f3628867cb2ec9f47bf5` | 3 pairs, all Rust services except Worker, trace enabled |
| [Untraced benchmark](https://github.com/auenkr/cadence-rust/actions/runs/45555552504) | `d44b307437d484fbe469925a4947316a6050e8eb` | Go, master Rust control, branch Rust, c1/c16/c64, 3 repetitions |
| [History CI](https://github.com/auenkr/cadence-rust/actions/runs/45555667273) | `7fb1fc575ad9eee97251e91b6e2e9ac15e04230e` | Fresh target, includes one-notification test |
| [Queue v2 History recovery gate](https://github.com/auenkr/cadence-rust/actions/runs/45555465722) | `d44b307437d484fbe469925a4947316a6050e8eb` | Fresh target, MySQL, gRPC, queue v2, two History hosts |

The benchmark applies the diagnostic Go patch only when `trace_decision=true`.
The Rust diagnostic adds one record per permit attempt, independent of how
many tasks the read returns. The first trace dispatch was superseded after
identifying that the benchmark's default Rust log filter discarded queue v2
logger output.

For permit counts in the trace artifact, use `services-*-{go,rust}.log` and
filter to `DTRACE queuev2 load` in the transfer v2 component. Restrict timestamps
to `measured_start_utc` through `measured_end_utc` from the corresponding
`result-*-c1.json`. Normalize by successful workflows. For decision waits,
join `result-*-c1-samples.json` on workflow and run ID, using Go task type 0
and Rust `c0_transfer_task_read`. Each task identity is workflow, run and task
ID. Count a delayed task if the joined wait duration exceeds the given threshold.

## Result of paired c1 trace (run 45556131815, 3 pairs, 1000 workflows each)

Permits: the earlier "per workflow" normalisation was wrong. Each pair measures
1,000 workflows, so Go uses ~2.98-2.99 transfer-load permits per workflow and
Rust ~2.93-2.94. **Rust does not consume more permits than Go.**

| Pair | Go permits | Rust permits | Go wait sum ms | Rust wait sum ms | Go >30 ms | Rust >30 ms |
|---|---|---|---|---|---|---|
| 0 | 2984 | 2930 | 10390 | 26574 | 214 | 544 |
| 1 | 2986 | 2935 | 11588 | 27740 | 244 | 577 |
| 2 | 2988 | 2928 | 9051 | 25764 | 195 | 530 |

Go's waits cap near 50 ms (1/20 s) and are concentrated on two of four shards
(shards 0 and 2: 100 and 93 waits >30 ms; shards 1 and 3: 15 and 6). Total
permit rate is ~72/s over 4 shards, near the 20 rps per-shard limit, so the
limiter saturates on hot shards. Rust's limiter math (`queue_exec.rs`
`DynamicRateLimiter::reserve`) matches x/time/rate. The Rust trace line had no
shard id, so the per-shard split could not be compared. The open question is
whether Rust has the same shard imbalance or spreads waits across all shards.
Next: trace line now carries `limiter=<ptr>` (one limiter per shard/category).

## Per-shard split (traced run 45567552339, head bd77bcc80, 3 pairs, c1)

Per shard (permits, wait sum ms, waits >30 ms), transfer queue only:

| Pair | Go (4 shards) | Rust (4 limiters) |
|---|---|---|
| 0 | (765,2,0) (801,823,20) (723,2,0) (711,2,0) | (715,4815,96) (733,4177,87) (742,3148,65) (770,9148,191) |
| 1 | (723,26,0) (729,739,14) (792,585,13) (755,312,6) | (711,7143,151) (713,4628,94) (808,7694,155) (735,3125,63) |
| 2 | (812,3876,82) (699,1318,25) (674,1974,40) (800,7195,149) | (728,3198,65) (715,2131,45) (763,10919,224) (756,4495,94) |

Conclusion: permits per shard are equal (~700-800 per 40 s, ~18/s against a 20 rps
limit). Rust's waits hit all four shards in every pair; Go's are lower and vary
by run (one pair is nearly idle). So the limiter math and permit count are at
parity, and the extra Rust wait comes from burstier permit arrival times (more
back-to-back loads inside a second), not from extra permits. No code change was
made; no Go-parity divergence found in the load path. Not yet checked: Go's
read batch/page size per load, and permit inter-arrival timestamps (the traces
contain them; compute per-second max permits per shard next).
Untraced run 45555552504, >60 ms at c1: Go 2.2-3.3%, master Rust 20.5-23%, branch
Rust 19.4-24.6% (this branch is trace-only, no behaviour change).
Gate/CI runs for this head: rust-ci 45567557600 success. History gate and the
c1/c16/c64 re-benchmark were not re-run because no behaviour changed.
