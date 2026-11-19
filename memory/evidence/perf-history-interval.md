# perf-history-interval — UNVERIFIED / PAUSED (2026-11-19)

Goal: explain Rust History-lane p99 ~195 ms vs Go ~110 ms at 25/s (perf-history-tail.md).

## Status
No code change. Branch = master 56b58c6c1. Diagnostic logging lives only on
`tmp/diag-history-interval` (bd5fc9a4c, NOT for merge).

## Findings
- Audit vs Go found parity in all of these:
  - Timers, poll and ack intervals.
  - NotifyNewTask: tokio notify_one stores a permit, so no wakeup is lost.
  - Task retry policy (50/100 ms x3).
  - AddDecisionTask push, which has no retry.
  - Shard write gate held across the SQL write; Go holds s.Lock() the same way.
  - SQL pool idle default 2.
- Minor gap, not the cause: `allow_internal` in queue_deps_transfer_active.rs is a TODO that always returns true.
- Diag run 44424310326: the Go arm collapsed (completion p99 13.8 s), so the runner was overloaded and its slow-RPC data is invalid.
- Diag run 44428472400 (25/s, clean runner):
  - No slow persist (append + gate + update stayed under 30 ms total).
  - No slow write RPCs.
  - Completion p99: Go 190.7 ms, Rust 204.1 ms.
- Baseline 44435934016 (master 56b58c6c1, 25/s, 3 pairs), completion p99 ms:

| pair | Go | Rust |
|---|---:|---:|
| 00 | 212.2 | 194.8 |
| 01 | 2797.1 (Go collapsed) | 216.6 |
| 02 | 404.7 | 360.4 |

Conclusion so far: no fixed ~200 ms interval in Rust. Rust p99 is at or below Go in
every pair. The earlier "Go ~110 ms" looks like runner noise.

Runs 44435311006 and 44435327350 (dispatched on master) were cancelled. Another agent
started History benchmarks on master, and the workflow concurrency group
(ref + service) cancels earlier runs. Benchmark only on your own branch.

## Next steps (on resume)
1. Run 5/s 3 pairs on perf-history-interval. Optionally rerun 25/s, since pair 01 had the Go arm collapse.
2. If parity holds, close as "no gap"; there is no code change.
3. Run history-mixed-gate mysql/grpc and postgres/tchannel only if a code change lands.
4. Delete /mnt/storage/service/tmp/phi-4116588.
