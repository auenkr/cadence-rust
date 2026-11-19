# perf-history-interval — CLOSED: no gap (2026-11-19)

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

## Resume results (branch perf-history-interval = master 56b58c6c1, completion p99 ms, 3 pairs)

| rate | run | pair 00 Go/Rust | 01 Go/Rust | 02 Go/Rust |
|---|---|---|---|---|
| 25/s | [44435934016](https://github.com/auenkr/cadence-rust/actions/runs/44435934016) | 212 / 195 | 2797 (Go collapsed) / 217 | 405 / 360 |
| 25/s | [44472845239](https://github.com/auenkr/cadence-rust/actions/runs/44472845239) | 191 / 203 | 175 / 215 | 193 / 212 |
| 5/s | [44439914369](https://github.com/auenkr/cadence-rust/actions/runs/44439914369) (noisy, both arms) | 108 / 255 | 483 / 462 | 318 / 425 |
| 5/s | [44468013476](https://github.com/auenkr/cadence-rust/actions/runs/44468013476) | 73 / 60 | 79 / 74 | 74 / 61 |

## Conclusion: no gap
Go's p99 at 25/s is 175-212 ms on clean runners, not ~110 ms. Rust stays inside the
same band (195-217 ms). In the clean 25/s repeat it is about 10-40 ms higher; in the first
batch it is lower. At 5/s the clean batch has Rust at or below Go. No fixed ~200 ms
interval exists. The audit found no timer, wakeup or retry deviation from Go.
No code change, so no rust-ci or history-mixed-gate run is required.
The diagnostic branch `tmp/diag-history-interval` (bd5fc9a4c) is NOT for merge.
