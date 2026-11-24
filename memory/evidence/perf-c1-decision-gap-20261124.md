# c1 DecisionTaskScheduled -> Started gap (2026-11-24)

Branch `perf-c1-decision-gap`. Code head 3bf9f3598a7f92ce0b0f744f4f688a6a32c3ab75 (one change, see below). Status: root cause NOT confirmed as Rust-specific; the tail is not removed.

## What was ruled out (code review, Go vs Rust)
- Matching: native dispatch wait loop (`matching_dispatch_native.rs` ~1283-1395) enables the `wake` Notify before claiming, with a 100 ms fallback, which is the same shape as Go's matcher. No Rust-only sleep or ticker found.
- TCP_NODELAY: SDK-facing TChannel (`tchannel.rs:681`), outbound tonic clients and the tchannel JSON client already set it. The tonic servers did not (tonic `Server::builder()` defaults to `tcp_nodelay=false`; grpc-go always sets it). Fixed in `grpc.rs`, `grpc_internal.rs`, `grpc_admin.rs`, `cadence-service-core/src/grpc_tls.rs`. Result: no change in the c1 tail (p95 99.8 ms before and after), so Nagle/delayed-ACK is not the cause.
- SDK pollers: `worker_pollers=4` (run 45342136980) vs default (run 45342132661): Rust c1 p95 still about 99.5 ms in both. Not a poller-count effect.
- CPU throttling, slow SQL: ruled out in perf-c1-tail-diagnosis-20261124.md.

## New finding: the slow workflows cluster by History shard
Slow share (latency > 55 ms) per `farmhash.Fingerprint32(workflow_id) % 4` shard, from `perf-c1-tail-data/result-0N-{rust,go}-c1-samples.json` (run 45337477057):
```
rust rep0 {0:39/242 1:64/250 2:85/260 3:67/248}   go rep0 {0:38/232 1:37/238 2:54/260 3:80/270}
rust rep1 {0:104/259 1:49/236 2:60/249 3:80/256}  go rep1 {0:13/240 1:30/235 2:46/247 3:139/278}
rust rep2 {0:35/238 1:106/255 2:52/249 3:90/258}  go rep2 {0:65/247 1:50/252 2:71/259 3:29/242}
```
The share varies strongly by shard and the slow shard changes between reps, in BOTH Go and Rust. Slow events are independent of the previous workflow (P(slow | prev slow) 0.24, P(slow | prev fast) 0.26 for Rust). Both decision gaps of a workflow are slow together. This points to a per-shard History transfer-queue timing effect (notify vs read pass, `MaxPollRPS` 20 limiter, i.e. 50 ms spacing, in Go `queue/processor_base.go:119`), which Go has too (Go 21.7% vs Rust 27.7% slow). It is not a Matching dispatch delay. Not proven: that needs History-side timestamps (transfer task created, read, dispatched to matching). A trace for that was not added in this branch.

## Verification of the nodelay change (head 3bf9f35)
- rust-ci cadence-matching: run 45348289810 success. rust-ci cadence-history: run 45348295009 success.
- Matching mixed gate (mysql default): run 45348316352 success (job `gate` success; the log has no literal `GATE SUMMARY` line, so I did not see `failed: []`).
- Frontend mixed gate (mysql): run 45348320651 success, log line `GATE SUMMARY failed: []`.
- Rust binary sha256 (benchmark run): cadence-server 5c1f45864ba836ff6b2003696612b5ac642ef3e579e0fb6e4e8e9bb015f1aeba.

## Paired benchmark, medians of reps (p50 / p95 / p99 ms), rust_services=frontend,matching,history
| conc | arm | before 45291978847 (5 reps) | after 45348300464 (3 reps, nodelay) |
|---|---|---|---|
| c1 | rust | 29.2 / 99.7 / 107.1 | 30.8 / 99.8 / 109.5 |
| c1 | go | 42.6 / 53.0 / 75.0 | 45.9 / 55.4 / 69.8 |
| c16 | rust | 265.9 / 354.4 / 395.0 | 272.8 / 378.9 / 419.6 |
| c16 | go | 386.7 / 509.8 / 563.9 | 414.5 / 579.7 / 663.2 |
| c64 | rust | 1281.3 / 1600.3 / 1719.1 | 1083.0 / 1669.6 / 1793.2 |
| c64 | go | 3484.5 / 4592.5 / 5588.5 | 3565.0 / 4593.4 / 5920.9 |

Other c1 runs: 45337477057 (diagnosis): Rust 18.8 / 100.3 / 107.5, Go 24.9 / 100.3 / 103.3. 45342132661 (default pollers): Rust 25.7 / 99.9 / 101.6. 45342136980 (worker_pollers=4): Rust about 28.5 / 99.5 / 105. Differences between before and after are within run-to-run noise.

## Conclusion
No fix for the c1 tail was found. The nodelay change is kept because it matches Go (grpc-go enables it) and passed the gates, but it does not change the numbers. Next step: add env-gated History transfer-processor timestamps per shard (task create, read, AddDecisionTask sent) in Go and Rust to see why one shard stays "slow" for a whole rep.
