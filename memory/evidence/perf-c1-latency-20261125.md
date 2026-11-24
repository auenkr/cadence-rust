# perf-c1-latency investigation (2026-11-25)

Branch `perf-c1-latency`, base master bd9f1ea65. No code change so far (no parity bug found by static review).

## Static review (Go comparison)
| Candidate | Rust | Go | Verdict |
|---|---|---|---|
| TCP nodelay, server sockets | tonic `Server::builder().tcp_nodelay(true)` (grpc.rs, grpc_internal.rs, grpc_admin.rs, grpc_tls.rs), `set_nodelay` on accepted streams (history grpc/tchannel, matching grpc_matching, tchannel.rs `connection`) | Go sets nodelay by default | OK |
| TCP nodelay, clients | tonic `Endpoint` default `tcp_nodelay=true` (history/frontend/matching clients via `endpoint_channel`/`connect_lazy`), tchannel_json_client sets nodelay, mysql crate 26 and mysql_async default `tcp_nodelay=true` | default | OK (only TLS connector `grpc_tls.rs:255` lacks it; not used in benchmark) |
| Transfer/timer queue poll | `max_poll_timer` 1m / 5m, notify_rx wakes immediately, `notify_new_transfer_tasks` called | `TransferProcessorMaxPollInterval` 1m, NotifyNewTask | OK (same) |
| Queue poll rate limiter | `max_poll_rps` 20, burst ceil(rps) | `NewDynamicRateLimiter` 20, burst ceil(rps) | OK |
| matching waiters `sleep(100ms)` | fallback wake in select with notification | n/a (safety) | not a fixed delay |
| sync match wait 200ms | `unwrap_or(now+200ms)` | `SyncMatchWaitDuration` 200ms | OK |

## Benchmark data (run 45461927639, c=1, 300 req, profile=1, all-Rust, head bd9f1ea65)
Go p50/p95/p99 43.5/51.6/56.9 ms; Rust 28.5/95.9/100.5 ms.
Rust latency is bimodal and quantized in ~40ms steps: 52/300 samples >60ms; slow samples
(~100ms) have `2_dec1_scheduled_to_started` ~41ms and `8_dec2_scheduled_to_started` ~41ms
(fast samples ~5ms); all other gaps equal fast ones. ~40ms is the Linux delayed-ACK interval, so
a Nagle/delayed-ACK interaction or a 40ms-ish wait in the decision dispatch hop
(history transfer -> matching AddDecisionTask -> poller -> RecordDecisionTaskStarted).
Profile (CPU) shows nothing: no hot spot.

## Bisect: one Rust service at a time, c=1, 300 req (same head bd9f1ea65, source tree unchanged)
| Run | Rust service | Go p50/p95/p99 (ms) | Rust p50/p95/p99 (ms) |
|---|---|---|---|
| [45473085366](https://github.com/auenkr/cadence-rust/actions/runs/45473085366) | frontend | 25.5/100.3/102.2 | 24.7/100.8/105.9 |
| [45473090401](https://github.com/auenkr/cadence-rust/actions/runs/45473090401) | matching | 35.0/101.0/117.0 | 33.8/98.5/106.7 |
| [45473096305](https://github.com/auenkr/cadence-rust/actions/runs/45473096305) | history | 41.7/49.6/102.7 | 29.6/97.6/103.7 |
| [45461927639](https://github.com/auenkr/cadence-rust/actions/runs/45461927639) | all three (profile=1) | 43.5/51.6/56.9 | 28.5/95.9/100.5 |

## Conclusion
- The ~40ms-quantized tail (scheduled->started gaps of ~40ms, i.e. delayed-ACK-sized) appears in the
  **all-Go arm** too: Go p95 is ~100ms in 2 of 4 runs and ~50ms in the other 2 (p99 ~102-117ms in 3 of 4).
  Slow Go samples in run 45473090401 show the same signature (`2_dec1_scheduled_to_started` 39-55ms,
  `8_dec2_scheduled_to_started` 37-42ms). So the tail is run/environment noise shared by both arms
  (Docker bridge + runner + Go SDK driver/worker side), not a Rust-only fixed timer.
- Rust p50 is consistently 5-15ms better than Go; p95 differences between arms flip between runs
  (Go 50 vs Rust 96 in two runs, Go 100 vs Rust 98-100 in the other two), so single-run c=1 p95 is not
  a reliable parity signal. Recommend multi-repetition (repetitions>=5) for tail claims.
- Static review found no parity bug (table above); CPU profile shows no hotspot. No source change made,
  so no rust-ci / history gate runs were needed.
- Pre-existing minor gap noted, not fixed (not in this path, TLS-only): `grpc_tls.rs` ClientTls::connect does
  not call `set_nodelay(true)` on the dialed TcpStream (Go's dialer sets TCP_NODELAY by default).

Head SHA at time of writing: branch perf-c1-latency (evidence commit only on top of bd9f1ea65).
