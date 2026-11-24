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

## Bisect (in progress)
Runs with a single Rust service, c=1: frontend 45473085366, matching 45473090401, history 45473096305.
