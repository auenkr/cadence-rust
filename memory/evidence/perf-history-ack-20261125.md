# perf-history-ack (2026-11-25)
Branch perf-history-ack. Found write-write patterns (16-byte tchannel header and payload as two write_all calls) in:
history server `write_frame` (wiring/server/tchannel.rs), service-core `tchannel_json_client.rs` write_frame (History->Matching client), matching `tchannel.rs` write_frame.
Fix: coalesce header+payload into one buffer/one write_all (no wire change). Other sockets already set nodelay (see perf-c1-latency evidence).

CI: rust-ci cadence-history run 45481491205 success (a273204e3); cadence-service-core 45484789996 and cadence-matching 45484796824 success (972ef243e). History mixed gate not run.

Benchmark (rust_services=history, c=1, 5 reps, median):
| Run | head | Go p50/p95/p99 | Rust p50/p95/p99 |
|---|---|---|---|
| 45481531809 | a273204e3 (history only) | 46.5/56.7/70.3 | 32.3/99.7/114.0 |
| 45484802109 | 972ef243e (+client, matching) | 31.9/100.3/149.2 | 23.7/106.3/188.2 |

Result: done condition NOT met. Run 2 Go itself had p95 100ms, so the ~40ms tail is environment noise present in both arms (consistent with perf-c1-latency). The coalescing is a harmless correctness/efficiency fix but did not remove the Rust tail in run 1. Remaining suspects not examined: Go-side/SDK delayed-ACK interplay, MySQL sync writes in History.
