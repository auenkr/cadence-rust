# Cassandra kill_history first-completion: regression or noise? (2026-11-26)

Question: run 45759281887 showed Go 58 ms vs Rust 1590 ms median first completion after killing History. Regression from fix-restart-lost-wf / fix-cassandra-parity3, or noise?

## Method
recovery-timing.yml database=cassandra scenarios=kill_history repetitions=5, three runs on master (aa1532864), plus one on branch perf-kill-history-first. First completion (ms) per rep, from results.json.

## Per-rep first completion (ms), reps 0-4
| run | Go | Rust |
|---|---|---|
| master 45765530651 | 37, 47, 29, 32, 1854 | 56, 49, 36, 49, 82 |
| master 45765534275 | 7, 1769, 53, 2062, 51 | 15, 1386, 15, 52, 1364 |
| master 45765537691 | 32, 52, 40, 14, 66 | 26, 60, 35, 50, 1486 |
| branch 45772626040 (8897ffd43) | 0, 6, 24, 46, 1393 | 47, 23, 1, 6, 10 |

Medians: master runs Go 37/53/40, Rust 49/52/50; branch Go 24, Rust 10.
Slow reps (>1 s): Go 5/20, Rust 4/20, in both arms, ~1.4-2.0 s. The distribution is bimodal in both arms (a rep either gets a completion within tens of ms because a task already queued/in flight, or waits for the shard re-acquire cycle of ~1.4-2 s). With n=5 the median flips when 3 reps land in the slow mode, which is what happened in 45759281887 (Rust 1590).

## Conclusion
Noise, not a regression: no systematic Rust-slower pattern across 20 reps per arm (Rust median 10-52 ms, Go 24-53 ms), the slow mode appears equally in Go. No startup-path change made. Stable-state medians (10 consecutive <2 s): Rust 1418-1897 ms vs Go 1602-2212 ms (Rust faster). Workflows lost: 0 in all runs.

## Code change: replication_dlq_ack_level
Go writes `ReplicationDLQAckLevel` (thrift field 46 in the shard blob) to the shard UDT. Rust's `ShardInfoRecord` lacked it so the UDT got null. Now `ShardInfoRecord.replication_dlq_ack_level` (field 46, read+write), `ShardInfoData.replication_dlq_ack_level`, carried in shard_events.rs both ways, written to the UDT in `build_udt`; unit test `udt_carries_all_go_fields_from_blob` extended.

## CI (head 8897ffd439fb7d2d1d2e8eba49c1c83b4223d501, branch perf-kill-history-first)
- rust-ci cadence-persistence: success, https://github.com/auenkr/cadence-rust/actions/runs/45765646660
- rust-ci cadence-history: success, https://github.com/auenkr/cadence-rust/actions/runs/45765650468
- rust-ci workspace: success, https://github.com/auenkr/cadence-rust/actions/runs/45765654098
- recovery-timing master x3: https://github.com/auenkr/cadence-rust/actions/runs/45765530651 , .../45765534275 , .../45765537691 (Rust cadence-server SHA256 7c22d05635b496a6dbfafd4ebf336b6539c411ddc703fe1a5222f668ade8ada9)
- recovery-timing branch: https://github.com/auenkr/cadence-rust/actions/runs/45772626040 success; Rust cadence-server SHA256 2effc2f63b0c1fd61fafaec2247f969ea468b5e2e290906a5d77bc059367933c; Go cadence-server bc985f56ea2258982f8ebe813e339f905293364baf5a13fc1686a4cf23229076. Branch run: go 5 reps, rust 5 reps, 0 lost.
