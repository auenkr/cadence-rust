# fix-lb-feedback (2026-11-19)

Branch `fix-lb-feedback` (from master 95dd68ed7). Code head `f8c84f8dab0c1725b35f3f37aa65fd6535fd9525`
(gates ran on 1675575e9 = same code before two rustfmt-only commits).

## Fixes (checked against Go go/client/matching/client.go)
1. HIGH, FE poll feedback (client.go:158-170, 199-211): `poll_decision`/`poll_activity` now call
   `provider().update_partition_config(domain, root tl, kind, tt, cfg)` and `lb.update_weight(tt, root req,
   polled partition, hints)` after a successful poll (`workflow_matching.rs::poll_feedback`). Note: the FE→Matching
   transport is Thrift, and Matching's Thrift PollFor*TaskResponse has no partitionConfig/loadBalancerHints
   fields (go/idls/thrift/matching.thrift), so both are nil. Go over TChannel behaves the same way, so the calls are no-ops today.
   They will take effect if FE moves to gRPC.
2. MEDIUM, History QueryWorkflow (client.go:240): `query_matching.rs` now applies `resp.partition_config`
   (proto api.v1 TaskListPartitionConfig → `partition_config_from_api`) to the provider for the root task list.
3. MEDIUM, poll tracking: `poll_partitions` is now `pollerID -> (generation, partition)`. `track_poll` returns
   a RAII `PollGuard` that removes the entry on drop only if the generation is still its own. Overlapping polls no longer
   clobber each other, and a dropped future no longer leaks its entry. CancelOutstandingPoll still routes to the last
   partition polled by that pollerID. Go has no map here (it rewrites the request in place), so this is the closest match.
4. LOW, `BoundedMap` in `matching_lb.rs` is now a true LRU (cap 3000, get refreshes, evicts the least recently used), as Go `cache.New` LRU does.

Tests: `bounded_map_evicts_least_recently_used`, `partition_config_from_api_maps_groups_and_version`,
`overlapping_polls_keep_newest_entry`, `dropped_poll_future_releases_entry`, `empty_poller_is_not_tracked`,
`poll_feedback_updates_root_partition_config`.

## CI (head f8c84f8)
- rust-ci cadence-common https://github.com/auenkr/cadence-rust/actions/runs/44435379093 success
- rust-ci cadence-frontend https://github.com/auenkr/cadence-rust/actions/runs/44435383357 success
- rust-ci cadence-history https://github.com/auenkr/cadence-rust/actions/runs/44435387396 success

## Gates (head 1675575e9, code-identical)
- matching-mixed-gate https://github.com/auenkr/cadence-rust/actions/runs/44428165208 success
- frontend-mixed-gate mysql https://github.com/auenkr/cadence-rust/actions/runs/44428171606 success, `GATE SUMMARY failed: []`
- history-mixed-gate mysql/grpc https://github.com/auenkr/cadence-rust/actions/runs/44428176525 success, `GATE SUMMARY failed: []`

## Benchmark: 4 read/write partitions, service=all, 16 pollers, 3 alternating pairs (head f8c84f8)
Runs: 30/s https://github.com/auenkr/cadence-rust/actions/runs/44435451141 ·
35/s https://github.com/auenkr/cadence-rust/actions/runs/44435461697 (temp branches fixlb-parts-30/35)

| rate | Go cps / p99 | Rust cps / p99 | Rust outstanding |
|---|---|---|---|
| 30 | 29.9-30.0 / 137-154ms | 29.9-30.0 / 217, 222, 645ms | 4, 2, 1 |
| 35 | 34.9 / 150-178ms | 34.8-34.9 / 215, 215, 245ms | 5, 6, 3 |

All 12 runs are within the pass criteria (p99 <= 1000ms, outstanding <= 9/10). Every stage exit_code is 0.
Go now also passes at 35/s (in the earlier run it failed 0/3). Rust is unchanged compared with perf-ceiling-partitions, except for one 645ms p99 outlier at 30/s (the known History tail).
To dispatch, the benchmark needs `-f sdk_openloop=true` (without it, only the paired-benchmark job runs).
