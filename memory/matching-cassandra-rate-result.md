# Cassandra lane: DescribeTaskList backing rate (gap-list item 2)

Date: 2026-10-25. Branch t3/mw-cassandra-rate (base t3/matching-integration-20261025). Source-only analysis; no cargo run, no gate dispatched.

## Verdict
No Rust divergence found. No code change. The `100000 !== 1` failure is historical (final2/final3 gates, 2026-10-23/24, `memory/matching-open-gaps-20261024-v2.md:164`) and is not reproduced on newer code.

## Evidence
- Harness assertion: `tests/matching-service-compat.cjs:1290` (`pacedDescribe.taskListStatus.ratePerSecond === 1`) after three activity polls carrying `taskListMetadata.maxTasksPerSecond=1`.
- Go: `task_list_manager.go:getTask` (l.702-722) calls `limiter.ReportLimit(rps)`; Describe reports `float64(c.limiter.Limit())` (l.794). `task_list_limiter.go` `ReportLimit`/`tryUpdate` takes the lower value (rps < current) and divides by partition count.
- Rust: `grpc_matching.rs:poll_rate` (l.1190-1240) mirrors this (override rps, else worker rate, `limiter.report_limit`), and Describe reports `queue.limiter.limit()` (l.3754). `matching_limiter.rs` has unit tests for the lower-wins, TTL and partition-divisor rules. The limiter file has not changed since the failing runs.
- 100000 is exactly `TASK_DISPATCH_RPS`, the default unreported limit. That value in Describe means the Describe was answered by an owner whose limiter never saw the poll (fresh default limiter, i.e. an ownership split between the two Rust members or a ring move between poll and describe), not a wrong rate computation. The default is a per-owner in-memory limiter in both Go and Rust.
- Harness already has owner-agreement checks before the poll, before describe and on failure (`paced-owner.json`, `tests/matching-service-compat.cjs:1215-1289`), added after the failing runs.
- Cassandra mixed gate later passed: run 40153493941 (https://github.com/auenkr/cadence-rust/actions/runs/40153493941), head 27d3ff5f6, branch matching-final-candidate-20261024, success. Latest Cassandra push run 40174866866 was cancelled (superseded), not failed.

## Remaining caveat
No Go-control run of this exact paced Describe exists (Go owner would behave the same way under a ring split). If the failure recurs, read `paced-owner.json` from the lane artifact: disagreement or a changed owner means ring-convergence timing, otherwise a real limiter bug. Fresh Cassandra lane evidence on the final binary is still the open item H (needs the gate, not dispatched here).
