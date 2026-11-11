# History audit P0 fixes (branch history-auditp0, 2026-11-11)

Source: memory/history-parity-audit-20261111.md (P0 list).

## Done
1. Handler RPCs ported from Go: GetReplicationMessages (shard fan-out, sort by earliest creation time, MaxResponseSize cap), RemoveTask (CompleteHistoryTask by transfer/timer/replication category), RatelimitUpdate (new `ratelimit_aggregator.rs`, port of Go `RequestWeighted`; unit tests). Gap errors removed. Aggregator metrics emission is NOT ported.
2. CloseShard/DescribeQueue/RemoveTask/ResetQueue: already served by gRPC (`wiring/server/grpc.rs`) and TChannel; the audit claim was stale. No code change.
3. Replicate completed/failed/terminated/canceled/timed-out: close tasks (CloseExecution transfer + DeleteHistoryEvent timer) were already generated; tests added. Go `writeEventToCache` is perf-only (executor falls back to history reader), so absence is equivalent. Stale doc comments fixed.
4. GenerateDelayedDecisionTasks: already wired in `add_first_decision_task_scheduled`; tests added (backoff 0, None/Cron/Retry -> WorkflowBackoff with timeout_type 1/1/0 and timestamp, Decider -> error); stale doc fixed.

## CI (head 81cb0e8356fa3e3682a90f9c45d44ef3851c90f9)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43068443718 success
- history-mixed-gate mysql/grpc/false: https://github.com/auenkr/cadence-rust/actions/runs/43068485997 success
- history-mixed-gate mysql/tchannel/false: https://github.com/auenkr/cadence-rust/actions/runs/43068492227 success
- Earlier failures fixed: clippy useless_vec (43066359141), tests missing running state (43067046359).

## Remaining
- Ratelimit aggregator metrics; events cache write (perf only).
