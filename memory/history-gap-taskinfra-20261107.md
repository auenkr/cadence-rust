# history-gap-taskinfra audit (2026-11-07)

Branch: history-gap-taskinfra. Rust under rust/crates/cadence-history/src/task/.

| Go file | Rust | Result |
|---|---|---|
| fetcher.go | n/a | Does not exist in history/task (only common/reconciliation/fetcher); out of scope |
| processor.go | processor.rs | Matches (weights, nice map, weighted keys, lifecycle). Scheduler construction injected (known TODO) |
| priority_assigner.go | priority_assigner.rs | Matches |
| rate_limited_processor.go | rate_limited_processor.rs | Matches |
| task_rate_limiter.go | task_rate_limiter.rs | Matches |
| redispatcher.go | redispatcher.rs | Matches |
| rescheduler.go | rescheduler.rs | Matches |
| event_logger.go, constants.go | event_logger.rs, constants.rs | Match |
| task.go | history_task.rs | 2 fixes (below) |
| interface.go, standby_task_util.go, task_util.go | - | Not audited here (time; task_util audited in earlier commit) |

## Fixes in history_task.rs
1. `Execute`: filter error now sets should_process_task=false (Go assigns the bool before checking err).
2. `HandleErr`: ErrTaskDiscarded / ErrMissingVersionHistories set err=nil in Go then fall through to the
   TaskFailuresPerDomain counter and the "Fail to process task" log before returning nil. Rust returned early.
   Now emits both (log has no error tag, as Go's err is nil).

CI: see final report (run URL / head SHA recorded in commit message follow-up).
