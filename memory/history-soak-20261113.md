# History soak gate (2026-11-13)

Branch `history-soak` (from master 4da929f43). Harness only, no Rust changes.

## What was added
- `.github/workflows/history-soak.yml`: dispatch inputs `database` (mysql/postgres/cassandra), `duration_minutes` (45), `two_history` (true), `rate` (4 wf/s). Job timeout 90 min, concurrency cancel-in-progress, artifacts `retention-days: 3` (logs tailed to 400 lines).
  - Also has a `push` trigger limited to branch `history-soak` and runs a 3-database matrix, because `gh workflow run` returns HTTP 404 until the workflow file exists on master. When merged to master, the push trigger and the matrix fallback can be removed (dispatch works).
- `tests/history-soak/` Go SDK driver (copy of frontend-soak): every workflow does timer, activity, child workflow, signal `wake`, continue-as-new, query `state`, signal `done`, result. Prints `STATS`/`FINAL` with p50/p95/p99 for e2e/start/query.
- `tests/gates/run-history-soak.py`: same cluster bring-up as `run-frontend-gate.py` (Rust History + Rust Matching + Rust Frontend, Go Worker). Mid-soak (`SOAK_RESTART_AT`, half of duration) SIGKILLs and restarts one Rust History host (`rust-history-2` if two_history, else the only host). Assertions (steps): `soak-zero-failed-workflows`, `soak-all-started-completed`, `soak-p99-recorded`, `<svc>-no-panic` for every service, `<host>-rss-stable` (median RSS of the last quarter vs first quarter after warm-up must be <= 1.5x or <= +150 MB), service-alive checks.
- Lane scripts `run-frontend-{mysql,postgres,cassandra}-lane.sh` accept `CADENCE_GATE_SCRIPT` to choose the harness (default unchanged).

## Run 1: https://github.com/auenkr/cadence-rust/actions/runs/43434980678
Head SHA `594bbe032419d52dbc578f76a3ce17e008ed2157`, 45 min, two_history=true, rate 4.
Binary SHA256 (all three jobs, identical): cadence-server `d2314ef61633f7990e6b279a578a7b977af06c2b8abdc624c0c1dd01ba12688d`, cadence-history `f2d23c80a7a871ed8725f33e7cb8c1ee379d6e3fcdde9791d769b3709ed4f173`.

| lane | job | conclusion | started / completed / failed | e2e p99 (completed only) | History RSS |
|---|---|---|---|---|---|
| mysql | https://github.com/auenkr/cadence-rust/actions/runs/43434980678/job/131917612771 | failure | 10800 / 12 / 10788 | 24306 ms | 218 -> 241 MB; host 2 233 -> 241 MB |
| postgres | .../job/131917613145 | failure | 5676 / 12 / 10788 (counter anomaly: failed > started, see below) | 24285 ms | 166 -> 301 MB; host 2 249 MB |
| cassandra | .../job/131917613226 | failure | 5632 / 11 / 10789 | 24427 ms | 240 -> 327 MB; host 2 261 MB |

Passing: all services alive, no panics in any log, restart happened (t=1350 s, listening again within ~10 s), p99 recorded. Failing: `soak-driver-exit`, `soak-zero-failed-workflows`, `soak-all-started-completed` in all three lanes. The RSS check passed for mysql and passed by the +150 MB rule for the others, but this is not meaningful while tasks are stuck.

### Root cause: Rust bug, identical on all three databases (not a harness problem)
Only the first ~12 workflows complete. After that, workflows sit in `state=="init"` forever (the `wake` signal is never processed, the driver gives up with `query state "init"` after 90 s of retries; 10 783 of the FAIL lines are stage `query`). Rust History logs the same task failure endlessly, with `attempt-count` growing:

```
ERROR cadence_history::task::deps: Fail to process task error=internal service error: invalid history action DecisionTaskScheduled (pending decision) (next event id 9) lifecycle=ProcessingFailed attempt-count=11
```

`next event id 9` = history 1..8: Started, DT scheduled/started/completed, TimerStarted (2 s sleep), TimerFired, DT scheduled, DT started. So a timer/transfer task tries to schedule a decision while one is already pending/started, and the task is retried forever instead of being a no-op. Suspected files:
- `rust/crates/cadence-history/src/wiring/assembly/queue_deps_timer_active.rs:193` `schedule_decision` calls `add_decision_task_scheduled_event(false)` unconditionally after the user-timer fired. Go only schedules when `!HasPendingDecision()` (compare Go `timer_active_task_executor` / `updateWorkflowExecution`).
- Erroring site: `rust/crates/cadence-history/src/wiring/assembly/engine_decision.rs:109` (`add_decision_task_scheduled`).
Other timer-executor callers of `schedule_decision` (activity timeout, decision timeout, workflow backoff) should be audited for the same missing guard.

Other observations: driver also saw `QueryFailedError: workflow must handle at least one decision task before it can be queried` (expected race, retried) and 27 `unhandled` lines. The postgres/cassandra `started` is lower than mysql because the driver stalls waiting on retries; the `failed` figure is the driver's per-lane counter and `failed > started` is a driver-side artifact from the shared FINAL snapshot in a failed drain (cosmetic, not investigated).

## Next steps
1. Assign the Rust fix above (needs the pending-decision guard in the timer executor path). Do not fix in the harness.
2. Re-run: `gh workflow run history-soak.yml -R auenkr/cadence-rust --ref <branch> -f database=<db> -f duration_minutes=45` (needs the workflow on master first) or push to `history-soak` to trigger the 3-db matrix.
3. Harness TODO if wanted: a short smoke mode (`duration_minutes=5`) is available through the same input and was not run.
