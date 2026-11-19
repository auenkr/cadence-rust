# fix-restart-gate (2026-11-19)

## Issue 2: benchmark per-table SELECT counts
Cause: both Go and Rust MySQL drivers use server-side prepared statements (COM_STMT_EXECUTE), which are not
aggregated in performance_schema.events_statements_summary_by_digest. The "13 transfer_tasks/timer_tasks SELECTs"
were the collector's own `COUNT(*) ... GROUP BY shard_id` probes (t=0 + every 5 s over 60 s).
Fix: Benchmark/collect_resources.py `sql_log_stop` now aggregates SELECTs per table from mysql.general_log inside
MySQL (REGEXP_SUBSTR on the FROM clause) -> `mysql_general_log.select_by_table`. Needs `sql_log_secs` = window.

All-services 25/s, 60 s, mysql_tmpfs=true, sql_log_secs=60 (Execute = service statements, per 60 s window):

| run | source | arm | transfer_tasks SELECT | timer_tasks SELECT |
|---|---|---|---|---|
| https://github.com/auenkr/cadence-rust/actions/runs/44521139394 | 0e5c84050 (Rust queue v1) | Go (v2) | 0 | 1 |
| same | | Rust v1 | 4444 | 3346 |
| https://github.com/auenkr/cadence-rust/actions/runs/44521133755 | 9d656c446 (Rust queue v2) | Go (v2) | 0 | 1 |
| same | | Rust v2 | 0 | 2 |

Verdict: queue v2 removes ~7.8k queue SELECTs/min (~130/s) in Rust History; now at Go parity.

## Issue 1: restart gate
Symptom (master run 44513215209): 337/4500 workflows fail `query state "init"`; dumps show TimerStarted (2 s) with
no TimerFired after 90 s, from the start of the run (before faults). "no matching hosts in the membership ring"
lines are only the deliberate s4 Matching kill window (11:37) — not the cause.
Regression bisect (restart gate mysql, 600 s):

| commit | run | result |
|---|---|---|
| 469779f95 | https://github.com/auenkr/cadence-rust/actions/runs/44520365320 | pass 1800/1800 |
| 451146825 | https://github.com/auenkr/cadence-rust/actions/runs/44520377220 | pass 1800/1800 |
| 56b58c6c1 | https://github.com/auenkr/cadence-rust/actions/runs/44520388503 | FAIL 15 |
| bc073c19b | https://github.com/auenkr/cadence-rust/actions/runs/44520399591 | FAIL 27 |
| 6c7ce1f5a | https://github.com/auenkr/cadence-rust/actions/runs/44520409690 | FAIL 107 |
| e480361a0 | https://github.com/auenkr/cadence-rust/actions/runs/44526321883 | pending |
| 35d5054de | https://github.com/auenkr/cadence-rust/actions/runs/44526309933 | pending |

## Update 2026-11-19: root cause 1 fixed; Issue 1 still partly open
Remaining bisect runs (restart gate, MySQL): d1dba9370 pass (44531059100), f8eccb140 pass (44531070180), 2bd2080d3 pass (44531083142).

Root cause 1 (35d5054de): events and timer expiry now carry full nanoseconds, but timer tasks fire at their visibility time truncated to ms. The Rust active executor compared `now.before(expiry)` at exact nanoseconds, so `now < expiry` by less than 1 ms. It broke out of the loop, and the user or activity timer never fired. Go uses `timerSequence.IsExpired`, which compares Unix() seconds.
Fix: timer_active_task_executor.rs compares seconds (user and activity loops, and the resurrection delay); timer_standby_task_executor.rs does the same (Go-parity IsExpired).

Validation:
- rust-ci cadence-history: 44531343722 success, 44538972815 success (after the standby fix)
- history-mixed-gate MySQL: 44531350818 success, `GATE SUMMARY failed: []`
- restart gate, fix applied to 56b58c6c1 (branch fix-restart-gate-bisect-56b58-fix): 44538960577 **PASS** (without the fix: 15 failures)
- restart gate, fix-restart-gate (master + fix): 44531357568 FAIL 54, 44538966210 FAIL 96 (master without the fix: 337)

Remaining: a second regression after 56b58c6c1 (bc073c19b 27, 6c7ce1f5a 107 before the fix). Its failure modes are query "init", "must handle at least one decision task" and History TChannel timeouts, which point to stuck decision or transfer dispatch. Next step: bisect 56b58c6c1..master with this fix cherry-picked.
