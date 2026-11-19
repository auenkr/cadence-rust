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

## Update 2026-11-19 (2): bisect with timer fix cherry-picked (branch fix-restart-regression)
Restart gate MySQL 600 s, each point = merge + e8d808562 + f611c1a85 (branches restart-bisect-<sha>), 2 runs each:

| merge | runs | result (failed/1800) |
|---|---|---|
| ac9997c51 | 44547381538, 44547388717 | pass, 1 |
| bc073c19b | 44547398283, 44547406483 | pass, pass |
| 6c7ce1f5a | 44547414380, 44547422531 | pass, pass |
| 27bf33105 | 44547431123, 44547437218 | pass, pass |
| 594ae6b11 | 44547444072, 44547450625 | pass, pass |
| 7e3c75d09 | 44547456811, 44547467311 | pass, 1 |
| 54d16d617 | 44547474456, 44547482524 | pass, pass |
| 0e5c84050 | 44547489550, 44547497435 | pass, pass |
| **9d656c446** (queue v2 default) | 44553433260, 44553438870 | **19, 14** |
| master 4ba368c63 | 44553446580, 44553454803 | 28, 34 |

Culprit: 9d656c446 (queue v2 switch). 1/1800 is baseline flake.

Bug 1 found (fixed, commit on fix-restart-regression): controller close callback removed the shard entry
from the registry before `Lifecycle::on_closed` upgraded its Weak -> engine.stop() never ran on shard close.
Queue v1 self-stops on ShardClosed (masked it); queue v2 does not (like Go), so v2 loops kept running on the
closed shard ("Failed to update queue state error=shard closed" every ~30 s). Fix keeps the removed cell alive
until the engine is stopped (Go removeEngineForShard always stops the item's engine).

Validation of the fix branch:
- rust-ci cadence-history 44561468088 success
- history-mixed-gate MySQL 44561479043 success, `GATE SUMMARY failed: []`
- restart gate 44561486156 FAIL 27, 44561494007 FAIL 13 -> "shard closed" errors gone (0), but failures remain:
  query state "init", ServiceBusy "cadence-history unavailable: TChannel error 1" and result deadline exceeded;
  history logs show 29x "Fail to process task error=context deadline exceeded". Some appear before faults.
Status: OPEN. A second queue-v2-specific issue remains. Next steps: compare restart gate with
history.enableTransferQueueV2/enableTimerQueueV2=false on master (if green, v2 path), then per category (transfer only / timer only).

## fix-v2-restart bisect (2026-11-19/20)
Restart gate MySQL 600s, branch fix-v2-restart:
- v1 (both v2 off): PASS 44569724164, 44569731235
- transfer-only v2: FAIL 44569673637, 44569679791; timer-only v2: FAIL 44569687064, 44569693894
- v2 cached reader disabled/shadow: FAIL 44576178696, 44576186728, 44576194660 (cached reader not the cause)
- Fix efe0ad7ce (scheduled max read level held while a task write is in flight; Go shard-lock parity):
  rust-ci cadence-history 44582621160 success; timer-only v2 PASS 44582627935, 44582633770;
  default (both v2) FAIL 44582640507, 44582648353; transfer-only FAIL 44582654324.
- Open: transfer v2 tasks delayed 5-10s (decision scheduled->started 9.3s). Read-level/notify ordering verified matches Go.
- Transfer-only 44582654324 vs v1 pass 44569724164: steady-state identical (e2e p50 4065 vs 4063ms, p99 35.6s vs 34.5s).
  v2 adds 10 failures, all inside fault windows (0 failed in first ~400): 6x result "TChannel error 1: Request timed out"/deadline,
  4x query "must handle at least one decision task". The earlier "9s steady-state delay" was fault-window recovery, not steady state.

## 2026-11-19/20: transfer v2 lost decision tasks, root cause and fix (head dca628c49)

Comparison runs before the fix (bb9eb2a85):
- restart gate, both v2, 600s: 44598123434 failed=9, 44598130499 failed=16. Failures were stuck runs (DecisionTaskScheduled only) and the result/query errors that follow from them.
- restart gate, queue_v2=v1, 1500s: 44598137891 success.
- restart gate, both v2, cached_reader=disabled, 600s: 44605815266 success. This narrows the fault to the v2 cached immediate queue reader.

Root cause: the v2 cached immediate reader serves `[ack, maxReadLevel)` from its in-memory cache. Go injects written tasks into the cache through `notifyingExecutionManager` inside the shard lock, before `updateMaxReadLevelLocked`. Its reader then takes the shard RLock to read the level. Rust differed in two ways:
1. It published the immediate max read level before it notified (injected), sometimes after dropping the write gate. A concurrent v2 read could cover a task ID not yet in the cache and skip that task for good.
2. `update_workflow_execution_with_new_as_active` (engine_start) never notified the new run's tasks at all.

Fixes on fix-v2-restart:
- fa0a634c5: notify the new run's tasks after the update-with-new write.
  - Alone, this made the gate worse: 44606414877 failed=39, from the ordering race in (1).
- 97f7688af: notify (inject) before `update_max_read_level` on every write path.
  - The new-run tasks are notified inside persist together with the mutation; the resetter uses `take_pending_tasks`.
  - Cassandra mixed gate 44614353596 then failed once, with "Context timeout is too short" on RespondDecisionTaskFailed: a lost queue wake delayed the decision task.
- dca628c49: the v2 immediate `process_new_tasks` awaits in-flight task writes (`lock_task_writes`) before reading the level. This matches Go's RLock in `getImmediateTaskMaxReadLevel`, so a wake from notify always sees the new level.

Validation at dca628c49 (all success):
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/44617689979
- History mixed gate MySQL: https://github.com/auenkr/cadence-rust/actions/runs/44617697434
- History mixed gate Cassandra: https://github.com/auenkr/cadence-rust/actions/runs/44617704654
- Restart gate, both v2, 1500s, run A: https://github.com/auenkr/cadence-rust/actions/runs/44617711152
- Restart gate, both v2, 1500s, run B: https://github.com/auenkr/cadence-rust/actions/runs/44617717446

There is no Go-baseline restart workflow, so no Go flake rate was measured.
