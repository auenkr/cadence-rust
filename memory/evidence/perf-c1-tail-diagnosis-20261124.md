# c1 tail latency diagnosis (2026-11-24)

Run: https://github.com/auenkr/cadence-rust/actions/runs/45337477057 (success), branch `perf-bench-instrumentation`, head 9284784133a9d8951aeede796ab778116d115a22.
Inputs: rust_services=frontend,matching,history, concurrency=1, repetitions=3, requests=1000, warmup=10, sql_slow_ms=40 (CADENCE_HISTORY_SQL_SLOW_MS on history, both arms). Raw data: `memory/evidence/perf-c1-tail-data/` (completion-order samples, tail history gaps for slowest 5% = 50/run + 20 random fast/run, cgroup cpu.stat before/after for all containers incl. MySQL, slow-SQL counts).
Instrumentation: driver `*-samples.json`/`*-tail-history.json`, `collect_resources.py --extra-output`, `sql_slow_ms` workflow input (warmup input already existed, default 10).

## Output of analysis (3 reps x 1000 workflows per arm; gaps are medians, ms, slow vs fast)
```
go n 3000 p50 24.9 p95 100.3 p99 103.3 frac>55ms 0.217
 gap median ms  slow / fast
  1_start_to_dec1_scheduled                      0.0     0.0  (n 150/60)
  2_dec1_scheduled_to_started                   43.1     5.9  (n 150/60)
  3_dec1_started_to_completed                    4.0     3.9  (n 150/60)
  4_dec1_completed_to_activity_scheduled         0.0     0.0  (n 150/60)
  5_activity_scheduled_to_started                0.0     0.0  (n 150/60)
  6_activity_started_to_completed                3.3     3.3  (n 150/60)
  7_activity_completed_to_dec2_scheduled         0.0     0.0  (n 150/60)
  8_dec2_scheduled_to_started                   42.4     3.5  (n 150/60)
  9_dec2_started_to_completed                    4.1     3.8  (n 150/60)
  10_dec2_completed_to_workflow_completed        0.0     0.0  (n 150/60)
  11_total_history_span                         97.3    20.7  (n 150/60)
  00-go-c1.json {'k-frontend-1': (391, 0, 0, 5085), 'rk-history-1': (392, 0, 0, 12486), 'k-matching-1': (392, 0, 0, 2959), 'mark-mysql-1': (391, 0, 0, 15116), 'ark-worker-1': (189, 0, 0, 130)}
  01-go-c1.json {'k-frontend-1': (396, 0, 0, 4845), 'rk-history-1': (397, 0, 0, 12094), 'k-matching-1': (397, 0, 0, 2835), 'mark-mysql-1': (396, 0, 0, 14748), 'ark-worker-1': (177, 0, 0, 124)}
  02-go-c1.json {'k-frontend-1': (392, 0, 0, 4841), 'rk-history-1': (392, 0, 0, 12096), 'k-matching-1': (392, 0, 0, 2841), 'mark-mysql-1': (392, 0, 0, 14783), 'ark-worker-1': (169, 0, 0, 124)}
 slowsql ['0', '0', '0']
rust n 3000 p50 18.8 p95 100.3 p99 107.5 frac>55ms 0.277
 gap median ms  slow / fast
  1_start_to_dec1_scheduled                      0.0     0.0  (n 150/60)
  2_dec1_scheduled_to_started                   44.3     3.8  (n 150/60)
  3_dec1_started_to_completed                    3.9     3.3  (n 150/60)
  4_dec1_completed_to_activity_scheduled         0.0     0.0  (n 150/60)
  5_activity_scheduled_to_started                0.0     0.0  (n 150/60)
  6_activity_started_to_completed                2.8     2.7  (n 150/60)
  7_activity_completed_to_dec2_scheduled         0.0     0.0  (n 150/60)
  8_dec2_scheduled_to_started                   43.1     2.6  (n 150/60)
  9_dec2_started_to_completed                    4.2     3.3  (n 150/60)
  10_dec2_completed_to_workflow_completed        0.0     0.0  (n 150/60)
  11_total_history_span                         98.2    15.8  (n 150/60)
  00-rust-c1.json {'k-frontend-1': (371, 0, 0, 3640), 'rk-history-1': (371, 0, 0, 6812), 'k-matching-1': (370, 0, 0, 2270), 'mark-mysql-1': (371, 0, 0, 9636), 'ark-worker-1': (264, 0, 0, 207)}
  01-rust-c1.json {'k-frontend-1': (384, 0, 0, 3363), 'rk-history-1': (384, 0, 0, 6273), 'k-matching-1': (383, 0, 0, 1998), 'mark-mysql-1': (383, 0, 0, 9223), 'ark-worker-1': (253, 0, 0, 201)}
  02-rust-c1.json {'k-frontend-1': (377, 0, 0, 3364), 'rk-history-1': (376, 0, 0, 6292), 'k-matching-1': (377, 0, 0, 1985), 'mark-mysql-1': (377, 0, 0, 9266), 'ark-worker-1': (249, 0, 0, 195)}
 slowsql ['0', '0', '0']
```
Columns in the gap table: slow / fast. cgroup tuples: (periods, nr_throttled, throttled_ms, cpu_ms) per container per rep.

## Conclusion
- (c) CPU throttling: ruled out. nr_throttled = 0 and throttled_usec = 0 for every container (all services and MySQL) in both arms.
- (a) History SQL/retry backoff: no evidence. Zero transactions >= 40 ms in all 3 Rust reps (and Go); the History handlers themselves (decision started->completed ~4 ms, event gaps 0.0 ms) are identical for slow and fast workflows.
- The tail is entirely the two **DecisionTaskScheduled -> DecisionTaskStarted** gaps (dispatch to the SDK poller): ~43-44 ms each in slow workflows vs 3-6 ms in fast ones; slow total ~98 ms = 2 x ~43 + ~12 ms of work. Every other gap is equal between slow and fast. This is the matching task-delivery step (the (b)-class mechanism: a task missing the sync-match / poller waiting, with a fixed wait before it is handed over) or SDK poller re-poll timing, not History or CPU.
- Go shows the SAME bimodal pattern in this run (Go p95 100.3 ms, 21.7% of workflows > 55 ms vs Rust 27.7%; Rust p50 18.8 vs Go 24.9 ms), so the ~43 ms step is not Rust-specific in this configuration (DisableStickyExecution driver, c1). Note this Go arm differs from the earlier report's 53/75 ms Go p95, so the earlier Go figure is not reproduced by this run; Rust's tail share is somewhat higher (27.7% vs 21.7%), p99 107.5 vs 103.3.
- Not determined: which exact wait (matching 100 ms fallback poll vs. SDK poller re-poll) produces the ~43 ms; that needs matching-side timestamps (task create/dispatch) which no current service logging provides. Next step: compare Scheduled->Started with matching sync-match hit rate, or vary worker pollers (`worker_pollers` input).
