# eff-db-writes-prepare (2026-11-19) — non-SELECT MySQL protocol overhead, Rust vs Go

Branch `eff-db-writes-prepare` from master 27bf33105. Fix commit ccd197dc5.
Benchmark: sdk-matching-benchmark service=all rate=15 warmup=30 duration=120 repetitions=3 (1800 wf/window).

## Harness extension
`Benchmark/collect_resources.py`: + Com_replace, Com_insert_select, Threads_created, Com_change_user,
Com_stmt_send_long_data, and per-table insert/update/delete row counts from
`performance_schema.table_io_waits_summary_by_table` (`tio_ins_/tio_upd_/tio_del_<table>`).

## Finding
Per-table inserted rows identical Go vs Rust (executions, current_executions, activity_info_maps, history_tree 1/wf,
history_node 6, timer_tasks 5, transfer_tasks 4). Rust issued +1 INSERT statement/wf: history tasks were inserted
one row per statement; Go `InsertInto{Transfer,Timer,Replication}Tasks` = one sqlx NamedExec multi-row INSERT
with RowsAffected == len(rows). Fixed in `history_task_sql.rs::insert_tx` (multi-row INSERT, postgres $n renumbered,
count check vs len; unit test). Everything else Rust already does less than Go (Go prepares+closes every statement,
SETs isolation and opens a new connection ~8x/wf). Nothing else to fix; SELECTs left to eff-db-selects.

## Per workflow (mean of 3 pairs)
| counter / wf | Go before | Rust before | Go after | Rust after |
|---|---:|---:|---:|---:|
| Com_insert | 18.00 | 19.00 | 18.02 | **18.01** |
| Com_replace / update / delete | 2 / 10 / 1 | 2 / 10 / 1 | same | same |
| stmt_prepare (= close) | 64.7 | 0.7 | 64.8 | 2.0 |
| stmt_execute | 64.7 | 75.9 | 64.8 | 74.9 |
| Com_set_option | 8.73 | 0.07 | 8.30 | 0.18 |
| Connections | 8.76 | 0.09 | 8.33 | 0.21 |
| Threads_created / window | 32-131 | 0-2 | | |
| Questions | 96.5 | 90.4 | 95.8 | 89.8 |
| MySQL CPU cores | 0.514 | 0.342 | 0.377 | 0.253 |
| History CPU cores | 0.432 | 0.207 | 0.328 | 0.162 |
Runner speed differed between runs (Go arm unchanged code: MySQL CPU 0.514 vs 0.377), so compare ratios:
MySQL CPU Rust/Go 0.665 -> 0.671 (noise; one fewer statement of ~90 is below resolution). Verdict: parity
achieved on write statement count; no measurable CPU change.

## Runs
- baseline bench https://github.com/auenkr/cadence-rust/actions/runs/44475416390 success
- after bench https://github.com/auenkr/cadence-rust/actions/runs/44481977392 success
- rust-ci cadence-persistence https://github.com/auenkr/cadence-rust/actions/runs/44481959951 success
- history-mixed-gate mysql https://github.com/auenkr/cadence-rust/actions/runs/44481964437 success (`GATE SUMMARY failed: []` asserted in job)
- history-mixed-gate postgres https://github.com/auenkr/cadence-rust/actions/runs/44481968609 success (same)
- matching-mixed-gate https://github.com/auenkr/cadence-rust/actions/runs/44481972707 success
