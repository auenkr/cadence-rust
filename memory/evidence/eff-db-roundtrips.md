# eff-db-roundtrips (2026-11-19) — PAUSED, after-results pending

Branch `eff-db-roundtrips` from master 56b58c6c1. Code head 4463266d3.

## Measurement harness
`Benchmark/collect_resources.py`: truncates `performance_schema.events_statements_summary_by_digest`
at measurement start, dumps top-200 digests at end (`mysql_digests` in `*-resources.json`), and adds
Com_stmt_prepare/execute/close, Com_begin/commit/rollback, Com_set_option, Com_admin_commands counters.
Note: COM_STMT_EXECUTE statements do not appear in the digest table (MySQL 8), so per-statement
attribution comes only from text statements (BEGIN/COMMIT/SET); use global Com_* for the rest.
Rust env-gated counter: `CADENCE_SQL_STATS_SECS=<n>` (History SqlBackend tx/autocommit/exec/query), stderr.

## Baseline (master 56b58c6c1, 15/s, 30 s warmup, 120 s, 3 alternating pairs)
History lane https://github.com/auenkr/cadence-rust/actions/runs/44427234634 (success);
all-Rust https://github.com/auenkr/cadence-rust/actions/runs/44427239929 (success).
Per completed workflow (1800 workflows/window), all 3 pairs identical within 0.3:

| counter / wf | Go | Rust (history lane) | Rust (all) |
|---|---:|---:|---:|
| Questions | 95.5 | 141.7 | 141.8 |
| stmt_prepare | 64.8 | 4.6 | 0.5 |
| stmt_execute | 64.8 | 76.0 | 75.9 |
| stmt_close | 64.8 | 4.6 | 0.5 |
| BEGIN / COMMIT | 7.0 / 7.0 | 32.7 / 32.7 | 32.7 / 32.7 |
| SET transaction_isolation | 8.2 | 0 | 0 |
| Com_select | 34.0 | 44.2 | 44.1 |
| insert/update/delete | 18/10/1 | 19/10/1 | 19/10/1 |
History CPU cores: Go ~0.22 vs Rust ~0.15 (history lane); Go 0.41 vs Rust 0.21 (all).
Round trips/wf approx: Go 65 prepare + 65 execute + 14 tx + 8 SET ~ 152; Rust 76 + 5 + 65 tx ~ 146.

Findings:
1. Rust wraps every History read in START TRANSACTION/COMMIT (+~26 tx pairs = ~52 RTT/wf). Go uses
   txExecute only for Create/Update/ConflictResolve/Delete workflow, CreateFailoverMarkerTasks,
   CompleteHistoryTask, UpdateShard, AppendHistoryNodes(new branch), DeleteHistoryBranch.
2. History MySQL transactions ran at server default REPEATABLE READ; Go DSN sets session
   transaction_isolation='READ-COMMITTED' at connect. Matching set it per tx (extra RTT per tx).
3. Rust prepared-statement cache already beats Go (Go prepares+closes every statement).
4. Rust issues ~11 more SELECT executes/wf than Go — NOT yet attributed (next step).

## Fixes (code head 4463266d3)
- `session_isolation_init` in matching_mysql.rs: init `SET <attr>='<lowercased>'`
  per connection (Go buildDSNAttrs/sanitizeAttr); per-tx isolation removed (matching + native). Unit tests.
- `SqlBackend::autocommit` (retry <=2 on connectivity, like maxBadConnRetries) / `autocommit_once`
  (no retry, inserts): reads in sql_workflow_store get/get_current/list/cross-cluster/policy,
  delete current execution, history branch/tree reads, fork tree insert, non-new-branch node insert,
  task pages, range complete, DLQ ops, visibility record/delete. Go-transactional ops unchanged.
- Postgres: SqlTx now also implemented for postgres::Client (autocommit path).

## CI
- rust-ci cadence-persistence on isolation commit: https://github.com/auenkr/cadence-rust/actions/runs/44427409557 success
- rust-ci cadence-history on 4463266d3: https://github.com/auenkr/cadence-rust/actions/runs/44438979764 success
- rust-ci cadence-persistence on 4463266d3: https://github.com/auenkr/cadence-rust/actions/runs/44441507314 success
- PENDING at pause (started on 4463266d3, unverified):
  - history-mixed-gate mysql/grpc https://github.com/auenkr/cadence-rust/actions/runs/44441490810
  - history-mixed-gate postgres/tchannel https://github.com/auenkr/cadence-rust/actions/runs/44441495221
  - after-benchmark history https://github.com/auenkr/cadence-rust/actions/runs/44441499077
  - after-benchmark all https://github.com/auenkr/cadence-rust/actions/runs/44441503390

## Next steps
1. Read pending runs; gate pass = success + `GATE SUMMARY failed: []`.
2. Compare after vs baseline (scripts: per-wf Com_* deltas from resources.json mysql_samples / 1800).
   Expect BEGIN/COMMIT ~7/wf.
3. Attribute the +11 SELECT/wf: run benchmark with CADENCE_SQL_STATS_SECS set (needs compose env
   passthrough) or compare Go/Rust History call paths (shard range reads, domain cache, mutable-state
   cache misses, history-branch reads).
4. Delete /mnt/storage/service/tmp/eff-db-4182088 and eff-scripts after evidence is final.
