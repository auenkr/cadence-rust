# eff-db-roundtrips (2026-11-19) — fixes verified (gates + after-benchmark green)

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
- history-mixed-gate mysql/grpc https://github.com/auenkr/cadence-rust/actions/runs/44441490810 success; `GATE SUMMARY failed: []`
- history-mixed-gate postgres/tchannel https://github.com/auenkr/cadence-rust/actions/runs/44441495221 success; `GATE SUMMARY failed: []`

## After (code 4463266d3, same settings)
History lane https://github.com/auenkr/cadence-rust/actions/runs/44441499077 (success);
all-Rust https://github.com/auenkr/cadence-rust/actions/runs/44441503390 (success).
Per workflow (all 3 pairs within 0.3): Rust BEGIN/COMMIT 32.7 -> 7.0 (= Go), Questions 141.8 -> 90.4
(Go 95.5), SET isolation per tx 0 (session init only). stmt_execute 76 / Com_select 44 unchanged (Go 65 / 34).
Est. round trips/wf: Rust ~146 -> ~94 (Go ~152).

CPU = container avg cores; completion latency ms (mean of 3 pairs).
| lane | arm | before History CPU | after History CPU | before MySQL CPU | after MySQL CPU | before p50/p99 | after p50/p99 |
|---|---|---:|---:|---:|---:|---|---|
| history | Go | 0.220 | 0.435 | 0.266 | 0.505 | 39.9 / 54.7 | 68.4 / 111.1 |
| history | Rust | 0.147 | 0.222 | 0.216 | 0.338 | 34.7 / 42.8 | 47.5 / 66.5 |
| all | Go | 0.407 | 0.402 | 0.478 | 0.475 | 61.0 / 105.9 | 60.2 / 103.1 |
| all | Rust | 0.212 | 0.171 | 0.361 | 0.305 | 45.6 / 62.6 | 40.5 / 56.8 |

Caveat: the History-lane after run landed on a ~2x slower runner (Go arm, unchanged code, doubled CPU
and latency), so compare Rust/Go ratios there: History CPU 0.67 -> 0.51, MySQL CPU 0.81 -> 0.67,
p50 0.87 -> 0.69, p99 0.78 -> 0.60. All-Rust lane (Go arm stable): Rust History CPU -19%,
MySQL CPU -16%, completion p50 -11%, p99 -9%.

## Next steps
1. Attribute the +11 SELECT/wf: run benchmark with CADENCE_SQL_STATS_SECS set (needs compose env
   passthrough) or compare Go/Rust History call paths (shard range reads, domain cache, mutable-state
   cache misses, history-branch reads).
2. Optional: re-run History lane after-benchmark to get a same-runner-class comparison.
(Temp artifacts in /mnt/storage/service/tmp deleted.)
