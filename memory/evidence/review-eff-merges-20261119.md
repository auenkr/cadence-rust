# Review: efficiency merges (2026-11-19)

Go-parity correctness review of 6c7ce1f5a, 27bf33105, 56b58c6c1, bc073c19b, ac9997c51.
Verdict: no correctness bugs found; no code changes, so no CI/gate run was needed.

## 6c7ce1f5a eff-db-roundtrips: PASS
- sql_history_store.rs:87: the append for a branch that already exists now uses one autocommit insert. This matches Go `AppendHistoryNodes` (go/common/persistence/sql/sql_history_store.go:147), which uses `m.db.InsertIntoHistoryNode` with no transaction. A new branch still uses a transaction for node + tree (Go txExecute :120). A duplicate key still maps to ConditionFailed (history_sql_backend.rs:119 MySQL 1062, :135 PG 23505).
- Reads that moved from a transaction to `autocommit` (task/timer/replication range reads, history branch/tree reads, execution/current reads, shard range_id): Go runs them as non-transaction `db.Select*`. Retrying after a connectivity error is safe because they are idempotent reads/deletes, like database/sql `maxBadConnRetries`.
- Non-idempotent inserts (visibility started/closed, DLQ put, tree insert) use `autocommit_once`, which never retries. That is correct.
- DeleteCurrentExecution, visibility delete, and range/DLQ deletes run as autocommit. Go also runs them without a transaction. They are idempotent, so a retry is safe.
- MySQL isolation moved from a per-transaction SET to a session init command (`SET transaction_isolation='read-committed'`), as go-sql-driver does with DSN attributes. The repo pool never calls `Conn::reset` (COM_RESET_CONNECTION would clear session variables), so the init setting persists. Transactions that keep a range-ID lock (`lock_and_check`, LockShard FOR UPDATE) are unchanged.
- Nit (not fixed): `autocommit` keeps the retry loop only when `discard` is set. That is the intended behaviour.

## 27bf33105 eff-history-cpu: PASS
- domain_cache.rs memo: the cache is keyed by id and checked with `Arc::ptr_eq` against the current snapshot Arc. Every refresh publishes new Arcs, and the memo holds the source Arc, so there is no ABA or stale entry. `engine_entry`/`task_entry` depend only on the metadata.
- Nit (not fixed): memo entries for deleted domains are never pruned. The growth is bounded by the number of domains ever seen.
- matching_domain_cache.rs: skipping lowercase conversion for keys already in ASCII lowercase gives the same results.

## 56b58c6c1 perf-history-tail: PASS
- history_sql_backend.rs:52 `blocking` uses `block_in_place` only when the runtime flavor is MultiThread. A current_thread runtime or no runtime runs inline, so there is no panic. Off-worker threads (spawn_blocking/block_on) are fine. Shard SQL logic is unchanged; LockShard/UpdateShard is still one transaction (Go UpdateShard).

## bc073c19b fix-lb-feedback: PASS
- LRU BoundedMap matches the eviction order of Go `cache.Cache`. Frontend calls `UpdatePartitionConfig`/`UpdateWeight` only after a poll succeeds (`reply?` comes first), the same as go/client/matching/client.go:158-169,199-210. A `None` config returns early, like Go's `config == nil`. A generation guard stops a stale poll from clearing the tracking of a newer poll from the same poller.

## ac9997c51 fix-lb-typed-flags: PASS
- The LB flag reads now use typed getters (Int/Bool/String), like Go `GetIntValue/GetBoolValue/GetStringValue`. Keys and defaults are unchanged (1, 1, false, "weighted", false).
