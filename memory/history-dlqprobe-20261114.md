# History DLQ bad-type probe + MySQL stale-connection retry (2026-11-14)

Branch `history-dlqprobe`, validated head `d44bc772e1157b77831ca00685a161b4ac40c76a` (code commit; memory commit follows).

## Causes / fixes
1. **DLQ bad-type probe**: Rust `HandlerImpl::get_dlq_replication_messages` ran one engine call for all infos and propagated the error (INTERNAL "unsupported task type"). Go batches infos per (domain, workflow, run), runs each batch independently, logs "Failed to get dlq replication tasks." (or warns "History engine not found for workflow ID.") and skips the batch; response is always OK. Rust now does the same via `gather_dlq_replication_tasks` (rust/crates/cadence-history/src/handler/handler_impl.rs); Go's checks are only shutting-down (no rate limit / nil error), also matched. Unit test: `dlq_replication_messages_skip_failed_batches`.
2. **mysql/tchannel recovery timeouts (run 43550174122)**: `CodecError: Broken pipe` came from `START TRANSACTION` on a pooled MySQL connection broken by the kill/restart; Rust poisoned the connection but returned the error to the caller. Go database/sql retries BeginTx on bad connections (maxBadConnRetries=2, then a fresh connection). Judged a real bug (not only a flake): `SqlBackend::transaction` (rust/crates/cadence-persistence/src/history_sql_backend.rs) now discards the connection and retries START TRANSACTION up to 2 times on connectivity errors (nothing executed yet, safe). Mid-transaction errors are still not retried (as in Go). No dedicated unit test (needs a live broken connection); covered by the recovery gate.

## Evidence (all on head d44bc772e1157b77831ca00685a161b4ac40c76a)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43554812485 success
- rust-ci cadence-persistence: https://github.com/auenkr/cadence-rust/actions/runs/43554815636 success
- history-mixed-gate, all 12 combos success with `GATE SUMMARY failed: []`: 43555515110, 43555517800, 43555520491, 43555523529, 43555526349, 43555529484, 43555532514, 43555535040, 43555538251, 43555540732, 43555543180, 43555546024 (https://github.com/auenkr/cadence-rust/actions/runs/<id>), including mysql/tchannel/two_history=true (43555520491).
- No local cargo/go/docker.
