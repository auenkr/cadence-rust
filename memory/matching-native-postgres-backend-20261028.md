# Matching native PostgreSQL backend (MatchingTaskStoreBytes) — 2026-10-28

Files: `rust/crates/cadence-persistence/src/matching_postgres_native.rs`
(private child module of `matching_postgres.rs` via `#[path]`), plus one
`mod native;` line in `matching_postgres.rs`. Legacy `MatchingTaskStore` API
is unchanged. **Not compiled or tested yet** (no local Cargo allowed); parent CI
must validate.

## Behaviour
- `impl MatchingTaskStoreBytes for PostgresTaskStore`: all 10 operations plus
  `fetch_latest_config` (delegates to the legacy implementation).
- Same SQL consts, pool, shard routing (`go_db_shard_id_bytes` over raw name
  bytes), encoding source, read-then-transaction lease flow, `FOR UPDATE`
  range fencing, per-task insert before fence check in `create_tasks_with_domains`,
  affected-row checks and `normalize_postgres_error` classification as legacy.
- Task-list name is bound via `RawText`, a `ToSql` for TEXT/VARCHAR/BPCHAR/NAME
  that writes the exact bytes (uses `postgres::types::private::BytesMut`, no new
  dependency). This mirrors Go: lib/pq sends `string` params as text with no
  UTF-8 validation, so PostgreSQL decides. A UTF-8 database rejects `\xff`,
  `\xfe` and NUL (SQLSTATE 22021); the error maps to `BackendErrorKind::Internal`
  (transient), exactly as Go-equivalent legacy classification of unknown SQLSTATEs.
  Nothing is replaced, escaped or aliased; U+FFFD is its own valid key. A
  SQL_ASCII database would store the raw bytes, as Go would.
- Blobs use `encode_/decode_task(_list)_bytes`, so partition maps, isolation
  groups and workflow IDs keep raw bytes; RunIDs resolve with
  `TaskDataBytes::for_sql`, and malformed RunIDs return
  `MalformedSqlRunId` with the raw bytes retained (non-transient).
- Only lossy spot: the human-readable stale-lease `ConditionFailed(String)`
  message uses `from_utf8_lossy` of the name (Go formats raw bytes with `%v`);
  it is never persisted or used as a key.

## Tests
Unit: raw wire bytes for `ff`/`fe`/U+FFFD/NUL, accepted types, independent
shard-hash inputs, task-type range error, stale-message parity with legacy,
malformed raw RunID retention. `#[ignore]` live test
`live_native_store_raw_bytes_fencing_and_legacy_cross_read` (needs
`CADENCE_POSTGRES_ADDR` etc. and the Cadence schema): server rejects `ff`,
U+FFFD lease works, `fe` stays absent, stale/fenced lease and update fail,
native write is read by the legacy store and vice versa, count/range delete,
`delete_task_list`.

## Compile risks
`use super::*` relies on parent private imports (ToSql, Uuid, Arc, BlobEncoding,
consts); `to_sql_checked!` via `postgres::types`; `postgres::types::private` is
hidden but public. Live DB behaviour for rejection of invalid bytes is
unverified until run against a real UTF-8 PostgreSQL.

## Parent integration correction

Lease-renewal messages now use the native `ConditionFailed { message: GoString }` variant and retain the original name bytes; the worker-era lossy-message limitation above is superseded. Native handler adoption must use the stored message bytes rather than diagnostic Display. The PostgreSQL live test covers native-write/legacy-read only; reverse cross-read coverage is still pending. All runtime claims remain unverified.
