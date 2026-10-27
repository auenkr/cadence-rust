# Matching native SQLite backend (MatchingTaskStoreBytes) — 2026-10-28

Implemented in `rust/crates/cadence-persistence/src/matching_sqlite_native.rs`
(private child module of `matching_sqlite.rs` via `#[path]`; legacy API untouched).

- Go's SQLite driver binds strings with `sqlite3_bind_text` + length, no UTF-8
  validation: arbitrary bytes are stored as TEXT. rusqlite only binds valid
  `&str` as TEXT, so raw names are bound as BLOB and wrapped in
  `CAST(? AS TEXT)` (exact byte reinterpretation, TEXT storage class, same rows
  as legacy/Go writes for valid UTF-8). `ff`, `fe` and U+FFFD are distinct rows.
- Blobs use `encode/decode_task(_list)_bytes`; RunID via `TaskDataBytes::for_sql`
  (error `MalformedSqlRunId` keeps raw bytes). Lease fencing, insert-then-fence
  atomic batches, cross-domain rows, paging, deletes mirror the legacy code.
- Only diagnostic error text (lease renew message) renders the name lossily;
  `ConditionFailed` holds a `String`. No persisted/key data is lossy.
- Tests (in the native file): ff/fe/U+FFFD isolation + TEXT class, legacy↔native
  cross-read, fencing rollback + raw partition-map bytes, malformed RunID bytes.
- NOT compiled or tested: no local Cargo per policy. Compile risks: `legacy()`
  generic `Into<StorageError>` bounds (`BlobEncoding` errors are `cadence_thrift::Error`
  → `From` exists), `StorageError::from` in `and_then`, unused-import warnings.
  Needs CI. Matching is not claimed complete.

## Parent integration correction

Lease-renewal messages now use the native `ConditionFailed { message: GoString }` variant and retain the original name bytes; the worker-era lossy-message limitation above is superseded. Native handler adoption must use the stored message bytes rather than diagnostic Display. The PostgreSQL live test covers native-write/legacy-read only; reverse cross-read coverage is still pending. All runtime claims remain unverified.
