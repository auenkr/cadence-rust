# Matching native (raw-byte) Cassandra backend — 2026-10-28

Status: implemented, **unverified** (no Cargo run; parent owns CI).

## Files
- `rust/crates/cadence-persistence/src/matching_cassandra_native.rs` — `impl MatchingTaskStoreBytes for CassandraMatchingTaskStore` (private `#[path] mod native;` registered in `matching_cassandra.rs`).
- `matching_cassandra.rs`: only shared helpers split (`ttl_seconds`, `expiry_not_passed`) plus the module registration; legacy behavior unchanged.

## Design
- Go gocql marshals `string` for `text` as raw bytes (no UTF-8 check/normalization). Scylla's `String`/derive UDT path cannot carry non-UTF-8, so `RawCql` (custom `SerializeValue`) writes `GoString` bytes verbatim for text leaves, UDT fields (by name, missing → null, unknown → error), sets/lists and maps. Scalars (UUID, ints, timestamp) delegate to the stock serializers.
- Statement texts, ordering, LWT fencing, TTL (`USING TTL n` inline, from `created = now`), ALL→LOCAL_QUORUM delete fallback, paged `LIMIT ?` reads, and error classes (`Store::internal`) are shared with the legacy store. `create_tasks_with_domains` now binds values instead of literals (needed for raw bytes); fence is `UPDATE ... IF range_id = ?`.
- Condition-failure messages are built as raw bytes (`ConditionFailed{message: GoString}`). Lease renewal: `leaseTaskList:renew failed: taskList:<raw>, taskListType:N, haveRangeID:<caller>, gotRangeID:<stored>` per Go `nosql_task_store.go:139`.
- Malformed RunID → `Legacy(BackendConfiguration)` via `cassandra_run_id`, before any statement is sent.
- Reads: rows decoded with existing UDT types (server-validated UTF-8) → `GoString`, lossless.

## Limitations / risks
- Real Cassandra rejects non-UTF-8 `text` server-side; bytes are sent as Go would, the server error surfaces as `Backend/Internal`. No client-side rewrite.
- `MatchingTaskStoreBytesError::lease_renewal` in `matching_native_task_store.rs` (not owned) differs from Go text (`renew failed:taskList:` without space, `haveRangeID:0`); this backend does not use it. Parent may fix it.
- Compile risks: `scylla::cluster::metadata`/`serialize::writers` import paths, `CellValueBuilder` API use, closure `overflow` reuse, unused-import lints under `-D warnings`.
- Unverified against a live Cassandra (UDT/collection wire encoding hand-written; unit tests cover bytes for text/UDT/map).
- Tests added: raw/non-normalized text bytes, UDT+map byte layout, lease renewal raw message, create fencing/raw name/lost-fence error/malformed RunID, legacy↔native blob conversion.

## Parent source reconciliation

The lease-renewal helper in matching_native_task_store.rs is deliberately SQL-specific in behavior: go/common/persistence/sql/sql_task_store.go:137 formats without a space after failed: and prints its local rangeID (still zero). The Cassandra/nosql reference at go/common/persistence/nosql/nosql_task_store.go:139 includes the space and the caller RangeID. Both backend-specific messages must remain distinct. The worker concern that the shared helper was globally wrong is withdrawn for its SQL callers; Cassandra correctly constructs its own message.
