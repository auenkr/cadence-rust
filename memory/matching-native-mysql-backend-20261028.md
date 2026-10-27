# Matching native MySQL backend — 2026-10-28

`MySqlTaskStore` now implements `MatchingTaskStoreBytes` in
`rust/crates/cadence-persistence/src/matching_mysql_native.rs`, a private child
module of `matching_mysql.rs` (`#[path]`). The legacy `MatchingTaskStore` impl
and public API are unchanged.

- Raw name bytes are bound as `&[u8]` (`Value::Bytes`), the same wire form Go's
  database/sql sends for a `string`; no UTF-8 conversion or replacement rune.
  If the MySQL column charset rejects invalid bytes, the server error is
  classified by the existing `normalize_mysql_error`, as Go does. Nothing is
  re-encoded. (Server-side acceptance of 0xff/0xfe was not verified against a
  live DB; needs a mixed gate.)
- Shard selection uses `go_db_shard_id_bytes` on raw bytes.
- Same SQL, transactions, lease fencing (range check before decode, insert of
  range 0 before stale check, FOR UPDATE lock + commit-only connection reuse),
  cross-domain task rows, paging, deletion, expiry as the legacy adapter.
- Blobs use `encode/decode_task(_list)_bytes`; RunID resolved by
  `TaskDataBytes::for_sql` before encoding, malformed RunID returns
  `MalformedSqlRunId` with raw bytes preserved, after the transaction begins
  (same order as legacy).
- Only lossy spot: error *message text* with the name (stale-lease message)
  uses `from_utf8_lossy`, since a Rust `String` cannot hold arbitrary bytes.
  No persistence value is affected.
- Tests (unit, no DB): bound parameters for ff/fe/U+FFFD stay distinct, shard
  hash on raw bytes, raw blob roundtrip and legacy-equal encoding for valid
  UTF-8, malformed RunID raw retention, error classification.
- Not covered: live DB lease-fencing/cross-read tests (require MySQL gate).

Compile risks (no local build was run): `use super::*` glob of private items
and `#[path]` module resolution; `mysql::Value: From<&[u8]>` in the test.
Not a Matching completion claim.

## Parent integration correction

Lease-renewal messages now use the native `ConditionFailed { message: GoString }` variant and retain the original name bytes; the worker-era lossy-message limitation above is superseded. Native handler adoption must use the stored message bytes rather than diagnostic Display. The PostgreSQL live test covers native-write/legacy-read only; reverse cross-read coverage is still pending. All runtime claims remain unverified.
