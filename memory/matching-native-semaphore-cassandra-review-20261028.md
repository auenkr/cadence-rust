# Matching native semaphore Cassandra review — 2026-10-28

## Scope and source pointers

- Native adapter: `rust/crates/cadence-persistence/src/semaphores_cassandra_native.rs`.
  - CQL and raw `GoString` binding: lines 19–48.
  - Metadata LWT: lines 68–87.
  - Page-state scan: lines 94–108.
  - Grant and release conditional batches: lines 111–148.
- Shared session/legacy typed adapter: `rust/crates/cadence-persistence/src/cassandra.rs`, especially constants at lines 39–48 and typed implementation at lines 249–518.
- Go parity references:
  - `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_metadata.go:31-62` uses `INSERT ... IF NOT EXISTS` and checks only `MapScanCAS`'s applied result.
  - `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens_cql.go:29-52` defines the two grant and release conditional batch statements.
  - `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens.go:58-109` maps grant's batch CAS failure to owner-held versus slot-taken.
  - `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens.go:34-50` documents the exact key sentinels.
- Driver behavior: `rust/vendor/scylla-1.9.0/src/client/session.rs:680-706` says nonempty-value `query_unpaged` prepares on a single connection and incurs two round trips. The native adapter's `query_unpaged` bound calls are supported but are not an explicit cached prepared-statement path. `Session::batch` docs at `session.rs:1250-1258` similarly note unprepared batch statements with values require preparation and extra round trips.

## Fix made

At adapter lines 76–85, metadata LWT decoding now branches on returned column metadata: successful `INSERT IF NOT EXISTS`'s one-column `[applied]` result decodes as `(bool,)`, while the six-column conditional-failure shape decodes the existing metadata row. The previous fixed six-column tuple could reject the normal success metadata shape before reading `applied`.

Grant/release batch CAS decoding at lines 117–131 and 142–148 now branches on actual result column count. It handles the one-column applied result and the failed first `UPDATE ... IF holder ...` shape (applied + six primary-key columns + the conditional `holder` column). The previous hard-coded ten-column decode did not correspond to that conditional UPDATE's returned row shape. Unknown shapes return a diagnostic persistence error instead of being misread.

Returned CQL `text` values continue through Rust `String` decoding and then `into_bytes()`: Cassandra validates text UTF-8, and UTF-8 decoding/re-encoding preserves those bytes without Unicode normalization. Bound identifiers and sentinels use the `RawText` serializer to preserve GoString bytes and reject non-text CQL metadata.

## Test and runtime evidence (unverified)

- Existing adapter unit tests at the bottom of `rust/crates/cadence-persistence/src/semaphores_cassandra_native.rs` are serializer-only (`raw_text_binding_keeps_go_string_bytes_exact`, `go_string_values_round_trip_bytes_at_adapter_boundary`). There is no available executor/response-metadata seam in this adapter for constructing real Scylla `QueryRowsResult` metadata locally without a live cluster. No test was added or run under the no-local-Cargo instruction.
- Parent reports CI compile check run `40718207876` covers the adapter as it existed before this review's metadata-shape edits. Those edits are **not** covered by that CI result; parent must dispatch CI for the review changes.
- No Cassandra/Scylla integration test or mixed-cluster gate was run. Live persistence behavior, LWT response metadata, restart/recovery, fencing, consistency and paging-state behavior remain unverified here.
- This review found the Rust driver supports current `query_unpaged` bound calls by preparing them internally, at a two-round-trip cost. It does not establish the intended consistency/serial-consistency profile of the shared session; verify configured session defaults and live LWT behavior in the parent gate.

Parent applied CI-reported paging fix: materialize driver paging state with bytes.to_vec() to match Option<Vec<u8>>. No local build/test ran.
