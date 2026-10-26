# Live protocol v2/v3 collection and UNSET bridges (2026-10-27)

Branch: `t3/codec-live-bridges` (base `t3/matching-cql-integration` dc84a0690). Not compiled or CI-run: no local Cargo per policy, parent coordinates CI.

Previously `protocol_collections::transcode_cell` had no live callsite and was not re-exported from `scylla`.

Wired now:
- **Request values (EXECUTE):** `Connection::execute_raw_with_consistency` calls the new `SerializedValues::transcode_for_protocol(prepared col_specs, v2)` on v2/v3 connections. UNSET is rejected on v2 and v3 (`OldProtocolValuesError::UnsetUnsupported`, surfaced as `RequestAttemptError::SerializationError`). On v2 each non-null cell is transcoded v3+ to v2 using the bind marker `ColumnSpec::typ()`. v4 path untouched.
- **Result rows:** `Connection::send_request` calls `DeserializedMetadataAndRawRows::transcode_v2_collections` on v2 responses, rewriting each non-null cell from v2 to v3+ widths using the response's real column types. Malformed rows give `ResultMetadataAndRowsCountParseError::V2RowsConversionError`.
- `scylla::protocol_collections` now also exports `transcode_cell`.

Known gaps (not done):
- BATCH values (`RawBatchValuesAdapter`) and unprepared QUERY with values are not adapted; they need the same UNSET rejection and v2 transcoding per bind-marker type.
- No unit tests yet for the new methods; need CI compile and tests.
- Persistence still rejects protoVersion 2/3 (`cadence-persistence/src/cassandra.rs:752`); keep until integration is validated.
