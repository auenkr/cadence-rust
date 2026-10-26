# Versioned CQL body codecs — 2026-10-27

Base: `t3/cql-versioned-frames` at `6fefb904f2d985dd6703cd15047c534298b38310`.

This increment adds explicit `CqlProtocolVersion` body APIs in the vendored
`scylla-cql` crate. Existing serialization/deserialization entry points keep
v4 behavior. QUERY/EXECUTE/BATCH/PREPARE and RESULT/EVENT codecs have
version-accepting entry points; prepared metadata omits partition-key indexes
before v4, v2 schema-change payloads use the legacy keyspace/table form, and
v2 batch parameters omit the flags byte. Byte fixtures cover query parameters,
batch flags, prepared metadata, and v2/v3 schema-change layouts.

The expected v2/v3 gates were checked against the pinned Go gocql source
`v0.0.0-20211015133455-b225f9b53fa1` (`frame.go`: `writeQueryParams`,
`writeBatchFrame`, `parsePreparedMetadata`, and `parseResultSchemaChange`).
Fixtures encode those wire layouts directly; they have not been run against a
live Go oracle.

## Body dispatcher integration (2026-10-27)

The assigned checkout is based on the matching transport head `88cec3652`;
codec implementation `3198153f5` was cherry-picked as `4a997f55f`. The
connection's configured `CqlProtocolVersion` now flows through
`SerializedRequest::make_with_version` into request body serialization,
including compressed requests. QUERY, EXECUTE (both request structs), BATCH,
and PREPARE implement the default-compatible trait method. The connection
reader carries its configured version with each response, and
`Response::deserialize_for_version` dispatches version-aware RESULT and EVENT
parsing. Existing public default entry points remain v4. A direct frame test
asserts that a timestamp-bearing QUERY has the expected v2/v3 body lengths.
CI evidence is pending.

## Remaining gaps

- Go-compatible v2/v3 rejection of `UnsetValue` is not implemented.
  `SerializedValues` stores protocol cells, but the public request error type is
  `QueryParametersSerializationError` from the separately sourced
  `scylla-cql-core` dependency and has no suitable unset/protocol error variant.
  Avoiding a misleading unrelated error or a broad core vendoring change needs
  a deliberate API decision. This must be closed before accepting v2/v3 values.
- Named QUERY values remain rejected by the existing deserializer.
- RESULT prepared metadata and schema-change parsing are version-aware. Row
  type metadata parsing (including collection and UDT recursive type encodings)
  still uses the v4 parser. Verify and implement v2/v3 type-ID/recursive layouts
  if Matching schemas expose those types; coverage is not claimed.
- There is no `response/error.rs` in this pinned source tree; ERROR parsing lives
  in `frame/response/mod.rs`. Error payloads have no versioned entry point.
- PREPARE has the same query-string body across v2-v4, so its versioned API
  delegates to the common encoding by design.

This work does not enable old-version negotiation or constitute
Persistence/Matching acceptance evidence. No mixed gate or persistence
acceptance is claimed.
