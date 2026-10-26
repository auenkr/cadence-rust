# Versioned CQL body codecs — 2026-10-27

Base: `t3/cql-versioned-frames` at `6fefb904f2d985dd6703cd15047c534298b38310`.

This increment adds explicit `CqlProtocolVersion` body APIs in the vendored
`scylla-cql` crate. Existing serialization/deserialization entry points keep
their v4 behavior. QUERY/EXECUTE/BATCH/PREPARE and RESULT/EVENT codecs now have
version-accepting entry points; prepared metadata omits partition-key indexes
before v4, v2 schema-change payloads use the legacy keyspace/table form, and
v2 batch parameters omit the flags byte. Byte fixtures cover default query
parameters, batch flags, prepared metadata, and v2/v3 schema-change layouts.

The expected v2/v3 gates were checked against the pinned Go gocql source
`v0.0.0-20211015133455-b225f9b53fa1` (`frame.go`: `writeQueryParams`,
`writeBatchFrame`, `parsePreparedMetadata`, and `parseResultSchemaChange`). The
fixtures encode those wire layouts directly; they have not been run against a
live Go oracle. Dependency CI is dispatched with `package=scylla-cql` and a
fresh target: <https://github.com/auenkr/cadence-rust/actions/runs/40412352115>
(head `3198153f5cdb430a69d2dca76170905f56d8d506`; status was `in_progress` at
recording time; conclusion pending).

## Remaining work before driver use

- The connection/frame path still hardcodes v4 and does not call these APIs.
  The `scylla` connection owner must pass the negotiated `CqlProtocolVersion`
  into request body encoding and response body parsing, after header integration
  is complete. `frame/mod.rs` and connection transport were deliberately left
  untouched in this increment.
- QUERY values still use the existing `SerializedValues` representation. It
  supports positional values and v4 unset cells, but this change does not reject
  an unset cell when asked to encode v2/v3 (the pinned Go source explicitly
  limits `UnsetValue` to v4). Matching's current Cassandra statements use
  positional values and do not intentionally use unset values; callers that do
  need a version-aware validation path before broader use.
- Named QUERY values are still rejected by the existing deserializer.
- Result type metadata parsing (including collection and UDT type encodings)
  still uses the v4 parser. Verify and implement the v2/v3 type-ID/recursive
  layouts if schemas used by Matching expose those types.
- There is no `response/error.rs` in this pinned source tree; ERROR parsing
  lives in `frame/response/mod.rs`. Error payloads have not gained a versioned
  entry point in this change.
- PREPARE has the same query-string body across v2-v4, so its versioned API is
  explicit but currently delegates to the common encoding.

The work is codec-only. It does not enable old-version negotiation or constitute
Persistence/Matching acceptance evidence.
