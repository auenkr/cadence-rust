# Protocol v2 typed collection cell transcoding (2026-10-27)

Branch: `t3/typed-collection-cell-transcoding`  
Commit: `842cc7d57acfc61a0f17ce4fc0e077f96f5d37d7`  
CI: focused `scylla-cql-core` runs <https://github.com/auenkr/cadence-rust/actions/runs/40414546664> and <https://github.com/auenkr/cadence-rust/actions/runs/40415052979> both failed at Clippy; latest code commit tested was `babe75d3a56ad231e4aa0bb117e1a133c5f58f1b`.

`scylla-cql-core::protocol_collections::transcode_cell` now accepts a typed
`ColumnType`, source and destination `CollectionProtocol`, and cell bytes. It
recursively rewrites list/set members and map keys/values using the schema,
keeps scalar values (including blobs) opaque, and returns
`CollectionCodecError` for malformed/overflow collection encodings. Protocol
v4 and newer use `CollectionProtocol::V3OrNewer`; v2 uses `V2`. The function
has no shared or negotiated-protocol state.

## Required live call sites

- **Result cells:** the body decoder must retain each result metadata column's
  `ColumnType` and pair it with that row's length-delimited value. For each
  non-null cell call `transcode_cell(column_type, wire_protocol, V3OrNewer,
  payload)` before passing the value to the existing driver value decoder.
  Metadata must be the response's actual result schema, not a guessed type or
  an inspection of payload bytes.
- **Bound request values:** the body encoder must pair each bound value with
  the matching prepared bind marker `ColumnSpec::typ()` from
  `PreparedMetadata::col_specs` (in marker order). Call
  `transcode_cell(marker_type, V3OrNewer, wire_protocol, payload)` before
  writing the length-delimited value. Do not transcode `NULL` values.
- Keep v4+ path byte-compatible. Select the protocol per connection/request;
  do not put it in global or thread-local state. Ensure this helper is actually
  called at both live body boundaries; this commit alone does not wire either
  boundary and does not complete Matching.

## Evidence and limits

Added core unit tests cover a Go-layout map with a pair count, recursively
typed map/list values, opaque blobs, v3+ identity, round-trip bytes, and
malformed nested values. `git diff --check` passed. Cargo, Go, and rustfmt were
not run on the host per project policy. The first CI attempt found and fixed a
test element type inference issue and two Clippy style findings in this file.
The retry no longer reports findings in `protocol_collections.rs`. It still
fails on unfulfilled `#[expect]` lints in unrelated core files
(`deserialize/value.rs`, `frame/frame_errors.rs`, `frame/mod.rs`, and
`deserialize/frame_slice.rs`) and a dead-code lint in
`deserialize/value_tests.rs` using the runner's Clippy. The workflow stops
before Cargo tests. No unrelated upstream files were changed to suppress those
warnings.
