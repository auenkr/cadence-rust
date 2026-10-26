# Matching Cassandra collection codec boundary — 2026-10-27

## Checkout and merge

Work is on `t3/cassandra-v2-collections`, based on `c455bb8ad`. The requested
`origin/t3/matching-cql-integration` ref resolved to the same commit, so the
branch already contained that merge with no delta. Implementation commit:
`86c5c1517` (`Add protocol-aware CQL collection codec boundary`).

## Implementation

Vendored the locked `scylla-cql-core 1.9.0` source into
`rust/vendor/scylla-cql-core-1.9.0`, patched crates.io resolution to the local
crate, and included it in the Rust workspace so `-p scylla-cql-core` is a valid
focused CI selection. The new `scylla_cql_core::protocol_collections` module
and `scylla::protocol_collections` re-export provide an explicit versioned
boundary:

* V2 reads/writes u16 count and item sizes and rejects count or item size above
  65535.
* V3+ reads/writes i32 count and item sizes and rejects negative decoded sizes
  or sizes above i32::MAX.
* List/set and alternating key/value map payloads are supported. Collection
  bytes can themselves be an element, so nested collection payloads work
  without a cyclic dependency on driver protocol types.
* Decode rejects truncation, odd map element count, negative v3+ sizes and
  trailing bytes. Encode rejects odd map element count and width overflow.

The byte regression tests cover empty and multi-element list/map, empty nested
set/list payloads under both widths, round trips, v2 count/element overflow,
truncation, negative v3+ count and extra bytes. These are source fixtures until
the linked CI run finishes.

## Required integration before enabling protocol v2/v3

This commit intentionally leaves the default v4 driver APIs unchanged. The
transport/body worker must do all of the following before Matching can claim
protocol-v2 collection compatibility:

1. Carry the negotiated protocol selection to both value encoding and result
   cell decoding, and invoke `scylla::protocol_collections` at the raw
   collection payload boundary. Do not route collection values through the
   existing fixed-i32 serializer/deserializer.
2. Apply the decoder to typed list, set and map values in Matching's schema
   metadata result path (`rust/vendor/scylla-1.9.0/src/cluster/metadata/`)
   and task row decoding (`rust/vendor/scylla-cql-core-1.9.0/src/deserialize/`)
   or equivalent callsites in their new transport implementation. Preserve
   element type validation and null semantics after extracting payload slices.
3. Apply the encoder to collection-bearing bind values, including collection
   metadata and task rows, recursively passing nested payload bytes as
   elements. Keep V4 on the driver's existing default behavior.
4. Reject UNSET values under v2/v3 at the outer cell layer. This collection
   module does not change outer value sentinels.
5. Add tests that exercise those live callsites and run mixed Go/Rust Matching
   against persistence with the Go Matching instance absent, including
   failures/restart/recovery. Do not enable persistence v2/v3 before these
   pass.

Reserved by sibling workers per task request: request/response body modules,
connection/session and `metadata/fetching.rs` were not edited here.

## Validation

No Cargo, Go, or rustfmt command was run on the development host. Focused
workflow dispatched with:

```text
gh workflow run rust-ci.yml --ref t3/cassandra-v2-collections -f package=scylla-cql-core
```

Run: https://github.com/auenkr/cadence-rust/actions/runs/40413948100
Head at dispatch: `3286e0c656d15c87f1b65b46890dd78407fe19ca`.
Conclusion at last status query: pending; tests/clippy/format not yet verified.
