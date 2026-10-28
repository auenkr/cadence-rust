# Native Cassandra semaphore persistence checkpoint — 2026-10-28

## Implementation

Added `rust/crates/cadence-persistence/src/semaphores_cassandra_native.rs` as a child module of `cassandra.rs`, with a `SemaphorePersistenceBytes` implementation for the existing `CassandraSemaphorePersistence` session. The module binds name, owner, and sentinel text using raw byte serialization, provides metadata create/read and paged bucket scans, token/owner lookups, and logged conditional grant/release batches using the legacy Go CQL and row keys.

## Source of truth

- Go CQL statements and sentinel values: `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_tokens_cql.go`, `semaphore_tokens.go`.
- Metadata statements and validation: `go/common/persistence/nosql/nosqlplugin/cassandra/semaphore_metadata.go`.
- Legacy Rust Cassandra implementation and executor/session ownership: `rust/crates/cadence-persistence/src/cassandra.rs` (`CassandraSemaphorePersistence`, `CassandraBlockingSession`).
- Raw Cassandra text binding precedent: `rust/crates/cadence-persistence/src/matching_cassandra_native.rs` (`RawCql`). This semaphore module currently has a narrow text-only serializer because that RawCql is scoped to the Matching task adapter.
- Byte boundary: `rust/crates/cadence-persistence/src/semaphores_native.rs`.

## Verification and gaps

Two bounded unit tests cover verbatim text parameter serialization and adapter-boundary byte preservation. They are unverified: per task direction, no local Cargo, rustfmt, Go, build, or test commands were run. No live Cassandra server or compatibility behavior is claimed.

Integration still required: parent must register the byte-persistence implementation in native Matching's persistence selection and ensure the existing Cassandra-backed native configuration selects it. Verify compilation and tests through the parent-controlled GitHub Actions workflow. The current read path uses the Scylla driver's UTF-8 validated `String` representation for CQL text and copies its bytes immediately; Cassandra itself rejects invalid UTF-8 text. A stronger raw text row decoder may be needed if the driver exposes a raw text representation. Conditional batch conflict parsing must also be exercised against Cassandra LWT response layouts; no executor seam or live-server result is available in this module yet.
