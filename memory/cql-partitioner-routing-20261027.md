# CQL partitioner routing (RandomPartitioner on ordinary Cassandra)

Branch t3/cql-partitioner-routing, based on dc84a0690. Scope: vendored scylla 1.9.0 only.

- Gap: ordinary Cassandra has no per-table partitioner column (system_schema.scylla_tables is Scylla-only), so `Table.partitioner` was None and prepared statements defaulted to Murmur3 even on RandomPartitioner clusters.
- Fix (`cluster/metadata/fetching.rs`): new `query_local_partitioner_name()` reads `system.local.partitioner`. Tables/views (legacy, modern and Minimal fetch paths) with no per-table partitioner inherit it; `Session::prepare` then resolves it via the existing `extract_partitioner_name`.
- `query_peers` rejects names `PartitionerName::from_str` cannot route (ByteOrdered, OrderPreserving...) with new `PeersMetadataError::UnsupportedPartitioner`, instead of silently using Murmur3.
- Unit test: `routing::partitioner::tests::partitioner_name_rejects_unsupported_partitioners`.
- Collection codecs and v2/v3 persistence untouched.
- Evidence: NOT compiled or tested (no local Cargo, no CI dispatch per instruction); parent must run rust-ci for the vendored scylla crate and a Cassandra RandomPartitioner gate.
