# Cassandra 2.x CQL metadata fallback

## Go compatibility contract

The pinned Go driver is `github.com/gocql/gocql` at
`v0.0.0-20211015133455-b225f9b53fa1` (local module source:
`/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/metadata.go`).
Its schema compatibility path uses:

- `system.schema_keyspaces(durable_writes, strategy_class, strategy_options)`;
  `strategy_options` is JSON and the class must be combined with it as the
  modern replication map's `class` entry.
- `system.schema_columnfamilies` for table names and validators including
  `key_validator`, `comparator`, and `default_validator`.
- `system.schema_columns` for column names, `component_index`, `validator`,
  and (with protocol v2+) `type`.
- V1 compatibility derives partition/clustering keys from key validators,
  comparators, key aliases, and column aliases because `type` is unavailable.

Go oracle run `40411377210` established that Cassandra 2.1.22 and 2.2 are
supported backends; the CQL oracle also covered Cassandra 3.11.17.

## Rust work in this change

`rust/vendor/scylla-1.9.0/src/cluster/metadata/fetching.rs` now has pure helpers
for decoding legacy keyspace replication options into the existing strategy
decoder and mapping scalar legacy marshal validator names to CQL types. Unit
tests cover NetworkTopologyStrategy option decoding, representative scalar
validators, and unknown validator rejection.

## Completed in the keyspace fallback increment

`ControlConnection::query_keyspaces` now first reads the modern
`system_schema.keyspaces` rows. If and only if that query yields the driver's
existing missing-table/column `DbError::Invalid` shape, it retries using
`system.schema_keyspaces` and converts `strategy_class` plus JSON
`strategy_options` into the modern replication map. It retains
`durable_writes`, and the converted values pass through the existing common
strategy decoder. Errors unrelated to missing modern schema are returned
unchanged. `keyspace_metadata_query` and the row conversion have direct unit
coverage in the vendored scylla crate.

CI now checks rustfmt on this file and explicitly runs the vendored scylla
library's metadata fetching tests. This prevents a package-filtered
`cadence-persistence` workflow from being mistaken for vendored driver test
coverage.

## Remaining live reader integration

The keyspace fetch falls back from `system_schema.keyspaces` to
`system.schema_keyspaces` only for the recognized missing-table/column
`DbError::Invalid` response. Peer discovery in `system.peers` / `system.local`
is unchanged.

The subsequent schema branch also calls
`query_user_defined_types`, `query_tables_schema`, `query_tables`, and
`query_views`, all modern `system_schema` readers. Those callsites need legacy
reader counterparts for `system.schema_usertypes` (where available),
`system.schema_columnfamilies`, and `system.schema_columns`. Legacy readers
must assemble keyspace-scoped table schemas, convert validators to CQL types,
preserve partition/clustering key positions, derive reversed clustering order,
and retain token-aware partition keys. Cassandra 2.x has no materialized
views, so its view result can be empty. Do not suppress metadata errors or
fall back to an empty keyspace/table map.

This increment now propagates the detected legacy mode into table-name and
partitioner metadata. `system.schema_columnfamilies` supplies table names in
both full and minimal paths; Cassandra 2.x has no per-table partitioner, so
minimal metadata uses `None` while peer token discovery remains untouched.
Views return empty only after legacy keyspace mode has been positively
detected. The focused test covers the legacy table query selection and
representative marshal validator conversion.

Full schema mode remains incomplete. `query_user_defined_types` still uses
`system_schema.types`, and `query_tables_schema` still reads
`system_schema.columns`; Cassandra 2.x will fail those modern readers before
column metadata can be published. The remaining implementation needs
`system.schema_usertypes`, `system.schema_columns`, and `schema_columnfamilies`
validator/key/comparator decoding, including frozen nested collection and UDT
conversion. Do not claim full Cassandra 2.x metadata parity yet. No local
Cargo/Go/rustfmt commands were used.

## CI evidence

- Commit: `bd07d81254e5fee01a3b0493b30232098cc9355c`.
- Push run: https://github.com/auenkr/cadence-rust/actions/runs/40412443565
  (matching head SHA, event `push`, conclusion `failure`, zero jobs). The
  workflow push filter only covers `master` and `matching-*`, so this assigned
  `t3/legacy-keyspace-fallback` branch got no validation jobs.
- Dispatch for this ref was rejected by GitHub with HTTP 422: the workflow at
  the default branch does not have a `workflow_dispatch` trigger.
- Earlier helper run `40411987955` belongs to `03171a099` and failed. It is
  retained as prior evidence, not attributed to this implementation.

The focused CI run has not executed; do not treat this increment as validated.

## Current metadata-reader increment

- Code is pushed to `t3/legacy-schema-metadata-1` and
  `matching-legacy-schema-metadata`.
- The Rust CI workflow currently has dispatch support only in the feature
  branch file, not in the default branch workflow definition. A dispatch for
  this ref was rejected with HTTP 422 (`Workflow does not have
  'workflow_dispatch' trigger`). Push runs observed for the `t3/*` branch
  ended with zero jobs and are not validation evidence. The attempted
  `matching-*` push did not produce a run in the workflow run list.
- No Cargo, Go, or rustfmt command was run locally. No successful CI result is
  available for this increment.
