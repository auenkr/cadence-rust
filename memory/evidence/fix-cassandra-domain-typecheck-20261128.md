# Fix: Cassandra domain metadata type check (2026-11-28)

Branch `fix-cassandra-domain-typecheck`, head 7844ad18af7618cd851d94fc58c29cf5fff0c8ae.

## Root cause
Upstream Go defines `domain_config.bad_binaries_encoding` with two types:
- `go/schema/cassandra/cadence/schema.cql` (fresh install, used by the gates): `blob`
- `versioned/v0.15/auto_reset.cql` (upgraded clusters): `text`

gocql accepts a Go `string` for both. Fix 10c360159 changed the Rust field from `Vec<u8>` to `String`, which fixed the text clusters and broke fresh-schema clusters. Fix: new `CassandraTextOrBlob` (cassandra.rs) checks the column type, decodes and encodes as text or blob, and the field now uses it. Program logic is unchanged.

## Audit
I checked every column of the domain, domain_config, cluster_replication_config and domain_replication_config UDTs against schema.cql. New tests (`cassandra::schema_type_check_tests`) parse schema.cql and run the scylla `type_check` of each Rust UDT against the schema types. They also cover the v0.15 text variant. Every other `*_encoding` column in schema.cql is `text`, so bad_binaries_encoding is the only column with two types. The shard, execution and task UDTs are not covered by the new tests. The history Cassandra gates below show they work at runtime.

## CI evidence (head 7844ad18a)
| Run | Conclusion | Binary SHA256 |
|---|---|---|
| rust-ci cadence-persistence [46118132776](https://github.com/auenkr/cadence-rust/actions/runs/46118132776) | success (492 passed, incl. 3 new) | - |
| matching-cassandra-mixed-gate [46118135340](https://github.com/auenkr/cadence-rust/actions/runs/46118135340) | success | cadence-matching f8bf44e0e00960918a4284a53827d6fbc3df637ccb1d9b9d71626c68c01d1cf1 |
| frontend-mixed-gate cassandra [46118137831](https://github.com/auenkr/cadence-rust/actions/runs/46118137831) | success | cadence-server 9320ce3f5b8502a96234aedca0f56aaf7db818be59cfad518af5e6ce5693d24d |
| history-mixed-gate cassandra [46118140356](https://github.com/auenkr/cadence-rust/actions/runs/46118140356) | success | cadence-server cf684e1eaf261045d1ecb7d47ed8a94fbe3098470a750a6c5cbabe110f297791, cadence-history a651609e0d7245c400b1775d99345a9a318f9d45cb787dfe4a32752dfe382262 |
| history-restartgate cassandra [46118143011](https://github.com/auenkr/cadence-rust/actions/runs/46118143011) | success | release cadence-server da11a37974ddc6082bb1093968bf4867bb044583a79f023b275550f3a4c23c2b, cadence-history f68425139db166bc335ddab0d2e01e6c8c020407b19f878ac440cfd552b565d7 |
| history-soak cassandra [46118145578](https://github.com/auenkr/cadence-rust/actions/runs/46118145578) | success | same release binaries as restartgate |
