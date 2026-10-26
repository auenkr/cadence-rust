# Matching Cassandra task UDT protocol oracle — 2026-10-27

## Contract and source evidence

Extended the pinned Go backend oracle to create a task-shaped custom UDT and
table, prepare/insert a row, read it back, compare all six fields, and drop the
table/type. The field layout follows `go/schema/cassandra/cadence/schema.cql`
lines 276–283: `domain_id uuid`, `workflow_id text`, `run_id uuid`,
`schedule_id bigint`, `created_time timestamp`, and
`partition_config map<text,text>`. Protocol v2 expects the exact pinned gocql
`ErrorUDTUnavailable`; v3 and v4 require a complete successful field roundtrip.

Pinned source is
`/home/auenkr/go/pkg/mod/github.com/gocql/gocql@v0.0.0-20211015133455-b225f9b53fa1/marshal.go`:
line 30 defines the exact error text, “UDT are not available on protocols less
than 3, please update config”; lines 155–157 return it when marshalling a
custom `UserType` below protocol 3; lines 259–261 do the same while
unmarshalling. This confirms that protocol v2 task UDT success is not a
compatibility requirement for this Go client.

The Rust example now uses the public `CassandraTaskUdt` for the same six-field
prepared insert/read roundtrip. This is source coverage only. The existing Rust
session configuration rejects protocols 2 and 3 before the example reaches the
UDT path. The Rust comparison gate remains opt-in and was not dispatched; do
not count this edit or Go success as Rust protocol parity.

## Validation and evidence

No local Go, Cargo, Rust, or rustfmt commands were run. `git diff --check` was
clean before commit. The focused Go matrix is dispatched with
`compare_rust=false` on the pushed branch; record its run URL, head SHA, and
conclusion here after completion. The matrix keeps protocol 2 / Cassandra
2.1.22 and protocol 3 / Cassandra 3.11.17 range-delete expectations, adds
protocol 4 / Cassandra 4.1.7 task UDT success, and preserves all scalar queue
CAS/count/batch/error/range assertions.
