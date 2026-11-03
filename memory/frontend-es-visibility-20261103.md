# Frontend advanced visibility (Elasticsearch) — 2026-11-03

Branch `frontend-es-visibility`. Ported (read path): List/Scan/CountWorkflowExecutions, ListOpen/ListClosed (+ByType/ByID/ByStatus),
GetSearchAttributes, query validator, SQL->DSL translation, page tokens (from/size, search_after, scroll), and the
StartWorkflow/SignalWithStart search-attribute validator (Go messages and limits).
Code: `cadence-frontend/src/{es_query,es_visibility,search_attr_validator,workflow_visibility}.rs`, `cadence-service-core/src/es_mapping.rs`.

## Evidence
- rust-ci `-f package=cadence-frontend` (fmt, clippy, tests incl. fake-ES unit tests): https://github.com/auenkr/cadence-rust/actions/runs/41602845094 — success (head after CI rustfmt patch).
- Mixed gate `frontend-mixed-gate.yml -f database=mysql -f kafka=true -f es=true -f rust_ref=frontend-es-visibility`:
  https://github.com/auenkr/cadence-rust/actions/runs/41610924645 — success, head cad6adf6757295717bf55c0c741d6aa9b8d6bc79, `GATE SUMMARY failed: []`, 169 ok steps.
  Rust binary sha256 `6a2a7d76691358f892c03e9c00c87395740453a768aaab21fc4c2fb530b2d539`.
- Topology: Go History+Worker (indexer, Kafka -> ES), Rust Matching, Rust Frontend, MySQL, Kafka 3.8, Elasticsearch 7.17.10 (limited container), Go's own index template.
- Proven by SDK steps (`tests/frontend-gate/es.go`): start with search attributes via Go SDK; List by attribute + CloseStatus; order by + paging (pageSize 1);
  Count; Scan (scroll); ListClosed by workflow id; GetSearchAttributes; invalid key / value rejected on Start; invalid attribute / syntax errors on List;
  `admin cluster add-search-attr` (gRPC) creates the field in real ES (`es-mapping-has-added-attribute`) and index has documents.

## Bugs found by the gate and fixed
1. gRPC admin AddSearchAttribute used proto enum values as thrift values (Keyword stored as Int, mapped `long`). Fixed with `v - 1`.
2. `is_system_indexed_key` wrongly included CadenceSchedule* keys (Go excludes them) -> scheduler StartWorkflow rejected, and DSL missed the `Attr.` prefix.

## Known gaps / notes
- Go per-domain `system.readVisibilityStoreName` routing not ported: ES is used whenever `advancedVisibilityStore` is configured.
- Go `not like` regex quirk, sqlparser syntax-error texts beyond "Invalid query." / "invalid where clause", and search_after overflow clamps only partly matched.
- A key added by AddSearchAttribute is mapped in ES, but the Go indexer drops values for keys missing from the Go worker's own dynamic config ("Unregistered field") — Go behaviour; the gate does not assert that value.
- Not gated: Postgres/Cassandra+ES lanes, OpenSearch (v6/os2), ES restart/recovery.
