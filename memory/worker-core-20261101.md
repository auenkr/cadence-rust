# Worker core — 2026-11-01

## Diagnostics pure logic
Ported in `diagnostics.rs` (Go-derived tests): runbook links, root-cause strings, `getIssueType`,
`capNumberOfIssues`, starter/workflow input wire names, usage-data Pinot payload (sorted keys).
Not ported: workflow bodies, activities needing frontend/messaging clients, invariants (timeout, failure, retry, timeoutrisk, antipatterns).
CI: https://github.com/auenkr/cadence-rust/actions/runs/41286336454 — success at `c34015c62`.
