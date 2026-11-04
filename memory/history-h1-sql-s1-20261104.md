# History H1 SQL Slice 1 handoff — 2026-11-04

## CI evidence

- Branch: `history-h1-sql-s1b`
- Tested head: `060ee811f8f16624d7e4a1cb19c8021b60a37802`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41800260897
- Command: `gh workflow run rust-ci.yml --ref history-h1-sql-s1b -f package=cadence-persistence -f clean_target=true`
- Conclusion: `success` (fresh-target cadence-persistence Clippy and tests)
- This run validates the unchanged master code; no implementation changes were made on this branch.

## Findings and exact remaining work

- `history_contract.rs` does not define typed contract records for `ActivityInfo`, `ChildExecutionInfo`, `SignalInfo`, `RequestCancelInfo`, or `TimerInfo`. Those values are currently represented as `DataBlob` collections. `WorkflowExecutionInfo` exists, but is a broader contract object and does not directly correspond to the Go SQL blob struct. The typed bidirectional adapters and round-trip tests requested in step 1 remain entirely unimplemented.
- The Go reference mapper is `go/common/persistence/serialization/thrift_mapper.go` (`workflowExecutionInfoTo/FromThrift`, `activityInfoTo/FromThrift`, and adjacent child/signal/cancel/timer functions). The exact SQL persistence fields and transactional semantics are in `go/common/persistence/sql/sql_execution_store.go`, `sql_execution_store_util.go`, and the mysql/postgres `sqlplugin/*/execution.go` and `workflow_state_maps.go` files.
- Rust has the `GoExecutionStore` interface and row types in `history_contract.rs` / `history_sql.rs`, but neither MySQL nor PostgreSQL implements it. Existing MySQL/PostgreSQL history adapters support History read operations and Matching persistence, not the execution/current row transaction flow.
- Consequently all of Slice 1 remains: create/current transition, read/reconstruction, current read/list, concrete list pagination, exists, concrete/current delete, shard ownership transaction locking, Go-compatible error mapping, and integration tests against both database services in `rust-ci.yml`.
- No SQL integration tests specific to the History execution store were added. CI success is not evidence of Slice 1 behavior; it only confirms the existing `cadence-persistence` crate remains green.

This is an incomplete implementation handoff, not a parity claim. Continue from the typed adapters in step 1 before attempting the execution store. The requested branch was pushed and must not be merged or deleted by this worker.
