# History ES / advanced visibility gate (2026-11-13) — RESULT: FAIL, real gap found

Branch `history-esgate` (from master e4e463056). Added: dispatch-only `.github/workflows/history-es-visibility.yml`
(Rust History+Frontend+Matching, Go Worker with Kafka->ES indexer, MySQL, Kafka 3.8 512m heap, ES 7.17 / OpenSearch 2 with
512m heap and `--memory 1800m` from lib-es.sh (limit 1800m not 1200m: reused the existing helper); `es_flavor` and `two_history` inputs)
and new SDK steps in `tests/frontend-gate/es.go`: `UpsertSAWorkflow` (UpsertSearchAttributes twice, then close) with
`es-upsert-list/count/scan/old-value-gone` assertions on top of the existing list/order/paging/count/scan/ListClosed steps.
Note: the workflow file is not on master yet, so `workflow_dispatch` returned 404; the run used a temporary push trigger
(removed in the final commit). After merge to master use `gh workflow run history-es-visibility.yml`.

## Run
- https://github.com/auenkr/cadence-rust/actions/runs/43445554635 — head 74bd5bce73313b3554a9768813d40c3b242157e9 — conclusion **failure** (~45 min, inside the 85 min limit).
- `GATE SUMMARY failed: sdk-sched-backfill-completed, sdk-list-open, sdk-list-closed, sdk-driver-exit, sdk-es-driver-timeout,
  sdk-es-list-closed-by-attribute, sdk-es-list-order-by-and-paging, sdk-es-count, sdk-es-driver-exit, es-index-has-documents, es-restart-data-retained`.
- `es-index-has-documents` false: ES index stayed empty, so every ES read returned 0 results until the 4-minute poll expired.

## Root cause (code evidence, not yet fixed)
Rust History has no advanced-visibility writer. `rust/crates/cadence-history/src/wiring/persistence/visibility.rs` only has
`SqlVisibilityWriter` and `CassandraVisibilityWriter`; `upsert()` returns `VISIBILITY_OPERATION_NOT_SUPPORTED`; there is no Kafka
producer in cadence-history (`rskafka` is used only by cadence-frontend async queue). Go equivalent to port:
`go/common/persistence/visibility_*` dual/ES-Kafka manager (`ES visibility store` producer path: `go/common/persistence/elasticsearch/es_visibility_store.go`
write side via `go/common/messaging/kafka/producer_impl.go`, message `indexer.Message` with Record/Upsert/Delete types), selected when
`persistence.advancedVisibilityStore` is set (History already reads that flag for task generation, `launcher.rs:310`).
The Rust side generates the visibility transfer/search-attr tasks, but the executor's write goes to the SQL store only.

## Not impossible, just not done
Porting the History-side ES visibility producer (Kafka producer + indexer message schema + upsert/delete + DLQ topic naming)
is a separate Rust port task; it was out of scope for "minimal harness + visibility fixes". Once it lands, re-dispatch this
workflow (flavor elasticsearch, then opensearch, optionally two_history=true). The harness side is ready; the new steps are unverified until then.
The frontend ES read path was already proven with Go History (memory/frontend-es-visibility-20261103.md).
