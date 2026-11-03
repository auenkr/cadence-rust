# Frontend archival: endpoint tests + mixed gate (2026-11-03)

Branch `frontend-archival-gate`.

## Code changes
- gRPC `ListArchivedWorkflowExecutions` (`cadence-matching/src/grpc.rs`) now routes through the installed WorkflowApi (new `hook_operation` entry `/api/v1/workflows/archived`); Thrift already reached the Handler via `dispatch_thrift` (the `yarpc.rs` stub is only the no-hook legacy fallback).
- Rust bugs found by the gate and fixed: archived visibility `closeStatus` was emitted as a string (typed response expects i32 -> "invalid type: string TERMINATED"); empty `nextPageToken` was `null` (typed response expects string) which failed the last page. Both fixed in `archive_readers.rs`.
- Handler tests with real filestore readers: `handler_lists_archived_visibility_through_real_filestore_reader`, `handler_serves_archived_history_through_real_filestore_reader`; yarpc test `archived_visibility_dispatch_reaches_installed_workflow_api`.

## Gate (tests/gates/run-frontend-gate.py archival phase)
Registers an archival-enabled domain (file:// URIs) via Rust Frontend (TChannel), DescribeDomain over TChannel+gRPC shows ENABLED + URIs, starts+terminates 2 workflows, polls `workflow listarchived` over TChannel and gRPC until both appear (Go History/Worker wrote the visibility archive), pages with `--ps 1`, rejects URI mutation with "Cannot update existing archival URI", disables history archival (describe shows DISABLED, URI kept), and an archival-disabled domain returns "Domain is not configured for visibility archival".
Not feasible: archived-only GetWorkflowExecutionHistory (Go archives history at retention expiry).

## Evidence
- Mixed gate (mysql, kafka=false) https://github.com/auenkr/cadence-rust/actions/runs/41555187877 on head 8312300273978965e23d7b09d59126f1cf86f6d9: success, `GATE SUMMARY failed: []`. Rust binary sha256 f49fa107c8928fcd42802951224e120d36a0c1eade98a7c46beb605d1ad62c2d.
- Earlier gate runs 41551474389 (list closeStatus bug), 41553352111 (null token bug) found the two Rust bugs.
- Final rust-ci (fmt, clippy, workspace tests) https://github.com/auenkr/cadence-rust/actions/runs/41556347721 on head affecd4d8a032af466a77b259ff6c94e56becc68: success (head differs from the gate head only by a unit-test method-name fix).
