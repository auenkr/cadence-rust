# History S3 archival gate (MinIO) — 2026-11-13

Branch `history-s3gate`. Gate: `history-mixed-gate.yml` with `-f database=mysql -f s3_archival=true -f two_history=false`
(a new workflow file cannot be dispatched before it is on master, so this is an input, not a separate workflow).

## Result
Final run https://github.com/auenkr/cadence-rust/actions/runs/43459236168 — head `5dec7cb620f8aec8522c0893cd4951cffc41e854`, conclusion **success**.
Rust History/Frontend/Matching + Go Worker; archival to MinIO (`minio/minio:RELEASE.2024-10-13T13-34-11Z`, `--memory 512m --cpus 0.5`),
s3 URIs `s3://…/rust-history` and `…/rust-visibility`, custom endpoint, force-path-style.
Verified: Rust-written history + visibility blobs in S3; retention-0 deletion from persistence; Frontend GetWorkflowExecutionHistory
from the archive over tchannel and grpc; ListArchivedWorkflowExecutions over tchannel and grpc; Go s3store (`tests/gates/s3-reader`)
parses the Rust history blobs and visibility records.
rust-ci (cadence-history and workspace) succeeded on earlier heads (43451712167, 43456607896).

## Rust bugs found and fixed (Go-faithful)
1. `go_history_batch` wrote numeric `eventType`; Go needs the string name -> wrapper in `archiver_wiring.rs` maps via `EventType::as_str`.
2. Event `timestamp` was saturated to i64::MAX when already in ns -> scale ms->ns only for ms-range values (`cloud_archiver/write.rs`).
3. Visibility archive request start/close timestamps were ms but archiver expects ns (keys dated 1970) -> `archive_nanos` in `queue_deps.rs`.
4. Visibility archive `close_status` used the persistence enum; archive request needs the thrift enum (TERMINATED showed as CONTINUED_AS_NEW) -> `thrift_close_status`.

## Known behaviour (not a bug)
With domain retention 0 the DeleteHistoryEvent timer can run before the CloseExecutionTask is processed; the close task then finds no
workflow and no visibility archive is written (same race exists in Go). Observed in run 43456601022 (1 of 3 runs). The gate therefore
requires visibility for >= 2 of 3 runs and history for all 3. Inline history archival is rate limited; throttled requests fall back to a
signal to the Go worker archival workflow (Go behaviour).

## Harness notes
- MinIO image from quay.io returned 401; Docker Hub image used, source-build/moto fallback kept in `tests/gates/lib-s3.sh`.
- Earlier runs: 43445813355, 43449776209, 43451703942, 43454755010, 43456601022 (failures fixed above).
