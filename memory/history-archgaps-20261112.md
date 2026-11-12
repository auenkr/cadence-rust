# history-archgaps (2026-11-12)

Branch `history-archgaps` closes the gaps listed in `history-s3gs-20261112.md`.

## Ported (go/common/archiver/{s3store,gcloud})
- `cadence-common/src/cloud_archiver/write.rs`:
  - `ArchiverMetrics` trait plus `ArchiveOptions` (metrics, `non_retriable_error`, `progress_manager`). All Go metric names for the History/Visibility archiver scopes are emitted: latency, success, transient and non-retryable error counters, blob exists, blob size, total upload size, history size, duplicate archivals and the histograms.
  - Retryability classification as in Go. S3: internal read error or retryable store error is transient. GCS history: anything other than `errUploadNonRetriable` is transient. GCS visibility: transient unless the error is bucket-not-found, scheme mismatch or invalid URI.
  - `NonRetriableError` substitution of non-retryable errors.
  - `HistoryIterator` follows Go `historyIterator`: JSON size estimator, 2MB target, `IteratorState{NextEventID,FinishedIteration}` encode/decode, resume from state. `ProgressManager` protocol: load, fall back to a fresh iterator on bad state, record after each blob. S3 uses `BatchIdx`; GCS advances the part (`CurrentPageNumber`) only when a ProgressManager is set.
  - Kept Go quirks: GCS part 0 without a ProgressManager, `Exist` on the bare filename, the GCS transient upload error counted twice, GCS visibility metrics emitted on `HistoryArchiverScope`.
- `ArchiverProvider::with_archive_options`. History `env.rs` builds the provider with `ArchiverMetricsAdapter` (in `resources/metrics.rs`) over the `MetricsRegistry`, so the archiver metrics land in the History registry.
- HTTP tests in `cadence-service-core/src/cloud_store.rs` against an in-process TcpListener mock: S3 PUT/HEAD (path-style paths, percent-encoding, SigV4 `Authorization` header, signed headers, `x-amz-content-sha256`, body, NoSuchBucket and 503 mapping) and GCS upload POST/HEAD (`/upload/storage/v1/b/<b>/o?uploadType=media&name=`, content-type, body, NoSuchBucket mapping).
- Unit tests for the iterator (split, state, resume), metrics, substitution, progress and parts.

## Remaining gaps
- `HistoryBatchSource` still reads the whole history up front (`read_all`), so the iterator slices in memory. Blob boundaries match Go's, but the page-by-page persistence reads and mid-iteration EntityNotExists are not modelled.
- The blob size estimate serialises Rust `History` JSON, which is not guaranteed byte-identical to Go's `json.Marshal(types.History)`.
- Inline archival from History passes no ProgressManager and no NonRetriableError (as Go inline), so those paths are only unit-tested.
- No real S3/GCS endpoint exercised.

## Evidence
Final code head `5d435c05a33a3473117c8e74e61423b6ee32d8ba` (origin/master merged, includes history-p2stubs):
- rust-ci https://github.com/auenkr/cadence-rust/actions/runs/43176947189 : success
- gate mysql/grpc/true https://github.com/auenkr/cadence-rust/actions/runs/43176957502 : success, `GATE SUMMARY failed: []`
- gate postgres/tchannel/false https://github.com/auenkr/cadence-rust/actions/runs/43176952046 : success, `GATE SUMMARY failed: []`
(Earlier intermediate runs failed on compile/clippy and were fixed.)

## Post-merge master validation (master eac60f24f, 2026-11-12)
- Full workspace rust-ci + all 12 history-mixed-gate combos: runs 43178625674..43178680042, all completed success.
