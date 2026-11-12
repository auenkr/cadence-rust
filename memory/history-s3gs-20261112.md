# history-s3gs (2026-11-12)

Branch `history-s3gs`: Go `common/archiver/s3store` and `common/archiver/gcloud` write path (history + visibility Archive) ported; History inline archival now dispatches on the domain URI scheme.

## Ported
- `cadence-common/src/cloud_archiver/write.rs`: `s3_archive_history` (HeadObject skip-if-exists, NoSuchBucket -> BadRequest, 2MB blob split, `HistoryBlob{header,body}` JSON), `s3_archive_visibility` (4 index keys), `gcs_archive_history` (body-only, part 0, `Exist` quirk kept), `gcs_archive_visibility` (close + start records, filename hashes). Unit tests with a mock `ObjectStore` (Go test cases: split, roundtrip via `*_get_history`, existing key not overwritten, error mapping, visibility keys, GCS filename).
- `ObjectStore` trait gained `put_object` / `head_object`; real HTTP implementations (SigV4 PUT/HEAD for S3, upload API for GCS) in `cadence-service-core/src/cloud_store.rs` (moved from `cadence-frontend`, re-exported there so Frontend is unchanged).
- `ArchiverProvider::archive_history/archive_visibility` scheme dispatch (file / s3 / s3-ap / gs; other -> error); `archiver_provider_from_yaml` builds the provider from the service `archival` config; wired History launcher -> `HistoryEnvParams` -> `FileInlineArchivers`.

## Gaps
- Go metrics not ported; `NonRetriableError` feature-catalog substitution not ported; ProgressManager / iterator state not ported (inline archival has no progress, part always 0, as in Go).
- GCS inline quirk (`Exist` checks bare filename, never skips) kept bug-compatible.
- Blob chunking approximates Go's iterator size via JSON-size estimate.
- No HTTP-level tests for `put_object`/`head_object` yet; no real S3/GCS endpoint exercised (gates use file://).

## Evidence
- rust-ci (pre-merge head) https://github.com/auenkr/cadence-rust/actions/runs/43170043525 : success
- Final head 61bc4c0602ed0089200031f81f3da8572bb08eee (master merged):
  - rust-ci https://github.com/auenkr/cadence-rust/actions/runs/43171709207 : success
  - gate mysql/grpc/true https://github.com/auenkr/cadence-rust/actions/runs/43171701389 : success, GATE SUMMARY failed: []
  - gate postgres/tchannel/false https://github.com/auenkr/cadence-rust/actions/runs/43171705232 : success, GATE SUMMARY failed: []

## Post-merge master validation (master eeec9044f, 2026-11-12)
- Full workspace rust-ci + all 12 history-mixed-gate combos: runs 43173389214..43173438803: 12 exit 0; rust-ci 43173389214 watch exited 1 (possibly GitHub API rate limit) - result UNVERIFIED, recheck.
