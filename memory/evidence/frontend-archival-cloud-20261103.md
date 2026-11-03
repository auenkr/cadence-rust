# Frontend archival providers: s3store and gcloud (2026-11-03)

Branch `t3/frontend-archival-providers`, head `038fcc78cdb90d5286d94ef08437f9916a2a8306`.

## Runs on the final head
- Mixed gate (mysql, kafka=false, s3=true), conclusion **success**: https://github.com/auenkr/cadence-rust/actions/runs/41646999287
  - Rust binary SHA256 `bcb606032895dc369bd0c15527fc239398d24e493c4efc657dcdf89ab9d335b9`
  - All `s3-*` steps passed: create-bucket, register rejected for a missing bucket, register rejected for a gs:// URI, register and describe, Go History writes visibility archives to MinIO, `listarchived` through Rust, archived-only history over tchannel CLI and over gRPC CLI, missing run rejected.
- rust-ci (clippy and workspace tests), conclusion **success**: https://github.com/auenkr/cadence-rust/actions/runs/41647004343

## Scope and caveats
- S3: URI parsing, ValidateURI, history read with pagination tokens, visibility query. Proven against a real MinIO (SigV4 verified).
- MinIO is built from Go source in CI (`go install github.com/minio/minio@RELEASE.2024-10-13T13-34-11Z`), because release binaries (410) and registry images are unavailable. `moto_server` is the fallback.
- The archived-only history blob is **hand-built** in Go format, not written by Go. Go archives history only at retention expiry, which a gate run cannot reach.
- GCS (gstorage): unit tests and fake-server tests only, no gate. GCS `QueryWithFilters` never emits a next-page token, as in Go.
- The Go CLI `workflow show` calls DescribeWorkflowExecution after exporting history. That call fails for an archived-only run in Go as well, so the gate judges the exported JSON.

## Bugs found by the gate and fixed
1. `cadence-api` enums only accepted integers. Go archives enums as strings (`"eventType":"WorkflowExecutionStarted"`), so the response failed to deserialize. The Go CLI swallowed the error and exported `[]`. Enums now accept numbers or Go names.
2. Go archives event timestamps in nanoseconds, while Rust HistoryEvent holds milliseconds. gRPC made the Go client panic (`timestamp ... after 10000-01-01`). The archive reader now divides by 1e6.

## Run history
Provisioning and diagnostic runs 41619824271 to 41643624762 (see git log). 41643624762 and 41645252727 failed on the two bugs above.
