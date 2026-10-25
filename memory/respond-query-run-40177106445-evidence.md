# Query branch run 40177106445 evidence (2026-10-25)

Branch `t3/respond-query-task-parity` @ `78006b797e658d3169828e8cab8f481c3894e2b4`.
Run: https://github.com/auenkr/cadence-rust/actions/runs/40177106445 (conclusion success, ubuntu-latest).
Artifact `rust-ci-logs` downloaded to `/mnt/storage/service/ci-evidence/40177106445/logs`
(commit.txt confirms the SHA). Actual exit files:

| file | exit |
|---|---|
| clippy.exit | 0 |
| test.exit | 0 |
| query-format.exit | 0 (query-format.log empty) |
| tls-format.exit | 0 |
| fmt.exit | 0 |
| workspace-format.exit | **1** (non-failing step; format.log 650100 bytes) |

Workspace-format drift is pre-existing, repo-wide, not caused by the query change:
cadence-matching (~397 diffs; src and tests), cadence-persistence (~190),
cadence-service-core (~106), cadence-api (33), cadence-common (12), cadence-proto build.rs,
cadence-server examples/matching_metrics_emission_oracle.rs, cadence-thrift tests/go_fixtures.rs.
The query-touched files (matching_query.rs, matching_registry.rs, grpc_matching.rs, matching_rpc_tests.rs) passed query-format.
No reformat was done or dispatched.

## Source review vs Go
- Nil-result guard: Go `ToRespondQueryTaskCompletedRequest` (go/common/types/mapper/proto/api.go:3182) dereferences `t.Result.ResultType`
  when `t.Request` is non-nil and `Result` nil -> panic, recovered by YARPC. Rust returns `Status::unknown("panic: ... nil pointer dereference")`
  before metrics/side effects. Matches by source; message text and gRPC code Unknown are source-derived, not wire-captured.
- Strong-query empty impl: Go `featureSupported` (go/common/client/versionChecker.go:224-248) has an empty `case consistentQuery:` so
  empty clientImpl falls through to the version/impl lookup and fails (empty version -> ClientVersionNotSupportedError, unknown impl too).
  Rust removal of `client_impl.is_empty() -> true` matches.
- Global QueryIndex (Go `lockableQueryTaskMap`, engine.go deliverQueryResult) was previously approved at 4c202759; the generation-based
  cleanup is a Rust-internal addition; no Go oracle for the replacement race.

## Limitations
No wire capture of the panic status, no mixed Go/Rust lane run on this SHA. Not merged. Open: absent Query payload and malformed domain UUID contracts.
