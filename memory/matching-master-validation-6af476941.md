# Matching validation evidence for master `6af476941031193317fdd6c44070e9785e6384b9`

Inspected 2026-10-25. Every run below is a completed GitHub Actions run on `master` at the exact target SHA. Run links are authoritative and include runner logs/artifacts. No validation was dispatched during this review.

## Rust CI

- [rust-ci 40195154984](https://github.com/auenkr/cadence-rust/actions/runs/40195154984): success; whole workspace (`PKG` empty), ordinary push event, shared/cache-eligible target at `/home/runner/work/cadence-rust/cadence-rust/rust/target`. This is **not** the required fresh uncached final run (`clean_target` was skipped). Clippy exit 0, workspace test exit 0, separate OpenFeature rustfmt check exit 0. The uploaded logs show `workspace-format.exit=1`, `tls-format.exit=1`, and `query-format.exit=1`; these checks were allowed to fail and the workspace format log has diffs. The job's green conclusion does not establish formatting parity. Matching emission-oracle comparison completed successfully for default/both/name-on/name-off. Its Rust example binary SHA256 is `38e3d7daffe35bb86a74e14d9fa3937ad43ba87ecdf8a98ee3947c773572fbf4` (not the cadence-matching service binary hash). The original Go malformed DescribeTaskList oracle artifact exit is 0.

## Mixed Go/Rust gates

All listed gates built Rust from a fresh run-specific `CARGO_TARGET_DIR`; workflow scripts record binary hash, build exit, and gate exit to their uploaded artifact. The GitHub log proves those files were generated but does not print their contents. Large gate artifacts (33–53 MB) were not downloaded in this review because the temporary filesystem was nearly full. Thus service-binary SHA256 values and exact exit-file contents remain to be copied from artifacts.

| Run | Scenario | Conclusion | Evidence scope |
|---|---|---|---|
| [40204240682](https://github.com/auenkr/cadence-rust/actions/runs/40204240682) | PostgreSQL | success | Go services with Rust Matching; PG GC-domain, query standby, list partitions, durable restart, auto-config hint; Matching restart/failure recovery enabled |
| [40204660498](https://github.com/auenkr/cadence-rust/actions/runs/40204660498) | MySQL, two Rust Matching nodes | success | MySQL lane; default restart/failure recovery; verifies two-node behavior |
| [40195314790](https://github.com/auenkr/cadence-rust/actions/runs/40195314790) | MySQL probes | success | backlog forwarding, query standby, list partitions, durable restart, auto-config hint |
| [40195809764](https://github.com/auenkr/cadence-rust/actions/runs/40195809764) | Thrift | success | Matching Thrift + History TChannel; auto-config hint |
| [40196298460](https://github.com/auenkr/cadence-rust/actions/runs/40196298460) | Cassandra | success | Cassandra mixed acceptance with Go services and Rust Matching |
| [40196794076](https://github.com/auenkr/cadence-rust/actions/runs/40196794076) | Active offer | success | Active offer mixed gate |
| [40197288308](https://github.com/auenkr/cadence-rust/actions/runs/40197288308) | HTTP TLS interoperability | success | Rust `cadence-server` binary, Go HTTP/TLS interop |
| [40197629016](https://github.com/auenkr/cadence-rust/actions/runs/40197629016) | Configstore | success | Matching configstore mixed gate |
| [40205093375](https://github.com/auenkr/cadence-rust/actions/runs/40205093375) | Shard distributor | failure | Failed in Go baseline probe (`RuntimeError: Go baseline probe failed`) before Rust Matching acceptance; not evidence of Rust failure or pass. |
| [40198127804](https://github.com/auenkr/cadence-rust/actions/runs/40198127804) | Shard distributor | failure | Same Go baseline probe failure, before Rust Matching acceptance. |

## Remaining evidence gaps

1. Run the required fresh, uncached whole-workspace Rust CI on this SHA, with clippy then workspace tests; preserve and report formatting results. Current CI was cache eligible and failed the workspace, TLS-file, and query-file format checks despite green aggregate status.
2. Retrieve small selected files (`binary.sha256`, `build.exit`, `gate.exit`, command/commit/target identity and relevant logs) from each green mixed-gate artifact, and verify values match this SHA. Do not infer the binary hash from the container image digest in logs.
3. Diagnose the shard-distributor Go-baseline failure independently; rerun that lane only after baseline is understood. No accepted shard-distributor evidence exists at present.
4. Resolve the Go oracle artifact's exit-file result and any mismatch with the green job step status.

These runs are substantial compatibility evidence, but they do not establish Matching completion or authorize moving to History. The parity audit and all required failure/restart/recovery evidence remain to be completed and recorded.
