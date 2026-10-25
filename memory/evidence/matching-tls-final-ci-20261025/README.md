# TLS candidate workspace CI evidence — run 40159753424

Run: https://github.com/auenkr/cadence-rust/actions/runs/40159753424  
Workflow: `rust-ci`, `workflow_dispatch`, completed `success`  
Branch: `t3/matching-tls-handshake-parity`  
Exact tested source: `ad997b07f57a435b26e56c1e5a1c2c9e89ceeede`

GitHub `gh run view` reports the run head SHA above and a successful `clippy-test`
job. The runner selected `/home/runner/work/_temp/cargo-target-final`; the
workspace-cache step was skipped. The job step list shows formatting checks,
Clippy, Formatting, then Test, all with successful step conclusions. Artifact
exit markers provide the command-level results:

| Evidence | Exit |
| --- | ---: |
| `clippy.exit` | 0 |
| `test.exit` | 0 |
| `fmt.exit` (workflow's focused Unleash rustfmt step) | 0 |
| `workspace-format.exit` (advisory workspace-wide check) | 1 |

The test log includes
`grpc_matching::durable_tests::missing_rpc_identity_starts_tls_listener_and_rejects_tls_and_plaintext_without_stopping ... ok`.
The metrics artifact's runner-built oracle SHA256 is
`19528dfab41620f88ff95a0f7bda1d999d494411e9265d9f43ca7f2c3db34bad`.
This is the metrics oracle binary digest, not the Matching server binary.

Compact extracted source/target identity, exit markers, clippy summary, test
lines, and oracle hash are stored beside this README. Original artifacts were
downloaded to `/mnt/storage/service/ci-evidence/run-40159753424/` (outside
`/tmp`); artifact ZIP SHA256 values:

- `rust-ci-logs.zip`: `9caeeb23fa2e7c611170d63ab013c7da7d52685fe95583b38a5a639f3004d9b5`
- `metrics-evidence.zip`: `0d17d5a68055f799c308d9dd91b0ee2ed2645de07ae11b2a4c5f0dfe5cb62372`

## Review status and limits

This is valid green CI evidence for the named source only; it is not merge
approval. Final code review found a blocking invalid-DER PEM CA parsing issue
in `ad997b07f`; the implementation worker is fixing it. The source-only final
review is still active. Do not integrate this candidate. After the fix lands,
run fresh source-pinned CI and complete source review on the new commit.

Formatting scope needs care: the workflow's `fmt.exit=0` step formats two
Unleash files. Its separate “Check changed Matching runtime formatting” step
passed, but its explicit path list does not include the changed
`cadence-service-core/src/grpc_tls.rs` or `grpc_matching.rs`. The workspace
format check exited 1. Therefore this run does not establish formatting
cleanliness for all TLS-touched Rust files. Run an applicable focused check in
CI on the fixed source, and retain the workspace advisory accurately.
