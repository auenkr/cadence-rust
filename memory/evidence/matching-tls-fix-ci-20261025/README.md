# TLS invalid-DER fix candidate CI — run 40161660335

Run: https://github.com/auenkr/cadence-rust/actions/runs/40161660335  
Workflow: `rust-ci`, `workflow_dispatch`, conclusion `success`  
Branch: `t3/matching-tls-handshake-parity`  
Exact head: `4b8a6060b341ace8987e73f25e0d146429410950`

GitHub run metadata confirms the head SHA and successful `clippy-test` job.
The job selected the fresh target
`/home/runner/work/_temp/cargo-target-final`; the Rust cache step was skipped.
Step order was formatting checks, Clippy, Formatting, then Test. Artifact
markers confirm:

| Evidence | Exit | Meaning |
| --- | ---: | --- |
| `clippy.exit` | 0 | Workspace strict Clippy passed |
| `test.exit` | 0 | Workspace tests passed |
| `tls-format.exit` | 1 | TLS-touched-file formatting check failed |
| `workspace-format.exit` | 1 | Advisory workspace-wide formatting check failed |
| `fmt.exit` | 0 | Separate workflow Formatting step passed |

Do not report this as an all-format-clean run. The two requested formatting
checks both have nonzero exit markers even though their workflow steps/job
continued successfully. The fresh-target, exit, and compact regression-test
evidence is preserved beside this README. Original artifacts are stored at
`/mnt/storage/service/ci-evidence/run-40161660335/`, outside `/tmp`; ZIP SHA256:

- `rust-ci-logs.zip`: `eb0562034668919f235ace2f3c58d6922be749b038eb490ce453b2f2ce0bbc1c`
- `metrics-evidence.zip`: `d6b4f4b918509d6b9bac3310db08fcc2a8e9ce56764c54ae5c076e0c8eddb27a`

The test log shows these passing:

- `grpc_matching::durable_tests::missing_rpc_identity_starts_tls_listener_and_rejects_tls_and_plaintext_without_stopping`
- `grpc_tls::tests::server_ca_and_client_auth_matrix_matches_go_startup_and_handshakes`

The second test includes the invalid-DER-only CA and mixed valid/invalid PEM
CA regression cases added by `d7b6f77eb` (`match Go filtering of invalid TLS
CA certificates`). The metrics artifact records the SHA256 of the metrics
oracle binary, not a Matching server binary.

## Review and integration status

This is successful CI evidence for the exact candidate source, not integration
approval. It includes the invalid-DER fix that addresses the blocker found on
`ad997b07f`, subject to the independent final code and formatting review still
in progress. Both TLS-touched-file formatting and workspace formatting exited
1 and must be resolved or dispositioned by that review. Do not merge or start
the serial mixed validation until approval; after approval, use the source
pinning and lane checklist in
`memory/matching-post-tls-final-validation-plan-20261025.md`.
