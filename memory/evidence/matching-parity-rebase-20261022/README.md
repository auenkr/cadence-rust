# Matching parity rebase evidence

Source: origin/matching-go-parity-fixes-20261022 at a478cd639, including implementation da1c307b2.
Base: origin/t3/matching-integration-20261021 at f17c25d95ccefc74b0c24b3d90bcdbc7e7801f88.
Rebased implementation: 70fac99cc; rebased documentation: 86f896f77.
Branch: matching-parity-fixes-rebased-20261022.

Only conflict: CancelOutstandingPoll in grpc_matching.rs. Retained integration's Go-compatible task-list type normalization (including removal of strict 1/2 validation), and used the same persistence_task_list_type conversion for the parity fix's durable_queue owner/load check. All other parity changes applied automatically; dispatch/writer files exactly match source. No unresolved conflicts.

Validation ran in tmux session cadence-rust. All cargo commands used flock /tmp/cargo.lock and -j2. Only cadence-server was touched.

- Initial check: exit 101, missing go/idls. git submodule update --init go/idls exited 0, initializing pinned bfbf273b41b9e36ce21e47a49600d22d73e4614c.
- Check rerun: exit 0.
- Clippy with --all-targets -- -D warnings: exit 101. Existing cadence-persistence/src/matching_domain_metadata.rs:221 drop_non_drop warning. No persistence code changed here; this remains a validation blocker.
- Tests: exit 0. 471 passed, 0 failed, 4 ignored across test binaries. External Go interop gates remained ignored; no mixed-cluster completion claim.

Exact commands, actual exit codes, and output are in accompanying .md and .log files. Successful check and tests used HEAD 86f896f7782c0613b214e0325713f0c45a4838ab. Final fetch after tests confirmed integration remains f17c25d95, so no further rebase was needed. Root checkout was not modified.
