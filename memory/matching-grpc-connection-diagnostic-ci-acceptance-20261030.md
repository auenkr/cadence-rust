# Matching gRPC transport diagnostic acceptance checkpoint (2026-10-30)

## Candidate and source review

- Candidate source: `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
- Candidate branch: `t3/matching-grpc-connection-diagnostic-565c8fc`.
- The remote branch's docs head was `00c00fef144cbbfc827d4222c5390520bbec4682`, whose parent is the candidate source SHA above.
- This candidate is descended from queued-activity recovery commit `565c8fc91893fae1a81cd8e47f10f2ee4744c046`, itself integrated from the `5ee69b5572688c2dba2245c7b411c7182c5b9b16` lineage. Keep the recovery assertions and this lineage intact.
- The direct Matching gRPC readiness diagnostic in `tests/matching-service-compat.cjs` captures grpc-js channel state plus an independent TCP connect bounded to one second, target/resolver mode, present proxy variable names (not values), `NO_PROXY` presence, and credential mode. It does not alter readiness thresholds or acceptance assertions. Existing idle and queued-activity owner-crash/restart checks remain in the test.
- The previous mixed-gate failure `41056791529` remains base evidence only: configured and bound Rust listeners were both `127.0.0.1:13898`; the grpc-js `Waiting for LB pick` timeout was a channel timeout; a handler trace reached `engine-complete` but was not correlated with that RPC. Those records establish no production defect.

## Exact-candidate workspace CI

Run: <https://github.com/auenkr/cadence-rust/actions/runs/41059165266>

- Workflow `rust-ci`, event `workflow_dispatch`, branch `t3/matching-grpc-connection-diagnostic-565c8fc`.
- Terminal conclusion: `success`; head SHA: `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
- Downloaded `rust-ci-logs` artifact records the same commit SHA in `rust/commit.txt`, target `/home/runner/work/_temp/cargo-target-final` in `rust/target-dir.txt`, `clippy.exit=0`, and `test.exit=0`. The job log shows `PKG:` empty for both commands, so the selector expanded to `--workspace`. The CI workflow skips rust-cache for its clean-target option; the target was a fresh hosted-runner temp directory.
- Clippy completed before workspace tests. The workflow steps and artifact exits both show successful Clippy and tests. Workspace formatting artifact `workspace-format.exit` is `1` and scoped query/TLS format artifacts are `1`; those checks are explicitly non-blocking in the current workflow. This does not change the Clippy/test evidence.
- Metrics artifact records candidate head SHA `6c73d9aa447171d4eeda37d71dd0a955d0996a9b` and Rust oracle binary SHA256 `fb7222e35ac7d5e8509c3c261379ab1d03fca2b5788c78d85a7e8a60b4735689`.

## PostgreSQL mixed-gate dispatch

- GitHub rejected `workflow_dispatch --ref 6c73d9aa447171d4eeda37d71dd0a955d0996a9b` with HTTP 422 (`No ref found`), because dispatch requires a branch or tag ref.
- Created and pushed annotated pin `validation/matching-grpc-diagnostic-6c73d9aa`, targeting exactly `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
- Before dispatch, queried active runs of `Matching production validation`; the result was empty.
- Dispatched `matching-production-validation.yml` with `lane=postgres` from that pin. Run: <https://github.com/auenkr/cadence-rust/actions/runs/41094318642>.
- At inspection, run head was exactly `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`, ref `validation/matching-grpc-diagnostic-6c73d9aa`, status `in_progress`; setup-go was running and build/gate steps were pending. No PostgreSQL acceptance result, Rust service binary SHA, or nested gate exit is claimed yet.
- Next action: once this run is terminal, retrieve its `matching-production-postgres` artifact and verify `commit.txt`, `binary.sha256`, `build.exit`, `gate.exit`, nested probe/lane/run exits, SDK/readiness/restart/recovery assertions, and `transportDiagnostic`. Record the run URL, exact SHA, binary hash, exits, and diagnostic object. Treat a connection cause as undetermined unless the artifact distinguishes it.

