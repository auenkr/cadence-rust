# Matching PostgreSQL and workspace validation, 2026-10-30

## Immutable PostgreSQL mixed-gate result

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41056791529
- Workflow: `Matching production validation`; event `workflow_dispatch`.
- Head: `5ee69b5572688c2dba2245c7b411c7182c5b9b16` (`validation/matching-postgres-5ee69b5`). This is the requested immutable base; the tag was not changed.
- Conclusion: failure. Fresh `cadence-matching` build passed (`build.exit=0`), Rust Matching server SHA256 was `d9a76461f356a783d89dd4309e585d773f78f07410c17cb2c385899302f32b50`, and the mixed gate exited 1 (`gate.exit=1`).
- Failure stage: the PostgreSQL wrapper launched the Go baseline probe, which raised `RuntimeError: Go baseline probe failed` from `tests/run-go-service-baseline.py:1305`, before the mixed Rust/Go SDK assertions. The uploaded artifact contains only the wrapper traceback, not the referenced `probe.log`, so the underlying Go baseline failure is unknown and does not demonstrate a Rust Matching defect.
- The artifact has no `lane.exit`; the final wrapper exit is 1. No readiness/direct-RPC failure diagnostic is present. SDK assertions, Matching restart, failure/recovery, and their cleanup assertions were not reached. The artifact records the PostgreSQL container name in `postgres-container-cleanup.txt`, but contains no explicit cleanup result, so cleanup success cannot be established from the uploaded files.
- Artifact: https://github.com/auenkr/cadence-rust/actions/runs/41056791529/artifacts/12977801450

## Related workspace baseline

Run https://github.com/auenkr/cadence-rust/actions/runs/41055162424 was reported as a fresh-target workspace run with package omitted, clippy and tests both exit 0, 1,829 passed and 28 ignored. This is the successful base workspace evidence; it does not cover the later supplemental candidate below.

## Supplemental candidate and next action

- Verified remote ref `t3/matching-queued-activity-candidate-5ee69b5` points to `565c8fc91893fae1a81cd8e47f10f2ee4744c046`, which descends from the immutable base above.
- No source defect was established by run `41056791529`; no source repair was made based on that run.
- Dispatched `rust-ci.yml` with package omitted and `clean_target=true` on that candidate. Run: https://github.com/auenkr/cadence-rust/actions/runs/41058036608
- At the single post-dispatch metadata check, the run was `in_progress` on the exact candidate head `565c8fc91893fae1a81cd8e47f10f2ee4744c046`. This is not a mixed-cluster gate. Do not start another mixed gate until this fresh workspace run passes.
- Next concrete action: once the run completes, inspect its conclusion and artifact. Only after a passing exact-head fresh workspace run, diagnose the absent Go `probe.log` through an appropriate mixed-gate run; do not infer a Matching fix from the current failure.

## Scope note

This checkpoint records this run's actual head, binary digest, stage reached, missing diagnostics, and pending fresh workspace run. The existing Matching coverage reconciliation at commit `8e6474fd7` already records the query-injection, refresh-RPC failure/recovery, and failover callback coverage audit; this run did not change that assessment.
