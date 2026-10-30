# Matching current validation handoff — 2026-10-30

## Current authoritative state

- Matching remains incomplete; do not begin History work.
- Source baseline under validation: `5ee69b5572688c2dba2245c7b411c7182c5b9b16`.
- Fresh workspace CI run [`41055162424`](https://github.com/auenkr/cadence-rust/actions/runs/41055162424) passed at that exact head: empty package selection, fresh runner target, Clippy exit 0 followed by tests exit 0; 1,829 passed and 28 ignored. This validates the baseline only.
- PostgreSQL mixed-service gate [`41056791529`](https://github.com/auenkr/cadence-rust/actions/runs/41056791529) targets immutable tag `validation/matching-postgres-5ee69b5`, whose head is exactly `5ee69b5572688c2dba2245c7b411c7182c5b9b16`. Inspected once at 2026-10-30 12:37:58 UTC: **in progress**, job `gate` at `Build fresh Rust Matching`; dependency probe and mixed validation had not started. No terminal artifact or gate conclusion is available yet.
- Candidate `565c8fc91893fae1a81cd8e47f10f2ee4744c046` is available at `origin/t3/matching-queued-activity-candidate-5ee69b5`. It descends from the baseline and includes source commits `3a1da7b`, `f07ef0a`, `d63c12d`, and `f009e8d`; its Rust source diff from the baseline is empty. Candidate remains **UNVERIFIED**.

## Candidate review

Static diff review covered the Go SDK fixtures, Matching production compatibility harness, Cassandra observer, and observer self-check. The supplemental scenario creates one durable activity task using the existing Go SDK, requires a positive activity range, asks both Rust Matching endpoints for the activity partition owner, SIGKILLs that owner, verifies survivor ownership, recovers through Go SDK workers, checks exact activity/workflow history counts and no timeout/failure events, confirms queue drain, then restarts the member and checks 64 owner observations from both Rust endpoints plus Go Frontend `DescribeTaskList`.

The Cassandra observer forwards task type 1 for activity rows and ranges while retaining decision type 0 by default. PostgreSQL task and range queries already consume the requested type. Review found no proven behavioral defect, so no speculative source change was made. The 64 samples demonstrate partition ownership convergence only, not an exhaustive Ringpop membership dump.

## Next actions

1. Once the current PostgreSQL gate is terminal, inspect its artifacts and record exact source SHA, Rust Matching binary SHA256, nested exit codes, original readiness error, structured direct `ListTaskListPartitions` result, request-stage traces, SDK/restart/recovery assertions, and cleanup. Diagnose only facts present in the artifacts.
2. If that gate proves a concrete source defect, make the narrow Go-compatible correction on the coherent `5ee69b5` ancestry, commit/push it, then dispatch fresh package-unset workspace CI with `clean_target=true`. Do not launch another mixed gate until that run passes.
3. If no defect is established, dispatch fresh workspace CI for candidate head `565c8fc91893fae1a81cd8e47f10f2ee4744c046` after the current gate is terminal. Do not move the frozen acceptance tag. Keep the candidate unverified until CI and required mixed PostgreSQL evidence exist.
4. Continue Matching parity work and its mixed Go/Rust failure, restart, and recovery evidence before moving to History.

No local Cargo, Go, build, format, test, or runtime validation was run. Host check before work: load averages `0.58, 0.84, 0.91`; available memory `4.3 GiB`.
