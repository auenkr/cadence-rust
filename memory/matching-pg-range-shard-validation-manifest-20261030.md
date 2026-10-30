# Matching PostgreSQL range shard candidate validation manifest

Candidate: `origin/t3/matching-pg-range-shard-filter` at immutable source SHA
`a47e4032d` (full: `a47e4032d`). Required ancestry is present:
`c22fdcf0a` -> `be7016ef2` -> `565c8fc91` -> `6c73d9aa4` -> candidate
(`git merge-base --is-ancestor` confirmed each listed commit is an ancestor).
This is a reproducible validation plan, not a pass or acceptance claim.

## Ownership and order

Main worker owns active Actions run `41100032910`; do not dispatch or poll CI
until that owner reconciles it. When authorized to run this manifest, validate
the exact candidate SHA on `rust-ci` first. Dispatch the immutable PostgreSQL
gate only after the workspace workflow concludes `success` on the same SHA.
If branch HEAD has moved, stop and restore/pin the candidate SHA before either
dispatch. No local validation is part of this procedure.

## 1. Fresh whole-workspace CI

From the repository, dispatch with package omitted (empty means full workspace)
and fresh target explicitly enabled:

```sh
gh workflow run rust-ci.yml --ref t3/matching-pg-range-shard-filter -f clean_target=true
```

Confirm the run's head SHA is exactly `a47e4032d` (full SHA above), and its
conclusion is `success`. The workflow selects
`$RUNNER_TEMP/cargo-target-final` and skips rust-cache when `clean_target=true`;
the clippy step precedes the workspace test step. Preserve artifact
`rust-ci-logs` (3-day retention), especially `rust/commit.txt`,
`rust/target-dir.txt`, `rust/clippy.log`, `rust/clippy.exit`, `rust/test.log`,
and `rust/test.exit`. This is the package-unset workspace gate, not a
`cadence-matching`-only run.

## 2. Immutable mixed PostgreSQL gate (only after step 1 passes)

Dispatch the production workflow at the full commit SHA, selecting PostgreSQL:

```sh
gh workflow run matching-production-validation.yml --ref a47e4032d -f lane=postgres
```

Confirm run head SHA is the full candidate SHA. The workflow preflights Node
syntax for the service compatibility, task observer and self-check, probe
scenarios, adaptive control, and process-evidence scripts; it runs the observer
self-check and compiles the Go SDK fixture (`go test -run '^$' .`) before
building fresh `cadence-matching`. Then it computes
`sha256sum "$CARGO_TARGET_DIR/debug/cadence-matching"` into
`out/binary.sha256` and runs the mixed Rust Matching + Go Frontend/History/Worker
PostgreSQL lane with restart and failure recovery. The lane's exact harness
arguments are:

```text
--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3 --probe-pg-gc-domain --probe-query-standby --probe-list-partitions --probe-durable-restart --probe-auto-config-hint
```

Diagnostics supplied by the workflow: `CADENCE_MATCHING_PROC_EVIDENCE=1` for
PostgreSQL; `CADENCE_MATCHING_LIST_PARTITIONS_TRACE=1`; and
`CADENCE_MATCHING_SHUTDOWN_TRACE=1`. There is deliberately no
`CADENCE_MATCHING_PROC_EVIDENCE_GDB` setting: its optional GDB attach remains
disabled (unset, not `1`).

The production gate records source identity in `out/commit.txt`, the exact
invocation in `out/command.txt`, status in `out/gate.exit` and
`out/gate/lane.exit`, and the Rust executable digest in `out/binary.sha256`.
Download/preserve the `matching-production-postgres` artifact (3-day
retention), including `out/binary.sha256`, logs and probe evidence. Record the
run URL, full head SHA, conclusion, and binary hash alongside the downloaded
artifact. Do not describe a failed, missing, or mismatched run as acceptance.

## Workflow facts checked

`.github/workflows/rust-ci.yml` has optional `package` and boolean
`clean_target` workflow-dispatch inputs; unset package selects `--workspace`.
`.github/workflows/matching-production-validation.yml` selects the lane and
all required preflight, PostgreSQL diagnostics, binary hash and artifacts.
Both relevant artifact uploads specify `retention-days: 3`.
