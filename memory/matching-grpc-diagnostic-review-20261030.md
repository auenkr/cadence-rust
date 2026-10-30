# Matching gRPC diagnostic review checkpoint (2026-10-30)

## Candidate source

- Candidate/tag: `validation/matching-grpc-diagnostic-6c73d9aa` at
  `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
- It is descended from queued-activity recovery commit
  `565c8fc91893fae1a81cd8e47f10f2ee4744c046`. Its only changes from that
  parent are this diagnostic harness change and its source note; recovery
  implementation/assertions are unchanged.
- Review of `tests/matching-service-compat.cjs` confirms the diagnostic records
  bounded TCP connect and gRPC channel state, target/resolver mode, proxy
  variable names and `NO_PROXY` presence, and credential mode. It does not
  change readiness conditions or acceptance assertions. The workflow's
  PostgreSQL lane enables process restart and failure-recovery scenarios.

## Exact-candidate workspace CI

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41059165266
- Terminal conclusion: success; run head is the exact candidate SHA above.
- The `rust-ci-logs` artifact records a fresh target directory at
  `/home/runner/work/_temp/cargo-target-final`, with workspace Clippy exit 0,
  workspace test exit 0, and candidate head SHA. Metrics evidence records the
  Rust oracle binary SHA256 as
  `fb7222e35ac7d5e8509c3c261379ab1d03fca2b5788c78d85a7e8a60b4735689`.
- `workspace-format.exit` is 1, while `fmt.exit` is 0. The workspace format
  diff log reports formatting differences in seven crates: cadence-api (32
  entries), cadence-common (15), cadence-matching (398), cadence-persistence
  (212), cadence-server (5), cadence-service-core (108), and cadence-thrift
  (1).
  These are pre-existing relative to this candidate: its diff from parent
  contains only `tests/matching-service-compat.cjs` and memory documentation,
  with no Rust source. The changed Matching runtime formatting check exits 0.
  No local formatter was run and no format result was edited.
- Artifacts were downloaded once into the initially empty directory
  `/tmp/matching-grpc-ci-41059165266-artifacts-20261030`.

## PostgreSQL mixed acceptance run: pending

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41094318642
- Ref: `validation/matching-grpc-diagnostic-6c73d9aa`; reported head SHA is
  exactly `6c73d9aa447171d4eeda37d71dd0a955d0996a9b`.
- At the one requested inspection (2026-10-30 16:56:04 UTC), run status was
  `in_progress`; the `Build fresh Rust Matching` step was running. Probe
  dependencies, mixed Go/Rust validation, and artifact upload were pending.
  No terminal artifact was available.
- Therefore this checkpoint records no PostgreSQL gate result, Matching
  service binary SHA, nested build/probe/lane/gate exit, SDK result, readiness
  or `transportDiagnostic` fields, restart result, queued-activity recovery
  result, or cleanup result. No connection cause is inferred from the earlier
  `41056791529` base evidence.
- Next action: once this run is terminal, download the retained
  `matching-production-postgres` artifact into a new unique empty directory
  and inspect its commit, binary hash, nested exits, readiness/transport
  diagnostics, SDK scenarios, restart/recovery evidence, and cleanup records.
  If it fails, diagnose from that artifact before making a behavior-preserving
  change or a bounded discriminating diagnostic. Dispatch fresh clean-target
  package-unset `rust-ci` before any new mixed gate after a change. If it
  passes, advance only to the next serial Matching acceptance lane after
  verifying all scenario evidence and confirming no competing gate.

Matching remains incomplete. The pending diagnostic lane alone cannot establish
the ten-lane acceptance matrix or service parity, and does not authorize work on
History.
