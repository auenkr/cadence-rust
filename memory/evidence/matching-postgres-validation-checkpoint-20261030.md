# Matching PostgreSQL validation checkpoint — 2026-10-30

## Frozen source and gate ref

- Frozen Matching source: `0032e7b3341275c028952daa82ba302d9ede0f02`
  (`t3/matching-failure-coverage-20261030142037`).
- Prepared immutable PostgreSQL gate ref:
  `t3/matching-postgres-gate-0032e7b334`, verified at the same exact SHA.
- The production gate workflow is present at the frozen source. Its blob is
  `9eb4169da7f6b0fe9117900842016df4d3b5e992` for
  `.github/workflows/matching-production-validation.yml`; the Rust CI workflow
  blob is `97c15034908b2aa97fbb653771a418fa015cfd7d`.
- The production workflow builds `cadence-matching` on `ubuntu-latest` in a
  unique `/tmp/cargo-target-final-${run_id}` directory, writes `commit.txt`,
  `build.exit`, and `binary.sha256`, then uploads gate artifacts with 3-day
  retention. PostgreSQL lane uses `tests/gates/run-postgres-lane.sh` with
  `--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3`,
  plus GC-domain, standby-query, partition-list, durable-restart, and
  auto-config probes.

## Fresh workspace CI checkpoint

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41031374784
- Head SHA: `0032e7b3341275c028952daa82ba302d9ede0f02`
- Observed once: run `in_progress`; Clippy and formatting passed; workspace
  `Test` step was still in progress. This is not a terminal workspace result.
- No artifact was treated as clean-target/test success, and no PostgreSQL mixed
  gate was dispatched. There is therefore no production binary SHA, endpoint
  or discovery result, ring PING, readiness, cleanup result, SDK result, or
  recovery result for this source yet.

## Next action

After the workspace run becomes terminal, inspect its uploaded artifact and
verify fresh-target provenance, `clippy.exit=0`, `test.exit=0`, the artifact
head identity, and current ignored-test counts. Only on a complete matching
pass dispatch:

```sh
gh workflow run matching-production-validation.yml \
  --ref t3/matching-postgres-gate-0032e7b334 -f lane=postgres
```

Record that run URL and source SHA, then inspect the PostgreSQL endpoint and
discovery diagnostics, ring PING, readiness, cleanup, SDK and restart/recovery
results, nested exit codes, and Rust binary SHA256. A dispatch alone establishes
none of those outcomes.
