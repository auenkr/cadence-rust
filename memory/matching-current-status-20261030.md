# Matching current status — 2026-10-30

## State and source pin

Matching remains **incomplete**; do not begin History migration. The frozen
candidate is `0032e7b3341275c028952daa82ba302d9ede0f02` on
`t3/matching-failure-coverage-20261030142037`. Its ancestry is the reviewed
Matching integration, followed by the semaphore startup cancellation and
domain-cache fixture corrections, then the current query failure and partition
refresh failure regression coverage. The exact chain includes
`377fb2f8e` → `f8739bfbf` → `17ca0308e` → `f7c81d9d6` → `22c695d65` →
`1c1c6e94d` → `3ad26eb6b` → `df461a3e0` → `0032e7b33`.

`22c695d65af367a0b49b605877f72ec33fa2beb8` is an ancestor of the frozen
candidate. It is the latest authoritative fresh, uncached workspace success
recorded before the current test-only commits: CI run
[41028655670](https://github.com/auenkr/cadence-rust/actions/runs/41028655670),
with 1,827 passed and 30 ignored. This validates that exact earlier SHA, not
the current freeze. Current source adds query-dispatch cleanup and refresh
failure propagation coverage (plus the callback type/import corrections).

For the current frozen SHA, Rust CI run
[41031374784](https://github.com/auenkr/cadence-rust/actions/runs/41031374784)
was observed `in_progress` in Clippy, with no conclusion. No test result or
mixed-cluster result is established for this SHA. Parent owns scheduling; do
not dispatch or poll runs from this checkpoint.

## Failure history and superseding fixes

- Fresh workspace CI [41025116183](https://github.com/auenkr/cadence-rust/actions/runs/41025116183)
  failed Clippy because the semaphore stop callback's captured weak reference
  shadowed the registry lock guard (`managers` binding). The registry capture
  was renamed and the lock scope corrected before the next candidate.
- Fresh workspace CI [41027247246](https://github.com/auenkr/cadence-rust/actions/runs/41027247246)
  passed Clippy but failed two tests because fixture metadata watermarks were
  equal to cached notification versions. Go's refresh semantics skip equal or
  newer notification versions. The two fixtures now use metadata watermark 1
  with record version 0. These failures do not establish a production cache
  defect.
- The prior PostgreSQL mixed run
  [40808511931](https://github.com/auenkr/cadence-rust/actions/runs/40808511931)
  failed readiness before SDK operations. A Go Ringpop membership snapshot
  showed one Rust peer before later empty-ring errors; Rust listener startup
  was logged. Retained evidence does not establish whether the Go RPC route
  reached/completed a Rust RPC or why the peer was withdrawn. The added direct
  primary endpoint diagnostic improves observability; it is not a production
  fix and does not resolve the cause.

The query dispatch and partition refresh source audit found no demonstrated
production behavior gap in the reviewed paths. Prior ignored cases depended on
mock-only failure seams; current coverage exercises the real cleanup and
propagation paths. This source audit is not CI evidence.

## Acceptance sequence and reproducible commands

Run gates serially on the immutable source SHA. Cargo and Go validation remain
CI-only. Parent schedules them after reviewing current Rust CI artifacts.

1. Fresh uncached workspace Clippy followed by workspace tests:

   ```sh
   gh workflow run rust-ci.yml --ref <immutable-branch> -f clean_target=true
   ```

   Leave `package` unset. Record run URL, exact head SHA and conclusion, and
   inspect both Clippy and workspace test artifacts. The prior success above
   cannot substitute for this exact-head run.

2. PostgreSQL mixed acceptance on that same validated source, with Go
   Frontend/History/Worker and no Go Matching:

   ```sh
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=postgres
   ```

   Workflow common arguments include `--matching-restart
   --matching-failure-recovery --matching-idle-check-seconds 3`; PostgreSQL
   adds GC-domain, standby query, partition listing, durable restart, and
   auto-config probes. Inspect `goMatchingStarted=false`, SDK results, nested
   exit codes, and the Rust binary SHA256 in retained artifacts.

3. Complete the remaining production lanes serially, preserving the exact
   commands for reproducibility:

   ```sh
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=thrift
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=shard-distributor
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=configstore
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=mysql-probes
   gh workflow run matching-production-validation.yml --ref <immutable-branch> -f lane=adaptive
   ```

   `adaptive-single-diagnostic` is diagnostic only and omits two-instance
   owner recovery; it cannot replace `adaptive` acceptance. Keep existing Go
   services running and verify SDK compatibility, discovery, failure,
   restart, and recovery. Record each run URL, source SHA, conclusion and
   binary hash.

## Known coverage boundary

The PostgreSQL lane covers hard owner failure with a queued decision and
multi-partition activity backlog across durable restart. It does **not** yet
prove that an activity task queued on a specific Matching owner is recovered
through the survivor after that owner crashes. Graceful shutdown's queued-work
scenario also covers a decision task, not a queued activity task. Track the
queued-activity owner-crash scenario as outstanding acceptance work; do not
infer it from the existing probes.

The discovery diagnosis remains unproven regardless of diagnostic logging. The
separate shard-distributor lane covers that discovery mode; retain both the
Go Frontend readiness failure and independent direct-Rust endpoint status if
PostgreSQL readiness fails again.

## Source-linked evidence notes

The following compact notes were copied onto this documentation branch so
these links resolve here. They preserve source reviews and acceptance boundaries;
they do not turn prior failures, source audits, or pending runs into passes.

- [Candidate repair and earlier CI diagnosis](matching-candidate-repair-20261030.md)
- [Domain-cache fixture failure and correction](matching-domain-cache-ci-fix-20261030.md)
- [PostgreSQL discovery diagnosis](matching-native-postgres-discovery-diagnosis-20261030.md)
- [Integrated source inventory and prior CI evidence](matching-source-integration-20261030.md)
- [Acceptance harness audit](matching-harness-acceptance-audit-20261030.md)
- [Query, refresh, and failover source audit](matching-failure-path-source-audit-20261030.md)
