# Matching startup-abort regression review — 2026-10-30

Reviewed `17ca0308e22973bfc9216e362a679596189907be` against
`f8739bfbfc9ce1921b3404dd9cf6fd6193cb9db5`.

## Source review

The direct-poll fixture is deterministic: the acquire future is polled while
the startup scan is parked, so it is known to have entered its startup wait
before the start task is aborted. `ScriptStore::scanning.notify_one()` retains
its permit if the test reaches `notified()` after the scan signals. The scan
gate is an atomic flag and is set false before creating the replacement, so
that load does not depend on releasing the abandoned scan notification.

The test checks the three intended lifecycle outcomes: the waiter returns
`NotReady`, `by_identifier` no longer returns the stopped manager, and a
distinct replacement is installed and subsequently reused. It also checks
the second scan. No production behavior defect was established.

One proven test-harness gap remains: `waiter.await` has no deadline. If a
future regression leaves the startup watch asleep, this test can hang instead
of reporting a bounded failure. Follow-up should wrap that await in
`tokio::time::timeout` and assert `Ok(Err(ManagerError::NotReady))`. No source
change was applied here because the candidate is checked out in its own
worktree and its CI run is pinned to the current candidate; the recommendation
must be applied as a follow-up candidate and revalidated. No Cargo, Go,
formatter, service, or runtime validation was run.

## CI ownership and PostgreSQL acceptance plan

Parent cancelled obsolete run `41026357394`. The fresh uncached workspace run
is [41027247246](https://github.com/auenkr/cadence-rust/actions/runs/41027247246),
dispatched for candidate `17ca0308e22973bfc9216e362a679596189907be` with the
optional package unset. Its result was not polled or inferred here. Parent owns
that run and any mixed-gate dispatch.

For PostgreSQL acceptance, dispatch
`gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref t3/matching-startup-abort-regression -f lane=postgres`
only while that branch still resolves to the exact candidate SHA. The manual
workflow checks out the dispatch ref; it records `git rev-parse HEAD` in
`out/commit.txt` and uses a fresh run-specific
`/tmp/cargo-target-final-${GITHUB_RUN_ID}` without a Cargo cache. It builds
`cadence-matching` there and records `out/binary.sha256`. The acceptance
decision must match all three identities: Actions head SHA, `out/commit.txt`,
and the intended candidate SHA; retain the binary hash with the result. If the
candidate changes before dispatch, stop and choose the new exact candidate
explicitly rather than attributing the gate to this SHA.

Require zero in `out/build.exit`, `out/gate.exit`, `out/gate/lane.exit`, and
`out/gate/run/run.exit`; `out/gate/run/results.json` must report no failure,
Rust Matching present, Go Matching absent, Go Frontend/History/Worker present,
and the PostgreSQL topology/config. In `probe.log`, require actual SDK workflow
completion and inspect the concrete `processRestart`,
`idleUnloadAndRecovery`, and `gracefulDrainRecovery` records, plus each emitted
scenario JSON for PostgreSQL GC-domain, query standby, list partitions, durable
restart, and auto-config hint. Correlate those with Rust and Go service logs.
Check the PostgreSQL container log and cleanup record. A green Actions result,
the `preliminary-passed` label, or a reported Ringpop readiness line alone is
not acceptance.

Interpret the initial Ringpop log lines as startup/discovery diagnostics only:
check for actual Rust Matching JOIN/bootstrap success and correlate the
probe's required Ringpop discovery path, topology in `results.json`, and
subsequent SDK calls. A short/truncated startup excerpt cannot establish
cluster membership or successful workflow traffic by itself. The existing
artifact checklist is
[`matching-native-postgres-artifact-checklist-20261029.md`](matching-native-postgres-artifact-checklist-20261029.md).

This review and the PostgreSQL checklist do not close the Matching parity
audit. The review commit SHA and any eventual PostgreSQL run URL/head/result
must be recorded on the review branch by the parent after the gate completes.
