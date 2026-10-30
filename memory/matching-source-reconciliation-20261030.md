# Matching source reconciliation (2026-10-30)

## Source graph

This audit uses Git object ancestry and tree diffs; the previous count-reconciliation
note is corroborated by the repository history.

| Ref | Parent / relation | Result |
| --- | --- | --- |
| `3a958668c040b289b495f9b76b2209969db7e2a0` | descends from `5755021dbaaeeb70b87ec9ba3c69bd06065398e4` | Previously validated source line (CI 41037584598: 1829 passed, 28 ignored). |
| `a534baae8c921e0ec8109527c6bc3184937b0323` | child of `5b0423a8781eecfa506e5b2726c874c597f754ac` | Active diagnostic line (CI 41040870450: 1813 passed, 30 ignored). |
| `merge-base(3a958668, a534baae)` | `5b0423a8781eecfa506e5b2726c874c597f754ac` | The two tested heads diverged here. `3a958668` is not an ancestor of `a534baae`. |

The discrepancy is explained by the branch split, not by the trace commit deleting
tests: `a534baae` was made directly on the older `5b0423a` line, while `3a958668`
is on the descendant line rooted at `5755021d`. The following commits are ancestors
of `3a958668` and absent from `a534baae` ancestry: `caa32882f` (dynamic-config
source scan), `f7c81d9d6` (metadata-watermark fixtures), `741dafd86` (refresh error
propagation), and `1c1c6e94` (query dispatch cleanup). The latter two add executable
failure/recovery tests and remove their corresponding ignored Go-test stubs.

`git diff 3a958668 a534baae -- rust/` reports 28 files, 229 insertions, and 2101
deletions. These are the exact Rust paths and the parity behavior or coverage missing
from the `a534baae` tree (or, where stated, diagnostics only):

| Rust path | Difference and effect in `a534baae` relative to `3a958668` |
| --- | --- |
| `crates/cadence-matching/src/grpc_matching.rs` | Drops typed `ServiceBusy`/internal semaphore error transport and the test-only query-dispatch failure seam used to assert RPC error cleanup. |
| `crates/cadence-matching/src/launcher.rs` | Replaces per-configured-datastore persistence limiters and published-ring member counts with one default-store limiter/count; also removes startup/shutdown stage diagnostics. |
| `crates/cadence-matching/src/matching_dispatch_native.rs` | Stops carrying the caller's parent deadline through backlog forwarding and slow parent resolution; removes the two deadline/withdrawal regression tests. |
| `crates/cadence-matching/src/matching_domain_cache.rs` | No parity deletion here; `a534baae` adds opt-in ListTaskListPartitions stage markers, retained in the candidate. |
| `crates/cadence-matching/src/matching_dyncfg_defaults_tests.rs` | Loses the corrected dynamic-config source-scan test fixture/assertions from `caa32882f`. |
| `crates/cadence-matching/src/matching_engine_control_native.rs` | No parity deletion here; `a534baae` adds partition-count and owner-lookup trace markers, retained. |
| `crates/cadence-matching/src/matching_launch_native.rs` | Deletes the configured gRPC semaphore startup timeout, cleanup, and retry regression scenario; `3a958668` also includes the `f7c81d9d6` watermark-correct fixture. |
| `crates/cadence-matching/src/matching_membership.rs` | Removes published resolver-ring member counting and its update test, affecting the Go-compatible membership count used by limiter configuration. |
| `crates/cadence-matching/src/matching_membership_lifecycle.rs` | Removes startup provider/subscribe/refresh stage traces; this is diagnostic coverage, not a changed Go contract. |
| `crates/cadence-matching/src/matching_partition_cancellation_tests.rs` | Deletes executable query-dispatch error cleanup and refresh error propagation/recovery coverage from `1c1c6e94` and `741dafd86`. |
| `crates/cadence-matching/src/matching_persistence_limiter.rs` | Removes independent limiters for all configured stores and the published-ring member-count sampling path; loses the dedicated multi-store test. |
| `crates/cadence-matching/src/matching_persistence_native.rs` | Drops context-aware, rate-limited semaphore confirm/grant calls, so cancellation-aware persistence and limiter accounting are bypassed. |
| `crates/cadence-matching/src/matching_reader.rs` | Only restructures timeout arithmetic; no observable parity loss was identified in this file's diff. |
| `crates/cadence-matching/src/matching_reader_native.rs` | Removes the reader start-state failure seam and no longer passes the parent deadline into backlog forwarding; this weakens dispatch deadline behavior. |
| `crates/cadence-matching/src/matching_registry_native.rs` | Removes constructed-domain-name/live dynamic-config binding and invalid-liveness-ticker partial-manager recovery behavior/test. |
| `crates/cadence-matching/src/matching_ringpop_provider.rs` | Removes provider startup/ready/monitor/healer stage traces; diagnostic only. |
| `crates/cadence-matching/src/matching_runtime_native.rs` | Deletes dynamic timeout/config binding by the manager's constructed domain name and its live-value regression test; includes the earlier metadata-watermark fixture correction. |
| `crates/cadence-matching/src/matching_semaphore.rs` | Removes typed internal and ServiceBusy error variants, losing original Go error classification before transport. |
| `crates/cadence-matching/src/matching_semaphore_manager_native.rs` | Drops typed/raw startup scan error context, caller-aware confirm/grant cancellation, reservation restoration coverage, and registry-serialized construction semantics. |
| `crates/cadence-matching/src/matching_semaphore_parity_tests.rs` | Removes the aborted-startup unregister/release/waiter-rebuild regression test. |
| `crates/cadence-matching/src/matching_semaphore_service_native.rs` | Removes independent startup-timeout cleanup/retry, raw-bucket scan failure, and typed/untyped Go wire-error tests and handling. |
| `crates/cadence-matching/src/matching_service_native.rs` | No parity deletion here; adds bounded, environment-opt-in ListTaskListPartitions request-stage tracing, retained in the candidate. |
| `crates/cadence-matching/src/ringpop_provider.rs` | Removes JOIN/PING diagnostic trace markers; the diff does not change discovery state transitions or wire handling. |
| `crates/cadence-matching/tests/matching_engine_go_tests_b.rs` | Adds back an ignored query-dispatch Go test stub; the executable Rust failure-cleanup test exists on the `3a958668` line. |
| `crates/cadence-matching/tests/matching_engine_go_tests_c.rs` | Adds back an ignored refresh-error Go test stub; the executable Rust propagation/recovery test exists on the `3a958668` line. |
| `crates/cadence-persistence/src/semaphores.rs` | Removes typed internal and ServiceBusy persistence error variants needed to preserve Go error details. |
| `crates/cadence-persistence/src/semaphores_cassandra_native.rs` | Removes caller cancellation around confirm/grant I/O and the test that preserves an already-ready result while cancellation is signaled. |
| `crates/cadence-persistence/src/semaphores_native.rs` | Removes the semaphore call context and default context-aware confirm/grant adapters, preventing cancellation/deadline context from reaching persistence backends. |

The count note's 16 previously passing test names were checked against the candidate
working tree with `rg`; all 16 definitions are present. The two ignored Go-test stub
names in `a534baae` are replaced on this line by the executable query and refresh
failure tests. `caa32882f`, `f7c81d9d6`, `741dafd86`, and `1c1c6e94` are all confirmed
ancestors of `3a958668` by `git merge-base --is-ancestor`.

## Candidate composition

Original source candidate: `t3/reconcile-matching-source-candidate`, based exactly at
`3a958668c040b289b495f9b76b2209969db7e2a0`. Applied only the three Rust source-file
hunks from `a534baae8c921e0ec8109527c6bc3184937b0323` (47 added lines in
`matching_domain_cache.rs`, `matching_engine_control_native.rs`, and
`matching_service_native.rs`). Did not bring over its diagnostic report or any of
the 28-file source reversion. The trace is bounded to 32 messages and inactive
unless `CADENCE_MATCHING_LIST_PARTITIONS_TRACE` is set.

## Failed PostgreSQL lane boundary and trace wiring

Run `41053262746` failed on `a534baae8c921e0ec8109527c6bc3184937b0323`.
Artifacts are downloaded to
`/mnt/storage/service/ci-evidence/matching-postgres-41053262746`.

- Build exit: 0. Rust Matching binary SHA256:
  `35dbba533304d735476161da8ca5a50f4566706e3132a3f18f824a8409cbc5ff`.
- Outer gate exit: 1; nested PostgreSQL lane exit: 1; service baseline exit: 1.
  PostgreSQL container initialized and became ready. Go Frontend, History, and
  Worker started; results record `workerReadiness.observed: true`,
  `goMatchingStarted: false`, and `acceptanceEligible: false`.
- The Rust process started and bound internal gRPC at 27448 and TChannel at
  19025. The first readiness iteration timed out on Go Frontend `DescribeTaskList`
  at `127.0.0.1:20882` (`remote_addr=127.0.0.1:20882`). The compatibility probe
  never advanced to its direct `ListTaskListPartitions` owner lookup. No SDK body,
  PostgreSQL scenario, restart/recovery scenario, or request-stage handler was
  reached. `probe.log` then reported `Rust Matching did not stop` during cleanup;
  shutdown evidence records SIGTERM. Consequently this run proves neither a
  Matching RPC logic defect nor any PostgreSQL persistence defect. The failure
  boundary is discovery/readiness through the Go Frontend, followed by cleanup.
- The failed run's workflow explicitly set
  `CADENCE_MATCHING_LIST_PARTITIONS_TRACE=1`, so opt-in tracing was active on
  `a534baae8`; its Rust log contains no request-stage trace because readiness did
  not reach Matching. The coherent source candidate inherited the three trace
  hunks but not the workflow line. The follow-up candidate restores that opt-in
  line in `.github/workflows/matching-production-validation.yml` so a subsequent
  diagnostic lane records matching ListTaskListPartitions stages when requests
  arrive.

## Current checkpoint (2026-10-30)

Working branch `t3/matching-postgres-readiness-trace-reconciled` starts from
`c435a2067c247a3da64936f444d5151be1c6a90b`, which descends from the previously
validated `3a958668` source line and retains its 16 additional tests plus all three
bounded request-trace source hunks. This avoids the divergence identified above:
`a534baae8` descends from `5b0423a`, not `3a958668`, and its large Rust tree diff
removes 16 of those tests and multiple behavior/failure-coverage fixes. The only
source-tree change to the follow-up is the missing diagnostic opt-in line; queued
activity candidate `f009e8d` remains separate and is not included.

The failed run artifacts were read only. No Cargo, Go, formatter, or runtime command
was run locally. The candidate was committed and pushed, then the fresh CI run below
was dispatched. No mixed-cluster gate is authorized until exact-head Clippy and
workspace tests pass.

## Reconciled candidate CI dispatch

Committed and pushed the workflow opt-in plus this audit on
`t3/matching-postgres-readiness-trace-reconciled`. Fresh `rust-ci.yml` was dispatched
with `clean_target=true` and package left unset. At dispatch, exact head was
`4ffa5b8d2776ee3217144188db5379412f4ce0cb`; run:
<https://github.com/auenkr/cadence-rust/actions/runs/41054585478>.
The one status read returned `in_progress` (no conclusion yet). Per the checkpoint
instruction, the run was not polled. No mixed-cluster gate has been started.
