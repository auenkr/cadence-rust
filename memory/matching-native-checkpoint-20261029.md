# Matching native checkpoint — 2026-10-29

Documentation-only state on shared `master`. Matching remains incomplete; no
native mixed lane has passed. Run `40840392628` is now terminal and was reviewed
against its artifacts at the expected exact head. No CI was dispatched and no
source, workflow, or service implementation was changed.

## Candidate and validation

- Integrated review candidate: `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`
  on `t3/matching-startup-integration-review`.
- Run [40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628)
  completed `success` at that exact head. Job `clippy-test` also concluded
  success. The downloaded `rust/commit.txt` matches the expected SHA. Compact
  evidence: `memory/evidence/matching-startup-candidate-ci-40840392628/`.
- Selected package was `cadence-matching`. Recorded exits: `clippy.exit=0`,
  `test.exit=0`, `fmt.exit=0`, `matching-runtime-format.exit=0`,
  `query-format.exit=0`, `tls-format.exit=0`; advisory
  `workspace-format.exit=1` due to a diff in unrelated
  `rust/crates/cadence-api/src/admin.rs:371`. The workflow captured this
  advisory formatting failure and continued. Test log has 27 result summaries:
  1,083 pass results, 0 failures, 16 ignored (including filtered/repeated
  library test invocations); the library suite itself was 973 passed and 2
  ignored. The run restored a Rust cache and used the ordinary package target,
  so it is not fresh uncached workspace acceptance.
- The log contains cold-domain/runtime startup coverage, including the
  `grpc_matching::cold_domain_metric_tests::*` cases,
  `grpc_matching::durable_tests::matching_grpc_listener_registers_meta_health_and_gates_startup`,
  `matching_host_native::tests::startup_gate_opens_and_shutdown_wakes_waiters_with_unavailable`,
  and `thrift_matching::tests::matching_thrift_nil_request_waits_for_startup_like_go_start_profiling`.
  No Rust test files changed in the run head's commit. Do not attribute later
  semaphore startup-timeout, expanded adapter/lock, or two-thread regression
  cases to this run. Specifically exclude candidates `25e`, `90cd`, `e84e`,
  and `8cc` from its evidence.
- This package run does not replace frozen candidate
  `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; its PostgreSQL mixed run failed
  before SDK execution. Adapter branch `04826c745` and semaphore branch
  `112217ac44cb88c25cb35a9a6bb0ea60d9a2f5e2` remain separate later unverified
  regression work. The ten mixed lanes below remain required.
- Requested audit note source thread
  `t3/matching-native-gate-coverage1fc484820` returned `thread_not_found`;
  it supplies no evidence.

## Ten required mixed lanes

All lanes require one coherent validated source SHA, Rust Matching with Go
Frontend/History/Worker and no Go Matching, and reviewed SDK, service interaction,
failure/restart/recovery, topology, exits, cleanup, and Rust binary SHA256.
No lane below has passed on the native candidate.

1. PostgreSQL — failed on old frozen `bf9f3d3`; SDK did not run.
2. MySQL probes.
3. Thrift Matching with TChannel History.
4. Configstore.
5. Real shard distributor / etcd.
6. Two-node MySQL — retain this lane; require dual-node behavior and recovery.
7. Cassandra semaphore/task interoperability.
8. Two-node adaptive scaling.
9. Active offer through Go History.
10. Production Matching TLS.

The supplemental Go History peer and PostgreSQL TLS dependency oracles remain
additional evidence, not substitutes for these ten lanes. Workspace CI must pass
fresh-target Clippy followed by workspace tests on the same frozen candidate
before serial mixed validation. There is no native lane pass to report.

## Dependency chain

1. Terra integration/review must settle the adapter and semaphore regression
   changes and their exact source identity. Keep them distinct from run
   `40840392628`'s earlier cold-domain/runtime tests.
2. Freeze one integrated candidate. Run fresh uncached workspace CI (Clippy,
   then workspace tests) and record URL, exact head, conclusion, actual exit
   files, and test totals.
3. Resolve startup/readiness and shutdown evidence before mixed acceptance.
   Execute all ten lanes serially on that same immutable candidate, retaining
   artifact identities, binary SHA256, SDK behavior, failure/restart/recovery,
   and cleanup evidence.
4. Complete the Matching parity audit and required supplemental oracles before
   migrating to History.

This checkpoint records evidence only and does not change source, workflows,
frozen refs, or mixed-lane status.
