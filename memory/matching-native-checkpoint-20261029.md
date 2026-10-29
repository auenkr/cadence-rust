# Matching native checkpoint — 2026-10-29

Documentation-only state on shared `master`. Matching remains incomplete; no
native mixed lane has passed. Run
[40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415)
is terminal success on exact head
`721e196d31b2cd7876fc09086c8ac4b2a8995ba3`; its retained evidence and the
workspace-format advisory failure are recorded in
`memory/evidence/matching-ci-40842468415.md`. This CI result is not a mixed
lane pass. Frozen candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`
remains unchanged.

Reviewed worker state for continuation:

- `t3/integrate-matching-scenarios` at `74e9f45bd` has scenario integration
  review; a possible startup dropped-handler lifecycle defect remains
  unverified.
- `t3/matching-persistence-qps-parity` at `71b7547f5` is partial. Per-datastore,
  configstore, and resolver parity gaps remain.
- `t3/matching-rpc-quota-map` at `c8f8724ab` contains quota-map/fix work but is
  unverified; its invalid UTF-8 filter limitation remains.
- Two Terra workers now own semaphore lifetime correction and the remaining
  persistence gaps. Do not overlap or interfere with those tasks.

No native mixed lane has passed. All ten required mixed lanes remain due.
No source/workflow changes or runtime validation are included in this update.

Current docs record the source closure review in
`matching-candidate-source-closure-review-20261029.md` and the formatting
artifact provenance in `matching-ci-scheduler-evidence-20261029.md`. Source
candidate `8cc72372ad605dcdce4f03e86c895dc90115fd38` is docs-only after `e84e`;
formatting commit `721e196d31b2cd7876fc09086c8ac4b2a8995ba3` changes two Rust
files and has no CI credit. The formatting patch SHA256 is
`a5b3d531e4a31da663e09e1381ec949b7e4c66b8687d52f3b2c5062b9484a58a`.
Frozen `matching-native-final-20261029` remains
`bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`.

Historical state below predates the terminal run and worker assignments above.
It remains useful background but does not override the current owners or status.

Next chain: Terra gap closure and focused formatted CI; integrate any source
changes; freeze one immutable SHA; fresh uncached workspace Clippy then tests;
actual PostgreSQL pool dependency oracle; then the ten mixed lanes serially;
finish parity/evidence closure before History. Earlier evidence below is
historical and must not be mistaken for current-source acceptance.

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
