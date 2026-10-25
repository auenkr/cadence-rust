# Matching final sign-off review (read-only, candidate 29bdd2b04)

Matching is INCOMPLETE. This review does not mark completion.

## Candidate limits
- Source `29bdd2b04b573309112286c57f1c347f0ba0b60c`. `git diff fade09c5..29bdd2b04 -- rust` is empty: Rust production is byte-identical to the fresh-workspace source `fade09c5`. Only `.github/workflows/rust-ci.yml` (Go DescribeTaskList oracle step), tests/ harness and docs differ.
- Fresh workspace 40168414544 is bound to `fade09c5`, not 29bdd. It does not run the new Go oracle step; that ran in cached run 40171379726 on `5cafc2dc`. No fresh run exists on 29bdd. Production equality justifies reuse for Rust tests, but must be stated as inference.
- HTTP TLS interop 40172574953: success on exactly 29bdd (verified via gh). Peer = original Go YARPC clients; History endpoint = real Go Ringpop member fronting the Rust History gRPC fixture, NOT original Go History. Fixture/standalone evidence, not Go-service mixed acceptance.
- TLS production fix e3c79b30 is in 29bdd (ancestor of fade09c5); fresh 40164484146 covers it.
- Backend/transport/discovery lanes (PostgreSQL, MySQL, Thrift, configstore, shard-distributor, Cassandra, active-offer) are all source-pinned at `29bdd2b04b573309112286c57f1c347f0ba0b60c`. PostgreSQL run 40173134481 succeeded with `preliminary-passed` / `acceptanceEligible:false`. MySQL extended run 40173683827 succeeded on the exact source with the same labels; compact evidence is in `memory/evidence/matching-mysql-extended-40173683827/`. Its `durableGcEvidence:false` entries mean no persisted row was observed before delivery for two activity queues, a measurement limit rather than a GC bug; `acceptanceEligible:false` is expected for this non-Cassandra lane. Exact queue objects and artifact source hashes are preserved in `mysql-gc-queues.json`. Thrift is now dispatched serially on the same pin; configstore, shard-distributor, Cassandra and active-offer remain queued. Older runs pre-date TLS merge and runtime-reader/Unleash/lifecycle changes and must not be inherited.

## Coverage classes
- Source/unit: 13 RPC handlers, lifecycle units, TLS matrix (invalid DER, lone cert/key, missing identity), metrics comparison (4 `match`, fade09c5), owner-order Go oracle, DescribeTaskList oracle (5cafc2dc).
- Fixture: HTTP TLS interop (Go ring stub + Rust History fixture), synthetic Go oracle mock errors (nil outer panic / unknown-domain mapping: explicit limitations, no Rust guard).
- SDK mixed (original Go Frontend/History/Worker + Rust Matching): only the serial lanes below; currently none current-source.

## Reconciliation of stale rows (verified read-only, supersedes the earlier list)
Ancestry (git merge-base): 27d3ff5f, 4e88d36b, 11f13b791, 944e7c2bc are all ancestors of 29bdd and fade09c5; 4e88d36b and 11f13b791 are ancestors of 27d3ff5f.
- Root-start persisted-config remote-child gRPC notification: CLOSED. Real test at 4e88d36b, CI 40090630855 / 40090951826 (audit L113-126). The old ignored placeholder is not evidence of a missing replacement. Earlier "no direct startup evidence" claim withdrawn.
- Config file reload: CLOSED. Production watcher tests at 11f13b791 (1->2->malformed/removed retains 2->3, awaited shutdown); CI 40107592237 / 40108030889 in `memory/evidence/matching-runtime-watcher-tests-20261024.md`.
- Raw batch size: CLOSED as no established gap. Go task_reader.go fallback to default for non-positive, reviewed in `memory/matching-historical-differences-audit-20261025.md` (944e7c2bc); raw getter preserved.
- Cancellation / refresh typed status: CLOSED, verified directly in 29bdd source. Full SHA `4cabb9cecee5a177a2a9462a9e8838ed9fb13324` ("style(matching): format test imports", a formatting-only commit) is an ancestor of 29bdd, as is merge `a568bcae1`. Tests in `rust/crates/cadence-matching/tests/matching_engine_go_tests_c.rs` (not ignored): `refresh_workflow_tasks_entity_not_exists_returns_ok` (typed google.rpc.Status EntityNotExists => Ok, one request, no retry), `refresh_workflow_tasks_already_completed_is_propagated` (code/message/details preserved, decoded type_url `...WorkflowExecutionAlreadyCompletedError`, one request, no retry), `refresh_workflow_tasks_canceled_context_is_propagated` (History Cancelled propagated unchanged; already-cancelled caller via `refresh_workflow_tasks_until` returns Cancelled "context canceled" with zero History requests), plus internal/generic propagation tests. Limit stated in-file: Go typed InternalServiceError detail cannot be built from tests/, so the internal case checks an untyped INTERNAL status only. Earlier "missing object" caveat withdrawn (I used the abbreviated SHA). Other `#[ignore]` cases in that file are named engine-private parity gaps (failover callback, partition-config injection), outside this row.
- Owner-order oracle: CLOSED. Fresh 40168414544 (fade09c5) preserves baseline/add/remove/identity/address-replace/empty-ring/root-child-empty/decision-activity cases.
Withdrawn: "older runs predate runtime-reader/lifecycle". Candidate 27d3ff5f (PostgreSQL 40108645569, Thrift 40153127238, Cassandra 40153493941, MySQL 40155956238 on e06fd8ca6, shard-distributor 40156296140, configstore 40086531679/e0aeae9e8 older still) is an ancestor of 29bdd.
Still stale, verified: `git diff 27d3ff5f..29bdd2b04 -- rust` (non-test paths) changes grpc_matching.rs (+/-1693 lines), launcher.rs, grpc_tls.rs, Cargo.toml/lock; matching_rpc_tests.rs also. So every backend/transport lane was run on a different production binary (TLS, embedded-startup, and grpc_matching changes). Their green status is not inherited; I did not inspect whether the grpc_matching diff is behavioral.
Real-Go-History x TLS: not required as an arbitrary cross-product. Transport interop (40172574953) and real-Go-History lanes are separate proofs. Meaningful remaining interaction: only that the Go History->Rust Matching callback path ran under the *final* binary (covered by active-offer + postgres/thrift lanes). Report the combo as a limit.

## Remaining open (after reconciliation)
1. Complete the 7 mixed lanes on final pin `29bdd2b04b573309112286c57f1c347f0ba0b60c`, serial. PostgreSQL and MySQL extended succeeded; Thrift is pending; configstore, shard-distributor, Cassandra and active-offer remain queued. No production Go-service gate can be counted from the HTTP fixture lane.
2. Fresh uncached workspace directly on 29bdd was not run. The exact `rust/` tree comparison to fade09c5 is empty, so reusing 40168414544 is a documented production-tree-equivalence inference, not an exact-29bdd CI result.
3. Workspace fmt advisory exit 1 reported, not a pass.

## Final done conditions
- Pin one `EXPECTED_SHA`; each run's headSha equals it; serial dispatch; artifacts downloaded and inspected; binary SHA256 per mixed lane; cleanup evidence.
- Fresh `clean_target=true` rust-ci (clippy then tests) on that SHA, or documented production-tree equivalence with `git diff` proof. The latter is now verified; no duplicate workspace dispatch was made.
- All 7 lanes success; labels `preliminary-passed`/`acceptanceEligible:false` kept as emitted and reconciled.
- Audit rows in item 3/4 each closed with a named test or Go-source justification.
- Tracker + audit updated with run URL/SHA/conclusion; then parent decides completion (not this review).
