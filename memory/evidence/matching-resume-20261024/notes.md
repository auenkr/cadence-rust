# Matching resumption — 2026-10-24

The root checkout was ff6d4b9f1, behind origin/master 4424a3b4d. Preserved its generated tracked Python bytecode change in stash `matching-resume-generated-pyc`, fast-forwarded, then integrated the previously unmerged branches matching-tests-port3-20261024 (includes port2), matching-testfix-20261024, and matching-twonode-20261024. Integration commit 55d8bc867; pushed to origin/master.

User now requires ALL Cargo work on GitHub Actions; AGENTS.md updated in fb8bb1dbb. Workers use gpt-6-luna, medium reasoning, normal/default service tier, up to five. Started three isolated workers: remaining matcher/forwarder tests, CI Cassandra acceptance, and final parity audit. Worker branches must be reviewed and integrated before completion.

425f50562 adds rust-ci workflow_dispatch clean_target=true: bypasses target cache and selects RUNNER_TEMP/cargo-target-final, records clippy/test exit files, commit and target directory. It also fixes a pre-existing workflow defect: echo after the clippy pipeline could hide a failure despite pipefail. Both commands now capture PIPESTATUS[0] and exit with it.

Fresh whole-workspace validation started at https://github.com/auenkr/cadence-rust/actions/runs/40073658725 for 425f50562. Confirmed live in Clippy with fresh-target selection succeeded and Rust cache step skipped. No pass claimed yet. Later worker changes require fresh validation again.

Source inspection found numerous ignored tests whose comments are stale or describe missing seams rather than behavioral gaps. Example: poller history callback production metric behavior is already implemented in PollLifecycle::begin and PollWaitGuard::drop, although matching_partial_upgrade_b ignores the callback API test. Do not add unused callback plumbing simply to reproduce Go class structure. Audit actual observable behavior and map replacement tests.

Matching remains incomplete. Required: worker outcomes, current-head acceptance evidence (Cassandra especially), fresh workspace CI success, and requirement-by-requirement parity closure. Existing MySQL two-node pass alone does not prove completion.

## Integration candidate

Reviewed and integrated existing unmerged semaphore params/pagination, P1 semaphore startup, lifecycle-v2, and nodiff A/B branches into `matching-completion-integration-20261024`. No master merge yet: candidate requires CI review. Resolved overlapping semaphore constructor conflicts in favor of the required stop callback and paginated persistence API, preserving both behavior fixes. Prior semaphore branch CI failed three lifecycle tests; combined candidate must prove these now pass.

Whole-workspace candidate run: https://github.com/auenkr/cadence-rust/actions/runs/40073886465 (pending). Remaining branch work includes OpenFeature (assigned to a replacement Luna worker), error seams, panic recovery and tracing; inspect before declaring any blocker closed. Current final-parity audit merged to master in d71bef324; its remaining-gap list predates these integrations.

TLS source evidence in nodiff-b notes resolves the TChannel question: pinned Go supports rpc.tls only on gRPC (and HTTP configuration separately); Go TChannel listener uses no credentials. Rust need not invent a TLS mode Go does not have. Source guard is supplementary, not runtime TLS evidence.

## Additional integration and current failure

Integrated remaining sync-task construction/isolation tests (`matching-missing-tests-20261023`) and the optional frontend AutoConfigHint acceptance probe (`matching-auto-config-hint-20261024`) into candidate cae99549a. Probe is opt-in and not yet evidence of a pass. Its historical local-build instructions are superseded by current CI-only Cargo rule.

Recovery integration worker is actively fixing candidate syntax after run 40078703654 failed Clippy with mismatched closing delimiters near DescribeTaskList in grpc_matching.rs (not a behavioral test failure). Avoid parallel edits in that worker's owned recovery/error-seam files. All three current workers were confirmed live. Metrics migration policy is the next independent worker task once a slot opens; implementation brief is memory/matching-metrics-migration-next-20261024.md.

## OpenFeature handoff and remaining work

Recovered saved worker code2629578df into candidate f1f576915 after its model endpoint failed with Cloudflare1033. Important: despite prior green package run, source remains PARTIAL: Unleash constructor validates config/bootstrap then returns NOT_PORTED, and no Matching live property reads call OpenFeature. New combined whole-workspace run40079689926 exposed approx_constant lint on float3.14 in service-core tests. Fixed fixture to4.25 in dac76d6ff; new run https://github.com/auenkr/cadence-rust/actions/runs/40079827230 pending.

Two overly broad worker tasks returned research-only with no changes (metrics round1, Unleash engine round1). Not counted as implementation. Rescheduled metrics implementation explicitly and split Unleash into repository polling/lifecycle first, evaluation next. Preserve complete scope: even once repository exists, provider and Matching filter-aware evaluations are still required. No completion claim.

Current runtime config seam: cadence-service-core/admin.rs DynamicConfigOverlay::property_values returns borrowed snapshot slices; 57 Matching source occurrences consume this API through persistence/dynamic_config typed helpers. OpenFeature must evaluate per lookup with property-specific typed default and domainName/taskListName/taskType/clusterName attributes. A snapshot polling approximation cannot preserve filter-specific arbitrary property evaluations. Future integration should introduce a typed provider abstraction used by runtime readers rather than pretending an evaluated snapshot is equivalent.

## Cassandra acceptance and next deployment lane

Reviewed and merged Cassandra CI/evidence branch to master6a9a2ae37. Run40079714508 PASSED with acceptanceEligible=true, Go Matching absent, binary6276cb36698e1b1e7c178cf2e5d058a421dbd26f86c83da66a8fffae84f9fa95, source2432047f861f4ab68735e3b0d91c0a82303fb7d0. This is historical source-bound evidence, not a pass for subsequent master implementations. Node syntax check passed on merged probe. Probe fixes retain original assertions; owner observations detect rate-test ownership churn explicitly.

Started fresh-target PostgreSQL deployment lane against master6a9a2ae37: https://github.com/auenkr/cadence-rust/actions/runs/40080485161. Includes PG GC/domain, query standby, partition listing, durable restart/drain, AutoConfigHint and restart/failure recovery. No pass claimed yet. Do not start another deployment lane until it ends.

Current workers: metrics migration implementation (correcting mode/default/rollup review findings); Unleash repository lifecycle/tests (evaluation still separate); new live OpenFeature runtime read integration (recording fake provider and all consumers). All Luna medium/default speed. Read-only/research-only earlier results were not counted as implementation.
