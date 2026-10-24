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
## Unleash repository polling continuation

Prior branch `origin/t3/unleash-repository-polling` (source commit `9a673f33800e6edc07043980ce6e1edfb38ba6c9`) was merged into isolated branch `t3/repository-polling-tests`; it had added polling/bootstrap code but no fake-server tests. Its run https://github.com/auenkr/cadence-rust/actions/runs/40079967460 failed Clippy because `UnleashProvider.repository` was unread. Do not claim evaluation parity: flag evaluation, variants and payloads remain unported.

Go reference: `go/common/dynamicconfig/openfeatureprovider/unleash/unleash.go` delegates client lifecycle to unleash-client-go v4.5.0. `client.go` appends `/` to its configured URL before parsing; `repository.go` parses `./client/features` (optionally `?project=<name>`), sends app name, instance ID, refresh interval, user agent, client spec `4.3.1`, configured custom Authorization header and optional ETag, and cancels fetch context before joining shutdown. Rust now mirrors base path resolution, constructs reqwest blocking client on its poller thread (avoiding Tokio runtime drop panics), applies a 500ms request timeout, wires provider shutdown to stop/join, and uses a mutex-protected join handle. Added local TCP-server cases for bootstrap then refresh, route/query/headers, retaining cache after HTTP and JSON failures, and shutdown during a stalled request. They remain unverified until CI.

Go URL escaping was confirmed by the passing local-server assertion: `project blue` becomes `project%20blue` in the request URI. CI run https://github.com/auenkr/cadence-rust/actions/runs/40080227784 passed formatting, service-core Clippy and tests on SHA `90fe57bba998d8e91e93b17e94a38781a9a49a7d` (`clippy.exit=0`, `test.exit=0`); bootstrap/refresh, route/query/headers, cache retention, and stalled shutdown cases passed. CI formatting emitted a broad workspace patch because existing Rust files are not all rustfmt-clean; only the Unleash file's formatting was applied. Added a Tokio-current-thread construction/shutdown regression test. Fresh uncached CI run https://github.com/auenkr/cadence-rust/actions/runs/40080426528 also passed on `772a85ecbcbf360810c48fed8d33d3bcdef2dcc7`, but the current test review tightened cache assertions and made bootstrap response ordering deterministic; rerun a fresh service-core CI gate for the final tested SHA. Cargo has never been run locally. No merge to parent/master was requested or performed.

## Recovery continuation: polling integration and PostgreSQL failure

Integrated polling/lifecycle tests in master 34deee93f. Run https://github.com/auenkr/cadence-rust/actions/runs/40082925833 passed service-core clippy/tests for 4cf3b017270b7e93a63b3f1bd78d276ad89c0053, but its format step rewrote workspace sources before testing. Applied its Unleash formatting patch and changed formatting to an advisory check (existing workspace formatting debt remains); subsequent clippy/tests now test unchanged committed source. Fresh source-bound validation is required again.

PostgreSQL production run https://github.com/auenkr/cadence-rust/actions/runs/40080485161 failed at matching-service-compat.cjs:712: the crash-recovery workflow task never appeared as a persisted row within 20 seconds (queued_rows=0, range_id=2). The preceding direct owner probe completed; this is not a build failure. Downloaded full artifact to /tmp/matching-production-40080485161 for investigation. Do not weaken the queued-before-crash assertion or claim acceptance.

Three recovery tasks are confirmed running through task_status: live-reader-resume-v2, unleash-engine-resume-v2, metrics-resume-v2 (Codex Luna, medium/default). Their original worktrees were clean when inspected. No local Cargo command ran.

PostgreSQL diagnostic rerun: https://github.com/auenkr/cadence-rust/actions/runs/40083288791, source 9d56f0567. Added history and DescribeTaskList snapshots only on the strict pre-crash persistence timeout; Node syntax check passed. Still awaiting CI result. Service-core exact-source fresh run https://github.com/auenkr/cadence-rust/actions/runs/40083216641 uses 49fa0bf81.

Current-source inspection also confirms root load-time partition notifications are implemented around matching_registry.rs:1510-1565, including notify_read_partitions; the ignored partial-upgrade-e start-push comment is stale and must not be treated as proof of a missing production behavior. The remaining final audit must map this to meaningful runtime evidence.
Final repository-polling gate: https://github.com/auenkr/cadence-rust/actions/runs/40083481620 completed `success` for tested source SHA `086a938c6ddd22cbcfd680c915c7c6ce7cc5a7cb`. It used `package=cadence-service-core` and fresh uncached `/home/runner/work/_temp/cargo-target-final`; formatting, Clippy (`clippy.exit=0`) and tests (`test.exit=0`) all passed. Test log: service-core 143 passed, 0 failed; cadence-matching dependency tests 27 passed, 0 failed. The three new tests (Tokio runtime lifecycle, deterministic bootstrap/refresh + URL/header/query + HTTP/invalid JSON cache retention, and in-flight `FeatureProvider::shutdown`) all passed. The CI-resolved `rust/Cargo.lock` was committed at this SHA, so reqwest and transitive dependencies are pinned. No local Cargo commands were run. Branch remains `t3/repository-polling-tests`; pushed commits include merged prior branch and this continuation, with no parent/master push and no merge claim. Evaluation/variants/payload parity remains unimplemented.

## Configstore deployment continuation

Run https://github.com/auenkr/cadence-rust/actions/runs/40085504501 failed in
the ring-convergence probe before reading the secondary's owner view:
ECONNREFUSED 127.0.0.1:29915. Secondary log later confirms its gRPC/TChannel
listeners bound successfully (configstore startup completed after initial
primary discovery). Existing Promise.all let transient UNAVAILABLE escape the
bounded convergence loop. Fix1fc9ee1fd retries only UNAVAILABLE within the
existing30s deadline and checks secondary process liveness, preserving owner
agreement checks. Node syntax passed. Rerun:
https://github.com/auenkr/cadence-rust/actions/runs/40086025472 (pending).
Full failed artifacts: /tmp/matching-production-40085504501.
