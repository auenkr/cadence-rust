# Matching final parity audit (2026-10-24)

> Historical audit: its original blockers below are not the current backlog.
> The 2026-10-25 reconciliation table immediately below supersedes stale claims;
> final acceptance status lives in
> `memory/evidence/matching-final-candidate-20261024/README.md`.

## 2026-10-25 blocker reconciliation

| Original item | Current evidence / disposition |
| --- | --- |
| OpenFeature startup unsupported | Implemented: provider registration, typed live reader, Unleash repository/evaluator/lifecycle/backoff. Candidate workspace run40108643448 passed; see runtime and Unleash evidence notes. |
| Semaphore validation/paging/startup | Implemented and active tests, including acquire-during-load deadline, paginated scans and shutdown admission. Final Cassandra lane40153493941 remains running for candidate backend acceptance. |
| matching.errorInjectionRate alias | Original claim incorrect: canonical Go Matching client key, distinct from persistence injection. See source correction below. |
| Legacy non-durable Describe | Test-helper limitation; deployed Matching uses durable queues. Candidate PostgreSQL recovery/status probes passed; other backend lanes pending. |
| TLS modes | Original TChannel concern incorrect; Go has plain TChannel and separate gRPC/HTTP TLS. Candidate workspace includes listener TLS tests; candidate Thrift/TChannel lane40153127238 passed. |
| Owner ordering after membership changes | Closed by executable production Go handler/resolver versus Rust handler oracle, including add/remove/identity/address/empty-ring snapshots. Final candidate run40108643448 and generated artifact preserved. |
| Metrics source and live comparison | Source/rollup audit and actual Go tally/Rust metric comparisons pass on final candidate. Representative shared emitters and actual GetTasks metered-wrapper success/error paths covered; limits remain explicit in comparison report, with no invented coverage of every sampled value. |
| Current-source mixed acceptance | In progress on pinned27d3ff5f: fresh workspace, PostgreSQL and Thrift passed; Cassandra and remaining lanes tracked in candidate README. No completion sign-off yet. |

Root startup notification and file reload now have passing production-path tests.
Active offer has a real Go History/zero-insert mixed probe. Two typed History
refresh error integration placeholders are being replaced with actual wire tests.
All local Cargo/Go validation remains prohibited. The candidate stays pinned;
master adds evidence and test-only supplements while its production tree remains
identical unless an actual bug is found.

Scope: current Matching service only. Read-only code/evidence audit; no Cargo or runtime gate was run in this checkout. This is not a completion sign-off. Matching remains incomplete until the blockers below are closed and reproducible mixed-cluster evidence is bound to the then-current source and binary.

## Branches and stale inventories reconciled

- `55d8bc867` merges `matching-tests-port3-20261024`, `matching-testfix-20261024`, and `matching-twonode-20261024`; all three are ancestors of this checkout. The branches are also listed under `git branch -r --merged origin/master`.
- The A/B/C ambiguity in old inventories refers to partial-upgrade test batches, not three competing production implementations. A, B, and C are integrated (`54a8e84e1`, `f85da2173`, `56baa794d`); D and E also have merged port commits. Keep the row inventories as historical accounting, not as a current list of missing code.
- Current tests include the added ports in `matching_partial_upgrade_{a,b,c,d,e}.rs`, matching engine Go test files, and forwarder tests. Port3's 22 focused tests passed (`memory/evidence/matching-tests-port3-20261024/matching-test.exit` = 0); testfix recorded clippy and workspace-test exits 0 (`memory/evidence/matching-testfix-20261024/{clippy,test}.exit`). These prove build/test health for their recorded commits, not production parity.
- The two-node MySQL gate passed on GitHub Actions run [40071731050](https://github.com/auenkr/cadence-rust/actions/runs/40071731050), source `95ba26b852ee`, binary SHA256 `6e7c89d62612e93a90d3fa8497911eb31ef3533eac229d3f4fb55da8fbcab161`. It exercised two Rust Matching nodes with Go Frontend/History/Worker, SDK traffic, idle unload/recovery, SIGTERM drain, SIGKILL restart, crash recovery, typed ownership errors, query/isolation/partition dispatch, GC and domain metadata. This materially closes the old “two-node MySQL has never passed” claim. It is one run on an older source and is not current-head evidence; the gate also says acceptance eligibility is false by design and flakiness was not measured.
- The legacy count of 41 `#[ignore = "parity gap"]` tests from `matching-open-gaps-20261024-v2.md` is not a current inventory. That document itself says 8 were fixed on branches and 8 were only proposed accepted differences; merges since then added more ports. Recount and classify from current source before using any numeric ignored-test backlog. An ignored test is evidence of a gap only after checking whether its behavior was implemented through another test or a behavior-level gate.

## Concrete completion blockers

### Production behavior gaps

1. **OpenFeature dynamic-config provider is rejected at startup.** Rust Matching explicitly bails for `DynamicConfigProvider::OpenFeature` at `rust/crates/cadence-matching/src/launcher.rs:613-616`; Go registers the named provider plugin in `go/common/dynamicconfig/dynamicconfigfx/fx.go:103-119` (the error message names this reference). Deployments using a Go-supported OpenFeature provider cannot start Rust Matching. Port plugin registration/provider behavior or explicitly establish that this supported deployment mode is outside the compatibility target; no such exclusion is recorded.

2. **Distributed semaphore API/manager parity is still incomplete.** Current source retains parity-gap tests for parameter validation and scan pagination in `rust/crates/cadence-matching/src/matching_semaphore_parity_tests.rs` (verify current test lines by name); Go's manager API exposes validation and paginated scans in `go/service/matching/semaphore/semaphore_manager.go` and its tests. The older open-gap audit also identifies acquire-during-load deadline behavior (`rust/crates/cadence-matching/tests/matching_engine_go_tests_d.rs`, `acquire_semaphore_request_during_load_gives_up_at_its_own_deadline`) against Go `semaphore/semaphore_manager.go` startup path. Existing grant/deadline unit tests and two-node runtime ownership evidence do not cover these input, pagination, and startup failure semantics. Close each behavior or document a justified API-level difference; do not count tests alone as the fix.

3. **Rust-only persistence fault-injection config key remains.** Rust reads `matching.errorInjectionRate` at `rust/crates/cadence-matching/src/launcher.rs:700-710` and installs it in `matching_fault_injection.rs:240`; Go uses `system.persistenceErrorInjectionRate` (`go/common/persistence/config.go:66`). This creates a Rust-only way to inject errors and can change behavior when a deployment supplies the otherwise unknown key. Remove the alias or record an intentional compatibility exception and test the canonical Go key.

4. **Non-durable DescribeTaskList still lacks durable queue status semantics.** The current non-durable status path and its explicitly ignored parity tests are in `rust/crates/cadence-matching/src/grpc_matching.rs` (DescribeTaskList) and `rust/crates/cadence-matching/src/matching_rpc_tests.rs` (`matching_rpc_non_durable_describe_reports_task_id_block_and_empty`). Go derives `TaskIDBlock`, `Empty`, ack/read levels, backlog and rates from its task-list manager/ack manager in `go/service/matching/tasklist/task_list_manager.go:767-799,983-988`. Rust's in-memory non-durable queue has no range lease/ack manager, so it cannot report equivalent values. The describe-gaps note closes poller rate/isolation reporting only for its implemented paths; it explicitly leaves task ID block/empty and arrival/limiter rates unresolved (`memory/evidence/matching-describe-gaps-20261024/notes.md`). This is a behavior gap, not merely stale test debt.

5. **TChannel transport security parity has no implementation/evidence.** The prior audit found gRPC and HTTP TLS paths/tests but no TChannel TLS handshake implementation/test (`memory/matching-open-gaps-20261024-v2.md`, P1-15). Go transport setup is in `go/service/matching/service.go` and TChannel bootstrap; Rust listener/bootstrap is `rust/crates/cadence-matching/src/tchannel.rs` and `ringpop_runtime.rs`. Verify against the exact Go deployment configuration and either implement the supported TLS mode or record an explicit deployment exclusion. This remains a concrete production compatibility question, not proof that every deployment requires TLS.

### Behavior not yet demonstrated sufficiently

6. **Task-list partition owner ordering is unresolved.** The current Go-parity test is `rust/crates/cadence-matching/tests/matching_engine_go_tests.rs::list_task_list_partitions_success_owner_hosts_match_go_lookups` and is still marked parity-gap in the source history. Compare Rust `grpc_matching.rs` ListTaskListPartitions path and `matching_membership.rs`/`matching_ringpop_provider.rs` owner lookup with Go `go/service/matching/handler/engine.go` ListTaskListPartitions and ring membership. Needed: same-membership Go/Rust fixture asserting ordered owner hosts and behavior after membership changes. Ordinary two-node workflow ownership success does not establish RPC ordering parity.

7. **Production metrics parity audit remains incomplete.** `memory/matching-go-parity-audit-v4-20261023.md` identified missing persistence error/response-size metric rows and no Go-vs-Rust emitted name/tag comparison. Reconcile current emit sites (`rust/crates/cadence-matching/src/matching_persistence_limiter.rs`, `rust/crates/cadence-service-core/src/matching_metrics.rs`) with Go `go/common/persistence/wrappers/metered/base.go`; record parity for names, dimensions, and emission conditions. Unit compile success is not evidence that operators see equivalent metrics.

8. **No mixed-cluster acceptance evidence is bound to current source.** The strongest named MySQL two-node run above was built at `95ba26b852ee`, while this checkout is `fb8bb1dbbfd7` and `origin/master` is `425f50562d6b`. Port3/testfix unit evidence is likewise from their branch heads. Required completion evidence must use current source, a fresh uncached CI runner target, record head SHA and Rust binary SHA256, and run the acceptance mixed-cluster gate(s) with Go Frontend/History/Worker and no Go Matching. Cover existing SDK workflows, service-to-service calls, backend/schema compatibility, forwarding, failures, restart/recovery, graceful shutdown, and the supported persistence backends/configuration. Repeat flaky lanes sufficiently to establish stability. The successful two-node run upgrades confidence but does not satisfy this binding requirement.

## Current evidence and limits

- Integrated port/test changes: `memory/evidence/matching-tests-port3-20261024/matching-test.exit` = 0; `memory/evidence/matching-testfix-20261024/clippy.exit` = 0 and `test.exit` = 0. These are compile/unit checks only.
- Two-node MySQL runtime: `memory/evidence/matching-twonode-20261024/run1/matching-mixed-gate/gate.exit`, `gate/lane.exit`, and `gate/run/run.exit` all = 0; exact run, head, binary hash, scenarios, and single-run caveat are in `memory/matching-twonode-20261024.md`.
- Earlier Cassandra/MySQL/PostgreSQL/Thrift gates in older audits are historical unless their source head and binary hash are shown to match the candidate being accepted. Do not inherit a green result across later Matching changes.
- No Cargo command was run for this audit, per request. No behavior was changed.

## Handoff to orchestrator

Prioritize OpenFeature deployment compatibility, semaphore input/pagination/startup semantics, non-durable queue-status behavior, and current-head mixed-cluster acceptance. Also close partition-owner ordering and metrics evidence. Recheck the Rust-only fault-injection alias before release. No concrete small code bug was fixed here; this was an evidence-backed audit, and no implementation was made to avoid overlap.

## Orchestrator corrections after checking primary source

The initial audit's blocker 3 is incorrect: `matching.errorInjectionRate` is a canonical Go key, NOT a Rust-only alias. See `go/common/dynamicconfig/dynamicproperties/constants.go` entries `MatchingErrorInjectionRate` and `go/client/clientfactory.go:183-185`, which read it once and wrap the Matching client. Rust's `matching_fault_injection.rs` documents this correctly. `system.persistenceErrorInjectionRate` controls a different persistence wrapper. Do not remove the Matching client key.

Blocker 5's TChannel TLS question is resolved by primary Go source: `common/config/config.go` limits rpc.tls to gRPC, `common/rpc/factory.go` creates a plain TChannel transport, and HTTP TLS has separate configuration. Rust must preserve these modes; TChannel TLS is not a missing Go feature. See integrated nodiff-b evidence notes for exact source references.

Blocker 4 describes the legacy non-durable test object, not a production Matching deployment. The launcher constructs MatchingRuntime for every `run_matching` path, and its configured source uses durable queues; even explicit embedded development uses a durable SQLite queue source. The fallback `Matching::new` is reached when no Matching runtime was requested. The ignored test still records a helper limitation, but it does not establish that deployed Rust Matching lacks lease/status behavior. Verify supported production sources in acceptance; do not invent range leases in the legacy frontend-state test object as a substitute.

Fresh whole-workspace strict clippy/tests passed for 425f50562 in run 40073658725; evidence under `memory/evidence/matching-fresh-ci-20261024`. Later integration changes remain unverified. Semaphore validation, paging and startup changes now exist in the candidate; it is not yet merged to master. Candidate run 40073886465 failed three fixture assertions because a lifetime scan counter was mistaken for a page index across replacement manager loads; fix 8bc884e58 separates these counters while retaining token assertions. No production behavior was changed to hide these failures.
