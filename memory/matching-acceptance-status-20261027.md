# Matching acceptance status — 2026-10-27

**Authority:** this is the current concise status. It supersedes conflicting
status and blocker claims in `matching-final-acceptance-ledger-20261026.md`,
`matching-integration-control-20261027.md`, and
`matching-resume-checkpoint-20261027.md`. Those notes and their historical
artifacts remain unchanged as provenance.

## Active combined candidate v4 (09:40 UTC)

Immutable tag `matching-acceptance-20261027-v4` points to
`64e709b96c398bb1613fd2a9a9b83a8d5babf6f4`, including typed name-lookup RPC
mapping, regression fixes, dual exporter capture and late-domain name probes.
Use this tag for subsequent serial lanes; documentation commits may follow on
master. No final acceptance pass is credited yet.

- Fresh uncached workspace CI: https://github.com/auenkr/cadence-rust/actions/runs/40482431857
- Two-node MySQL, Prometheus and late-domain probes: https://github.com/auenkr/cadence-rust/actions/runs/40482436756

Both were dispatched at the candidate SHA and remain pending review. Duplicate
automatic workspace run40482429103 was cancelled in favor of the explicit
fresh-target run. Earlier name-readthrough focused run40481424583 failed
compilation of its regression (Result/Option assertion); candidate64e709b96
corrects that and decodes tonic status detail bytes with prost. Source and
runtime results still require actual CI artifacts. Never infer pass from fix.

## Latest follow-up (09:37 UTC)

Adaptive single-node diagnostic [40480825761](https://github.com/auenkr/cadence-rust/actions/runs/40480825761)
**succeeded** at `d8759bd09c4b74908d77818a14e5a9f15a3aab4a`.
Downloaded artifact verifies build/gate/runtime/probe exits0, Go
Frontend/History/Worker and no Go Matching, load24/24, scaling1->12 write/read
partitions, SDK completion and process restart. Binary SHA256:
`5076795be1078e189066c1be5d3148026638f933e5c6a8000b7a877140059ff8`.
Compact proof: `memory/evidence/matching-adaptive-single-40480825761/`.
The unchanged assertions passed once; this does not prove the prior delay fixed,
nor two-node recovery or final-pin acceptance. Worker comparison is pending.
Combined workspace CI40480829938 was cancelled by later master pushes during
its test step; it is not a passing combined validation result.

Native name read-through9bca9b856, cache source classification7a159cea8,
dual-exporter capture/reviewd0b68d53e/5860bf54e and late name probe8160da894
are integrated on master. Typed RPC mapping remains in progress; source
classification alone does not fix the handler's all-not-found conversion.
Optional live name probes retain the shared-cache/background-refresh limitation.
All ten final-pin acceptance lanes remain required.

## Immutable pins and current state

| Item | Pin / result | What it establishes |
|---|---|---|
| Current memory source | master `ec3c894428a61f5d172aaf35a13738c5105a7ccf` | Documentation-only update; current Rust production Matching source remains unchanged since candidate v3. Startup/control/fixture edits do not prove Rust runtime behavior. |
| Candidate v3 | tag `matching-acceptance-20261027-v3`, SHA `ff3b638b49190f7653c2de3b062232d7ccc57f07` | Candidate baseline for existing runtime evidence; not service acceptance. |
| Fresh workspace CI | run `40467005617`, candidate v3 | Passed at its pin; does not validate later source changes or replace mixed gates. |
| PostgreSQL mixed | run `40468419712`, candidate v3; binary SHA256 `adb469745e34a617fb0377d69b4c7f40a79432a3d46eef3fbb7613a59fc7f678` | Passed SDK workflow, drain/recovery, restart, PostgreSQL, forwarding and partition scenarios with Go Frontend/History/Worker and no Go Matching. Artifact remains `acceptanceEligible:false`; closes only this lane. |
| Retry/adaptive diagnostic | run `40469125507`, tag `matching-retry-trace-20261027-v1`, SHA `7d072c25422c165c92e6912d4210de7b8bb741a4`; binary SHA256 `c347383fda712d9c1da14c96a63a61116fc2240cf42e2064e207dd1b3f9e42e0` | Failed later in activity-timeout after isolated retry and other workflows completed. Instrumentation showed retry adds reached Matching and tokens returned; activity-timeout's later decision missed SDK deadline. Diagnostic only, not acceptance. Retry timeout is an attempt timeout, not a pre-scheduling delay. |
| Go cache caller oracle | run `40470232857`, original-Go overlay at `b93b162061cb32b62caecc22673241579edbec30` | Passed with mocked successful persistence existence and canonical refresh rows. Proves raw cache-key caller behavior only; it is not backend UUID parsing or Matching handler metric/quota attribution. |
| Thrift startup failure | run `40469948997`, candidate v3; binary SHA256 is in its evidence directory | Failed before Matching scenarios: original Go Worker died during concurrent system-domain registration. This is not a Rust Thrift transport acceptance result. Worker readiness coordination was added to the harness afterward; this edit alone proves no runtime behavior. |
| Original-Go control, first attempt | run `40472035315`, `fed7fd7fa41e9b444439d9439686ff4628d20368` | Failed provisioning (`DefaultDomainCache ... domain exists but cannot be loaded`) before adaptive scenarios; all-Go control only, not Rust evidence. |
| Integrated fresh workspace CI | `40472031580` on `fed7fd7fa41e9b444439d9439686ff4628d20368`, success | Downloaded exact identity, fresh-target/cache-skip steps and Clippy/workspace test exits0 retained under matching-clean-workspace-40472031580. Later fixture corrections are not included; no mixed acceptance. |
| Original-Go control, endpoint failure | `40472428123` on `9a3673d0de3284e3bda434c4f722a412f753eb75` | Scaling grew12->72->216, but the SDK targeted Matching TChannel for public Frontend calls and never started its workflow. Comparison remains unproved. |
| Successful runtime comparison | all-Go adaptive control `40473213335` on `fc359c05e16b2abff94b757c7d301986e6b93572`, success | Downloaded artifact reports adaptive passed, initial load24/24, completed SDK suite and live partition routing. Original Go Frontend/History/Matching/Worker; `acceptanceEligible:false`. One Go Matching instance does not establish two-node Rust parity. Independent evidence review underway. |
| Repeated Rust adaptive failure | `40474255482` on `5808e205a1b58b2169084b2e46eaea219f8b7cf2`, failure | Full SDK failed at activity-timeout: only WorkflowExecutionStarted and DecisionTaskScheduled in captured history. Workflow-task-retry completed in19.894s; activity-timeout started with14.823s suite budget and failed after14.778s. Rust binary SHA256468f9306660ffd1d1693be8d43995187e52fabbfd682059a488ddc35cc05ae9c; compact artifacts retained under matching-adaptive-40474255482. MySQL cleanup output retained. This is delivery lag evidence, not an established source defect. |
| Failed one-node Rust comparison | `40476007015` on `463fd92220f6f2e9b8df64fe0dcbed4576c04564`, failure | Retained under matching-adaptive-single-40476007015. Initial load finished quickly, then activity-retry18.486s, child11.784s, workflow-run-retry20.564s; workflow-task-retry exhausted remaining7.344s. This disproves any claim that two Rust instances are necessary for the observed adaptive delay. It does not identify its source. Diagnostic only; two-node acceptance remains required. |
| Failed traced one-node diagnostic | `40476881898` on `a5276f33659c6a1cea47f157d909983c2ce0885e`, failure | Retained under matching-adaptive-traced-single-40476881898, including570 trace events. Activity-retry20.280s, then child exhausted38.035s. Child parent decision tokens returned, but child completion is not proved. Durable-reader dispatch remains an instrumentation gap. |
| Failed late-domain live probe | `40478120133` on `c06aa9b1e70632e216a2bf868c89b06d0c44fc0e`, failure | Both late-domain AddDecisionTask calls returned NotFound, before quota rejection. Cache read-through learned the record but its Go-compatible change callbacks omit new entries; Rust's separate consumer domain view was not populated. Retained under matching-cold-domain-40478120133. Recovery phases were not reached. |
| Late-domain fix runtime, gate still failed | `40479417510` on `7a731e61a728747642b4e1a6d221dfabc8e8b351`, failure | Actual request outcomes0 and8 plus positive canonical samples for both task lists retained under matching-cold-domain-40479417510. SDK/restart/drain/recovery preliminary result passed. Overall gate failed exporter validation: expected RespondQueryTaskCompleted sample absent from captured primary exporter. Do not mark lane accepted. Capture/routing review required. |
| Active full-boundary adaptive diagnostic | `40480825761` on `d8759bd09c4b74908d77818a14e5a9f15a3aab4a`, queued at dispatch | One-node adaptive with unchanged SDK assertions, durable-reader traces, poll correlation, Add startup timings, bounded child histories, and atomic missing-only cold publication. Diagnostic, not two-node acceptance. Fresh combined workspace CI40480829938 also dispatched at this pin. |
| Passing integrated focused CI | `40477160902` on `52a42dd453cb52d280d2f85988f50177924507aa`, success | Downloaded Clippy/test exits0, fresh target path and all three cold-domain regression successes retained under matching-focused-40477160902. Package cadence-matching only, not final workspace acceptance. Later probe quota edits are outside this pin. |
| Integrated cold canonical metric fix | combined master `201a70b5e`, baseline code `c2e631b4383a2b61313654d47588da727f058bdb`, review fix `32ae5527e`, regression `0aeb53989` | Baseline focused CI40472811597 passed. Reviews led to preserving resolver panic capture and reusing the first semaphore domain resolution. The stronger regression invokes AddDecisionTask through the real cache after an empty initial scan. Fresh combined workspace CI40474658191 is pending; earlier combined CI40474523522 is superseded. Current adaptive run does not include this fix. |

## Superseded claims and evidence boundaries

### Latest integrated validation follow-up

The stronger regression's focused run40474488446 failed Clippy on
`field_reassign_with_default`; master51ba24f8a initializes the request directly.
Source review then found the test fixture's row notification version equaled
the exclusive metadata bound, which correctly prevents cache insertion.
Master866b210d4 raises that mock bound above the row version. Replacement
fresh workspace CI (also includes Go SDK fixture and canonical Go cache oracle):
https://github.com/auenkr/cadence-rust/actions/runs/40475730254,
source866b210d4 (pending at dispatch). Runs40474658191 and40474883530 are
superseded. No runtime result is inferred from either fixture correction.

Subsequent test runs40476193408 (older trace branch) and40476277002
(resolver regression branch) failed tests, not compilation. The former still
had the superseded metadata-bound fixture; the latter's ordinary resolver-miss
case expected InvalidArgument but correctly reached domain quota resolution
and returned NotFound. Master52a42dd45 fixes that test expectation; its
captured resolver-panic case passed. Combined focused clean-target CI:
https://github.com/auenkr/cadence-rust/actions/runs/40477160902
at52a42dd45 (pending at dispatch), including Go SDK fixture checks.
Later traces and optional probe additions still require final workspace CI.

Merged baseline branch `t3/matching-cold-cache-metrics` at005b7849f was verified
an ancestor of pushed master, clean and inactive, then its worktree
`/home/auenkr/.t3/worktrees/cadence-rust/t3-6d878a3e`, local branch and remote
branch were removed. Source and review evidence remain in master.

Also removed clean, inactive, merged worktrees/branches after verifying their
remote heads in `git branch -r --merged origin/master`:
`t3/matching-single-lane` atdb5ca9f984b1e940c83278eda3016114b3af59d1
(`/home/auenkr/.t3/worktrees/cadence-rust/t3-74d71833`) and
`t3/go-domain-cache-uuid-oracle` at121ba50d616f9527eb5759f10c8e3ce2ab1f4362
(`/home/auenkr/.t3/worktrees/cadence-rust/t3-d52880f4`). Local and remote refs
were deleted; commits and evidence remain in pushed master. Their completed
worker tasks were no longer using either worktree.

- **Cassandra normalization is not a mandatory acceptance change.** The Go
  oracle confirms raw-string cache lookup after the persistence existence
  check. The prior ledger's requirement to normalize recognized UUID spellings
  is withdrawn; preserve Go's raw-cache-key behavior. Do not reopen the held
  normalization patch based on the old claim.
- **Retry scheduling-delay is not a blocker or established behavior.** The
  one-minute value is the timeout of an already-started decision attempt, not
  a deliberate wait before scheduling the retry. The adaptive trace observed
  retry delivery. Its later activity-timeout failure remains a failed
  diagnostic outcome; do not relabel it as proof of a Rust/Go mismatch or a
  harness defect.
- Keep three cache/metrics questions separate: the Go oracle above mocks
  persistence and observes cache caller behavior; backend UUID parsing is a
  distinct concern; canonical cold-cache metric attribution is a source trace
  and handler telemetry is a separate runtime concern. None of those latter
  two is established by the mocked oracle. No normalization requirement follows
  from metric attribution.
- The Thrift failure is Go Worker startup, not transport acceptance. The Go
  readiness fixture change and the adaptive control provisioning wait change
  are setup changes, not Rust runtime evidence. Only inspect their resulting
  runs for what those runs actually exercised.
- Do not add speculative blockers (physical internal partition identity,
  exhaustive RPC error-branch mixed calls, or separate scaler-unload probe)
  without a concrete source-backed mismatch or an explicit required contract.

## Only remaining acceptance checklist

### Newly verified cold name-lookup gap

Parent inspection found GetTaskListsByDomain and ListTaskListPartitions resolve
domain names directly from the consumer InMemoryDomainStore, bypassing the
UUID metric resolver. Go engine.GetTaskListsByDomain and getAllPartitions call
domainCache.GetDomainID. Thus a fresh name-based call can still miss a late
domain even after the UUID read-through publication fix. The delegated
entry-point audit's no-gap conclusion was incomplete: it explicitly did not
inspect grpc_matching.rs. A focused name-read-through fix/regression is needed;
do not mark all cold-domain entry points closed from the earlier audit.

Matching remains **not accepted** until all ten required exact-final-pin mixed
lanes below pass and their artifacts are reviewed. Each lane must show Go
Frontend/History/Worker, Rust Matching, no Go Matching, completed SDK and
service assertions applicable to that lane, actual failure/restart/recovery
results (not merely requested flags), cleanup, immutable head SHA, and Rust
Matching binary SHA256. Preserve `acceptanceEligible:false` on historical
artifacts; do not promote them retroactively.

1. `matching-production-validation.yml`: `postgres`
2. `matching-production-validation.yml`: `mysql-probes`
3. `matching-production-validation.yml`: `thrift`
4. `matching-production-validation.yml`: `configstore`
5. `matching-production-validation.yml`: `shard-distributor`
6. `matching-mixed-gate.yml`: two-node MySQL
7. `matching-cassandra-mixed-gate.yml`
8. `matching-production-validation.yml`: corrected `adaptive`
9. `matching-active-offer-gate.yml`
10. `matching-production-tls-mixed.yml`

Also require a fresh, uncached dedicated-target `rust-ci.yml` workspace run on
the same final source pin, with Clippy followed by workspace tests. Record its
URL, head SHA and conclusion. For each mixed lane record URL, head SHA,
conclusion, scenario results, topology and binary SHA. Run gates serially and
review artifacts before marking each checklist item complete. The PostgreSQL
candidate result is historical support, not a checked item for a different
final pin. The HTTP/TLS History fixture is supplemental and is not one of these
ten lanes.

When, and only when, fresh CI and all ten final-pin mixed lanes meet these
criteria and the Matching parity audit is closed, update this table with the
actual evidence and mark Matching complete before starting History migration.
