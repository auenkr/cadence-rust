# Matching acceptance status — 2026-10-27

**Authority:** this is the current concise status. It supersedes conflicting
status and blocker claims in `matching-final-acceptance-ledger-20261026.md`,
`matching-integration-control-20261027.md`, and
`matching-resume-checkpoint-20261027.md`. Those notes and their historical
artifacts remain unchanged as provenance.

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

Merged baseline branch `t3/matching-cold-cache-metrics` at005b7849f was verified
an ancestor of pushed master, clean and inactive, then its worktree
`/home/auenkr/.t3/worktrees/cadence-rust/t3-6d878a3e`, local branch and remote
branch were removed. Source and review evidence remain in master.

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
