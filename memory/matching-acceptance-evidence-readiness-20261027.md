# Matching final acceptance evidence readiness — 2026-10-27

Current-source corrections: the shard-distributor production lane now passes
--real-shard-distributor and runs the real Go service with single-node etcd,
rather than the earlier handler-only fixture described below. Verify the exact
current artifacts against matching-v7-transport-discovery-evidence-guide-20261027.md.
The TLS probe now retains matching-tls-handshakes.json; see
matching-tls-runtime-evidence-20261027.md. Historical collector comments below
describe the source pin recorded in this original review.

Read-only workflow/collector review at source `64e709b96c398bb1613fd2a9a9b83a8d5babf6f4` (branch `t3/acceptance-evidence-map`, same tree as master at review). No gate was dispatched or polled; no local Cargo, Go, build, test, format, or runtime command was run. The ten required lanes in `matching-acceptance-status-20261027.md` remain unchanged and mandatory. This note is a dispatch/review map, not a pass claim.

## Dispatch and common credit rule

Freeze one full immutable `FINAL_SHA` before dispatch. Dispatch `--ref <branch>` only while its resolved SHA is that value; after each run verify both Actions `headSha` and downloaded `out/commit.txt` equal `FINAL_SHA`. One differing SHA invalidates cross-lane final-pin credit. Workflow conclusion, build exit, gate exit, nested `run.exit`, `results.json` result, and scenario-specific evidence must all agree. Save each run URL and artifact under `memory/evidence/` or the established external CI evidence directory, and enter the artifact paths in the ledger below. For every mixed lane record `out/binary.sha256` (or the documented `binaries.json` SHA for captured harness evidence), topology proving Go Frontend/History/Worker plus Rust Matching and absence of Go Matching, the actual SDK/probe outcomes, and resource cleanup. Do not turn a historical `acceptanceEligible:false` into a pass; it is descriptive runtime metadata, not a promotion flag.

Serial order below follows the ten checklist rows. The workspace CI is a separate same-pin prerequisite, not an eleventh mixed lane. Commands assume `FINAL_REF` names the frozen commit and repository is `auenkr/cadence-rust`.

## Ten-lane command and artifact map

All mixed gates run serially. Use the matching artifact name listed and retain the entire uploaded `out/` tree (short retention is three days, so download promptly).

| # / lane | Exact dispatch | Expected artifact / required review |
|---|---|---|
| 1. PostgreSQL | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=postgres` | `matching-production-postgres`. Verify `out/commit.txt`, `binary.sha256`, `build.exit`, `gate.exit`, `command.txt`; nested `out/gate/run/results.json`, `run.exit`, `probe.log`, PostgreSQL config/evidence. Require PostgreSQL shared backend, Go F/H/W + Rust Matching/no Go Matching, SDK suite, PG GC-domain, query standby, list partitions, durable restart and auto-config-hint assertions. Inspect restart/recovery actual results and `postgres-container-cleanup.txt`/gate cleanup. No PG-specific compact DB observation is normally emitted (`productionDatabaseEvidence` can be “not collected”); that is not a missing assertion. |
| 2. MySQL probes | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=mysql-probes` | `matching-production-mysql-probes`. Verify identity/hash/exits and nested `results.json`, `probe.log`, `db-observations.json`, configs. Require MySQL Go schema only, SDK suite, backlog forwarding, query standby, list partitions, durable restart and auto-config hint; actual restart/drain/recovery from result/probe, and `mysql-container-cleanup.txt`. |
| 3. Thrift | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=thrift` | `matching-production-thrift`. Verify exact identity/hash/exits and `results.json` records Matching transport `thrift`, History transport `tchannel`; inspect SDK and service logs plus restart, two-node recovery, idle recovery and auto-config-hint evidence. Confirm Go F/H/W/no Go Matching and MySQL cleanup. |
| 4. Configstore | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=configstore` | `matching-production-configstore`. Verify exact identity/hash/exits, selected configstore mode and its seed/config artifacts, `results.json`, SDK/probe/service logs; require SDK suite, restart, idle and two-node failure recovery outcomes, plus teardown/cleanup evidence. |
| 5. Shard distributor | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=shard-distributor` | `matching-production-shard-distributor`. Verify exact identity/hash/exits, `results.json`, shard-manager fixture logs/artifacts, SDK/probe and both Rust node logs. Require actual fixture-backed shard assignment/routing, TTL expiry, assignment cleanup, restart, idle and two-node crash recovery. The workflow tests pinned Go shard-manager handlers over an in-memory store, not a full etcd-backed Go shard-distributor service; report that scope accurately. Confirm fixture process-group stop and MySQL container cleanup. |
| 6. Two-node MySQL | `gh workflow run matching-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | `matching-mixed-gate`. Default args request restart, failure recovery, idle interval 3. Verify `out/commit.txt`, `binary.sha256`, cargo/gate exits, `command.txt`, nested `results.json`, `probe.log`, DB observations, two Rust Matching instances in topology. Require SDK suite and observed SIGTERM drain/recovery plus owner SIGKILL, survivor takeover, persisted task/ownership assertions; inspect MySQL cleanup. |
| 7. Cassandra | `gh workflow run matching-cassandra-mixed-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f extra_args='--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3'` | `matching-cassandra-mixed-gate`. Verify identity/hash/build/gate exits and nested `results.json`, probe, Go-schema observations, semaphore/task-row interop output. Require Go Cassandra schema shared by Go F/H/W and Rust Matching; SDK suite; two-node owner kill/takeover, restart/drain and persisted task/semaphore owner+token checks. Confirm only its runner-owned Cassandra container was removed (`cassandra-container-cleanup.txt`). The runner setting `acceptanceEligible:true` is not itself proof; scenario data is. |
| 8. Corrected adaptive | `gh workflow run matching-production-validation.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f lane=adaptive` | `matching-production-adaptive`. Verify identity/hash/exits, `results.json`, adaptive probe transcript/load partitions/scaler events and Rust server logs. Require unchanged SDK assertions and actual adaptive scenario completion plus the common restart, two-node failure recovery and idle recovery from common args; confirm MySQL cleanup. Do not substitute `adaptive-single-diagnostic`, `adaptive-go-control`, retry-only, or earlier adaptive runs. |
| 9. Active offer | `gh workflow run matching-active-offer-gate.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | `matching-active-offer-gate`. Verify identity/hash/build/gate and nested run exits, `results.json`, `activeOffer.json`, `probe.log`, Rust Matching logs. Require actual active-offer delivery by Go History (scheduled task/token), zero persistence rows/inserts as asserted, SDK suite, graceful drain/restart and two-node crash recovery. Confirm MySQL cleanup. |
| 10. TLS mixed | `gh workflow run matching-production-tls-mixed.yml --repo auenkr/cadence-rust --ref "$FINAL_REF"` | `matching-production-tls-mixed`. Verify identity/hash/build/gate/nested exits; `results.json`, `probe.log`, Rust logs, redacted TLS config and config hashes. Require CA-backed hostname-verified TLS on the production mixed topology (not merely TLS flags), Go F/H/W plus Rust Matching/no Go Matching, SDK suite, restart/drain and two-node failure recovery, and MySQL cleanup. Never include private keys/password material in retained evidence. |

### Same-pin workspace prerequisite

`gh workflow run rust-ci.yml --repo auenkr/cadence-rust --ref "$FINAL_REF" -f clean_target=true` (leave package empty). Download `rust-ci-logs`; require run `headSha == FINAL_SHA`, fresh target path, `clippy.exit=0` followed by `test.exit=0`, and record URL, SHA and conclusion. Preserve `format.exit`/format logs as reported, but the workflow's format step is advisory/continue-on-error and is not a replacement for the specified Clippy-then-workspace-tests acceptance. Do not run this concurrently with a mixed gate.

## Review limits and collector findings

The workflows have per-run concurrency cancellation, hosted `ubuntu-latest`, 90-minute limits and three-day artifact retention. The production validation workflow and MySQL/Cassandra/active-offer/TLS workflows record the checked-out commit and SHA256 of the built `cadence-matching`; each uploads `out/` while excluding generated executables and sensitive config paths. The mixed harness `tests/matching_evidence.py` records repository head/status in `source.json`, Rust/Go binary SHA256 values in `binaries.json`, results, command, logs and redacted configs. The MySQL/Cassandra wrapper cleanup traps write cleanup output. Therefore the reviewed collectors provide the basic source-pin, binary-identity, scenario, and cleanup material needed for these lanes; no collector code change is justified by this review.

Credit is impossible when an artifact is absent/truncated before the relevant checks, when its recorded commit or binary SHA cannot identify the tested process, when only requested flags appear without actual outcome evidence, when Go Matching is present or any required Go peer is absent, when SDK/recovery outcomes are missing or failed, when cleanup is unverified, or when run/head/artifact pins differ. Such a lane remains pending/failed; do not infer missing output from a green job. No new service blocker or parity requirement is introduced here.

## Compact final-pin ledger template

Fill from downloaded evidence only; do not pre-mark any row. `Result` is `pending` until independently reviewed, then `pass` or `fail`.

```text
FINAL_SHA: <full immutable commit>
Rust CI: URL=<...> head=<...> conclusion=<...> target=<...> clippy.exit=<...> test.exit=<...> format.exit=<...>

| # | Lane / run URL | head == FINAL_SHA | artifact commit == FINAL_SHA | result | binary SHA256 | topology + backend/transport/discovery proof | SDK/scenario + restart/recovery proof | cleanup proof | reviewer/artifact path |
| 1 | postgres | | | pending | | | | | |
| 2 | mysql-probes | | | pending | | | | | |
| 3 | thrift | | | pending | | | | | |
| 4 | configstore | | | pending | | | | | |
| 5 | shard-distributor | | | pending | | | | | |
| 6 | two-node MySQL | | | pending | | | | | |
| 7 | Cassandra | | | pending | | | | | |
| 8 | adaptive | | | pending | | | | | |
| 9 | active-offer | | | pending | | | | | |
| 10 | production TLS | | | pending | | | | | |

Matching acceptance: pending until all ten exact-pin rows and same-pin workspace
CI pass and the separate Matching parity audit is closed.
```
