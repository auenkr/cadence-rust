# Project memory

`memory/` stores durable project context: service parity findings, design and migration decisions, progress summaries, and reproducible CI or mixed-cluster evidence. Check the current service status before continuing work; evidence applies to the exact source revision recorded in each report.

## Current and reference notes

| File | Summary |
| --- | --- |
| [progress.md](progress.md) | Chronological record of significant implementation and validation milestones. |
| [project-goals.md](project-goals.md) | Compatibility requirements and project constraints. |
| [service-migration-plan.md](service-migration-plan.md) | Service-by-service migration sequence and replacement gates. |
| [service-map.md](service-map.md) | Service definitions, listeners, and implementation boundaries. |
| [migration-inventory.md](migration-inventory.md) | RPC surface and remaining migration scope. |
| [api-compat.md](api-compat.md) | Definition of SDK, wire, and persistence compatibility. |
| [monorepo-layout.md](monorepo-layout.md) | Repository and source layout. |
| [port-ledger-summary.md](port-ledger-summary.md) | Summary of Go-to-Rust source coverage. |
| [all-rust-cluster-20261114.md](all-rust-cluster-20261114.md) | Cluster status checkpoint and validation context. |
| [history-parity-audit-20261114.md](history-parity-audit-20261114.md) | Latest History parity audit in the top-level notes. |
| [frontend-final-review-opus-20261114.md](frontend-final-review-opus-20261114.md) | Frontend final review findings. |
| [matching-v7-final-acceptance-ledger-20261027.md](matching-v7-final-acceptance-ledger-20261027.md) | Matching v7 acceptance ledger and evidence pointers. |
| [performance-status-20261119.md](performance-status-20261119.md) | Current performance investigation summary. |
| [evidence/bench-rerun-20261121.md](evidence/bench-rerun-20261121.md) | Paired Go/Rust benchmark measurements and run links. |

Service-specific notes remain at the top level and are grouped by filename prefix: `matching-*`, `frontend-*`/`fe-*`, `history-*`, and `ringpop-*`. Other notes cover persistence, transport, CI/infra, benchmarks, orchestration, and branch cleanup. Search by topic or use the detailed evidence reports linked from each note.

## Archive and evidence

- [archive/](archive/) contains older notes superseded by later checkpoints, validation runs, or acceptance ledgers. Files are moved with Git history preserved.
- [evidence/](evidence/) contains reproducible CI, compatibility, mixed-service, recovery, soak, and benchmark records. Markdown evidence records are retained.

## Adding evidence

Add one Markdown file per branch at `memory/evidence/<branch>-<yyyymmdd>.md`. Record the run URL, exact head SHA, command or workflow inputs, result/conclusion, and any required binary SHA256. Keep raw logs or large machine-generated artifacts only when needed to reproduce or interpret the result, and link them from the Markdown record. Update the relevant service note or `progress.md` with a short pointer when the result changes project status.
## Top-level note index

| File | One-line summary |
| --- | --- |
| [active-active-next.md](active-active-next.md) | Go reference rules (registration/failover helpers now ported; UpdateDomain remains incomplete): |
| [adaptive-single-diagnostic-lane-20261027.md](adaptive-single-diagnostic-lane-20261027.md) | The `adaptive-single-diagnostic` option in `.github/workflows/matching-production-validation.yml` runs the adaptive scaler SDK/load/routi… |
| [admin-describe-workflow-history-20261102.md](admin-describe-workflow-history-20261102.md) | - `go/service/frontend/admin/handler.go:252-296`: validates nil request and execution, computes Go workflow shard, resolves History owner… |
| [admin-history-proxy-ci-20261102.md](admin-history-proxy-ci-20261102.md) | Branch: `t3/admin-history-proxy-ci` |
| [all-rust-cluster-20261114.md](all-rust-cluster-20261114.md) | Branch: `all-rust-cluster`, based on `origin/master` at `9ec37416a145a13bdfc1cfa031e886a428783cf9`. |
| [api-compat.md](api-compat.md) | The Rust server must be a drop-in replacement for SDK clients. |
| [benchmark-service-suite-20261113.md](benchmark-service-suite-20261113.md) | Service benchmark checkpoint: compact evidence in Benchmark/evidence/service-comparison.md. Runs: History 43479770520 success with ~20s c… |
| [branch-cleanup-20261027.md](branch-cleanup-20261027.md) | Performed on branch `t3/cleanup-merged-matching-branches`. |
| [branch-cleanup-second-20261027.md](branch-cleanup-second-20261027.md) | Performed from `t3/cleanup-merged-matching-branches-1`. |
| [branch-cleanup-third-20261027.md](branch-cleanup-third-20261027.md) | Performed from `t3/cleanup-merged-matching-worktrees`. |
| [crate-split-plan-20261023.md](crate-split-plan-20261023.md) | Goal: matching / history / frontend / worker are independent crates + binaries; legacy `cadence-server` is only the all-in-one launcher.… |
| [domain-update-next.md](domain-update-next.md) | Verified UpdateDomain controller checkpoint: |
| [fe-async-restart-loss-20261114.md](fe-async-restart-loss-20261114.md) | Failing run: https://github.com/auenkr/cadence-rust/actions/runs/43599084228 (mysql, head f7a35e30) - step |
| [fe-restart-loss-20261114.md](fe-restart-loss-20261114.md) | Branch fe-restart-loss, based on 1004338ef1258b282d3940ce26a21f0bb21b993b. |
| [free-audit-r2-plan-unverified.md](free-audit-r2-plan-unverified.md) | Orchestrator review: this was written despite a read-only brief. Interface-only |
| [free-audit-r2-unverified.md](free-audit-r2-unverified.md) | Orchestrator review: this report was written despite a read-only brief. |
| [frontend-aa-persisted-20261103.md](frontend-aa-persisted-20261103.md) | Branch `t3/frontend-aa-persisted` (from master incl. 387b007b5). |
| [frontend-admin-coverage-20261104.md](frontend-admin-coverage-20261104.md) | Branch `t3/frontend-admin-coverage`, gate head 2595ac438ed5384f9caa9924e66338ca889a1fea (evidence file committed after). Only gate files… |
| [frontend-admin-domain-2026-11-01.md](frontend-admin-domain-2026-11-01.md) | Branch `fe-admin-domain` (from `frontend-domain-admin` @ c39bcd19a). Continues |
| [frontend-admin-dynconfig-20261103.md](frontend-admin-dynconfig-20261103.md) | Branch `frontend-admin-dynconfig` (from master). Evidence: CI run https://github.com/auenkr/cadence-rust/actions/runs/41531769629 (full w… |
| [frontend-admin-gaps-20261101.md](frontend-admin-gaps-20261101.md) | Items (one commit each, all validated only on GitHub Actions): |
| [frontend-admin-metrics-20261114.md](frontend-admin-metrics-20261114.md) | Branch `frontend-admin-metrics`, code head f7a35e302. |
| [frontend-admin-replication-luna-20261101.md](frontend-admin-replication-luna-20261101.md) | Date: 2026-11-01 |
| [frontend-admin-replication-terra-unblock-20261101.md](frontend-admin-replication-terra-unblock-20261101.md) | 2026-11-01; research against integrated audit `60fa3b657` and current branch. This is authorization to extend cadence-api and Thrift mode… |
| [frontend-api-coverage-20261103.md](frontend-api-coverage-20261103.md) | Branch: `t3/frontend-api-coverage`. Head SHA: `b7404ea995d5ad86f8074bc4c78066c14afa539a`. |
| [frontend-async-framing-20261101.md](frontend-async-framing-20261101.md) | Source audit found `handler_workflow_async.rs::thrift_payload` emitted unversioned Thrift bytes. Go frontend initializes `thriftrwEncoder… |
| [frontend-async-publisher-slices-20261102.md](frontend-async-publisher-slices-20261102.md) | Bounded source audit for production `StartWorkflowExecutionAsync` and `SignalWithStartWorkflowExecutionAsync`. Current tree rechecked: `h… |
| [frontend-audit-residuals-20261104.md](frontend-audit-residuals-20261104.md) | Branch `t3/frontend-audit-residuals` (master merged in). Head under test: fccbd6567. |
| [frontend-cassandra-history-20261102.md](frontend-cassandra-history-20261102.md) | Branch: `t3/frontend-cassandra-history-reader`. |
| [frontend-cassandra-lane-20261103.md](frontend-cassandra-lane-20261103.md) | Branch t3/cassandra-lane (= origin/t3/cassandra-runtime-boundary + master merge + fixes). |
| [frontend-checkpoint-20261103.md](frontend-checkpoint-20261103.md) | Master full rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/41557574536 (success). |
| [frontend-clusterredirection-20261103.md](frontend-clusterredirection-20261103.md) | Branch `frontend-clusterredirection`, code head dd42132f6. |
| [frontend-config-residuals-20261103.md](frontend-config-residuals-20261103.md) | Branch: `t3/frontend-config-residuals`. Validated head: 15bab12a5d96c8abbc467e7cf35d3288a1987a31 (not merged). |
| [frontend-core-20261031.md](frontend-core-20261031.md) | Branch: `frontend-core`. |
| [frontend-core2-20261031.md](frontend-core2-20261031.md) | Branch: `frontend-core2`, based on `origin/frontend-core` at start. |
| [frontend-cross-cluster-tasks-20261103.md](frontend-cross-cluster-tasks-20261103.md) | Branch `frontend-redirection-xc`, head 38ddf4ce69affaef2b10f38e838a4abbf1c7c554. |
| [frontend-discovery-next-20261101.md](frontend-discovery-next-20261101.md) | At integration 62775b41c, Frontend launcher starts TChannel with `tchannel::serve` (launcher.rs ~1944); that path supplies no Ringpop han… |
| [frontend-domain-admin-2026-10-31.md](frontend-domain-admin-2026-10-31.md) | - Work branch: `frontend-domain-admin` (local worktree branch `t3/frontend-domain-admin`). |
| [frontend-domain-visibility-bug-20261103.md](frontend-domain-visibility-bug-20261103.md) | Root cause: not a Rust Frontend bug. `frontend-mixed-gate.yml` built cadence-server from input `rust_ref`, default `fe-integ` (433 commit… |
| [frontend-domain-visible-20261101.md](frontend-domain-visible-20261101.md) | `rust/crates/cadence-matching/src/launcher.rs` cloned `state` into `grpc_state`/`history_state`/`tchannel_state` and |
| [frontend-domainrepl-20261114.md](frontend-domainrepl-20261114.md) | Branch `frontend-domainrepl`. Gap: Rust Frontend gRPC admin `get_domain_replication_messages` was a stub ("domain replication queue not e… |
| [frontend-dynconfig-gate-20261103.md](frontend-dynconfig-gate-20261103.md) | Branch `t3/frontend-dynamic-config-gates` (from master 4254b4963). Gate phase added to `tests/gates/run-frontend-gate.py` (all three lane… |
| [frontend-es-visibility-20261103.md](frontend-es-visibility-20261103.md) | Branch `frontend-es-visibility`. Ported (read path): List/Scan/CountWorkflowExecutions, ListOpen/ListClosed (+ByType/ByID/ByStatus), |
| [frontend-final-audit-2-20261103.md](frontend-final-audit-2-20261103.md) | Branch `t3/frontend-final-audit-2` from master `cef54c866`. READ-ONLY audit: no Cargo/Go/Docker run, no Rust change. Evidence is read fro… |
| [frontend-final-audit-20261103.md](frontend-final-audit-20261103.md) | - Run https://github.com/auenkr/cadence-rust/actions/runs/41583260241 — success, head 074a8db184d4c64fc21ffca2d61ece226ffa0971 |
| [frontend-final-gaps-20261104.md](frontend-final-gaps-20261104.md) | Branch `t3/frontend-final-gaps` (from t3/merge-audit-residuals). Code head: 8fe78d4ce (evidence commit follows). |
| [frontend-final-review-opus-20261114.md](frontend-final-review-opus-20261114.md) | This is a read-only review of the Rust Frontend (`rust/crates/cadence-frontend` plus the shared `cadence-service-core` dispatch) against… |
| [frontend-final-review-sol-20261114.md](frontend-final-review-sol-20261114.md) | Reviewed commit: `924bcf463ca1d7b5bfdaf0c3655e783af845a867` (origin/master when this review branch was created). |
| [frontend-forwarding-rl-gate-20261104.md](frontend-forwarding-rl-gate-20261104.md) | Branch `t3/frontend-forwarding-rl-gate` (not merged). Validated code head: 1aa726ce6b73539d9973f1451c31c760017d4888. |
| [frontend-gate-extras-20261102.md](frontend-gate-extras-20261102.md) | Branch: `t3/frontend-gate-extras`. No local Cargo, Go, rustfmt, builds or runtime gates. |
| [frontend-grpc-history-luna-20261101.md](frontend-grpc-history-luna-20261101.md) | Branch: `t3/6e5c5418` (prepared worktree `t3-6e5c5418`). This is a bounded hook-routing slice and does not establish Frontend service com… |
| [frontend-grpc-remote-full-20261103.md](frontend-grpc-remote-full-20261103.md) | Branch `t3/frontend-grpc-remote-full` (from master 30b607b0f). Not merged. |
| [frontend-grpc-start-luna-20261101.md](frontend-grpc-start-luna-20261101.md) | Branch: `t3/frontend-grpc-start-signal` |
| [frontend-history-routing-20261102.md](frontend-history-routing-20261102.md) | Branch: `frontend-history-routing-20261102`. Scoped implementation and validation complete; no Frontend service-completion claim. |
| [frontend-integration-20261101.md](frontend-integration-20261101.md) | Frontend is incomplete. User retains Go Worker, uses Rust Matching, and now permits at most two workers at normal speed: Luna for impleme… |
| [frontend-ipv6-bind-20261102.md](frontend-ipv6-bind-20261102.md) | Branch: `t3/frontend-ipv6-bind`. |
| [frontend-isogroup-cass-20261114.md](frontend-isogroup-cass-20261114.md) | Failure: run 43537738073 (cassandra, two_cluster, rust_frontend_active, master 938b77005): |
| [frontend-json-routing-contract-20261101.md](frontend-json-routing-contract-20261101.md) | Source audit at master `f2ff7d27a`; implementation remains pending. Two Luna delegates already own async grpc.rs and configured visibilit… |
| [frontend-latency-20261113.md](frontend-latency-20261113.md) | Branch `frontend-latency`. Head SHA of validated code: 8ee8a44538bde6e29734c7a00d19926059ec9dd5. |
| [frontend-leftovers-20261114.md](frontend-leftovers-20261114.md) | Branch: `frontend-leftovers-20261114`, based on origin/master `b5d5d358a`. |
| [frontend-master-sync-20261102.md](frontend-master-sync-20261102.md) | Integrated completed gate extras (`b92bd41b62e2ebad3c36156ca56e88b2c6b95568`) and PostgreSQL visibility/runtime fixes (`3c4ae6ae6`) into… |
| [frontend-matching-benchmark-20261104.md](frontend-matching-benchmark-20261104.md) | User requested a Go versus Rust benchmark, scoped specifically to Frontend and |
| [frontend-matching-benchmark-final-results-41767896292.md](frontend-matching-benchmark-final-results-41767896292.md) | Run: https://github.com/auenkr/cadence-rust/actions/runs/41767896292 |
| [frontend-matching-benchmark-initial-results-41762497645.md](frontend-matching-benchmark-initial-results-41762497645.md) | - Workflow: [Frontend + Matching paired benchmark](https://github.com/auenkr/cadence-rust/actions/runs/41762497645) |
| [frontend-mixed-41331739932-result.md](frontend-mixed-41331739932-result.md) | Run https://github.com/auenkr/cadence-rust/actions/runs/41331739932 ended at approximately 17:43 UTC on 2026-11-01. Both workflow and Rus… |
| [frontend-mixed-gate-20261101.md](frontend-mixed-gate-20261101.md) | Workflow: `.github/workflows/frontend-mixed-gate.yml` (dispatch with `-f rust_ref=<ref>`), harness |
| [frontend-multi-history-churn-20261103.md](frontend-multi-history-churn-20261103.md) | Branch `frontend-multi-history-gate` (from master 439300f77). Rust source head validated: `7ed9b495d0d43b46b11bbb90fe3acb87cf7a4054`. |
| [frontend-oauth-authorizer-20261103.md](frontend-oauth-authorizer-20261103.md) | Branch: t3/frontend-authorization-cluster-redirection |
| [frontend-parity-audit-20261102.md](frontend-parity-audit-20261102.md) | Audited source HEAD `855909c97d62bdf83483435914381c96cd7ca0eb` on `t3/frontend-parity-audit`. Frontend is **not complete**. The final mix… |
| [frontend-parity-gaps-2-20261103.md](frontend-parity-gaps-2-20261103.md) | Branch `frontend-parity-gaps-2` (from master 9d1653fa4). CI: package cadence-frontend, run https://github.com/auenkr/cadence-rust/actions… |
| [frontend-postgres-visibility-20261102.md](frontend-postgres-visibility-20261102.md) | Branch: `t3/frontend-postgres-visibility-gate`. |
| [frontend-progress-checkpoint-20261101.md](frontend-progress-checkpoint-20261101.md) | - fe-workflow-service and fe-admin-domain are contained in master (0 commits ahead). Last branch CI: fe-workflow-service run 41325887606… |
| [frontend-ratelimit-config-20261103.md](frontend-ratelimit-config-20261103.md) | Branch: `t3/frontend-ratelimit-config-w2` (the name `t3/frontend-ratelimit-config` is held by another worktree). Head: 027e6fa04b3558cd7c… |
| [frontend-redirection-grpc-aa-20261103.md](frontend-redirection-grpc-aa-20261103.md) | Branch `t3/frontend-redirection-grpc-aa`. |
| [frontend-replication-wire-luna-20261101.md](frontend-replication-wire-luna-20261101.md) | Implementation branch: `t3/replication-wire-models`. |
| [frontend-resume-checkpoint-20261101.md](frontend-resume-checkpoint-20261101.md) | User requested saving progress and a one-time restart with Claude at 23:15 Asia/Kolkata today (17:45 UTC). Automation scheduled-task:b07e… |
| [frontend-review-fix-20261114.md](frontend-review-fix-20261114.md) | Source: memory/frontend-final-review-opus-20261114.md. No cargo/go/docker was run locally. |
| [frontend-ringpop-lifecycle-20261101.md](frontend-ringpop-lifecycle-20261101.md) | Integration base: `71c3c273334836a9634cc2c509f4c2cdcf22671c`. |
| [frontend-server-entrypoint-20261101.md](frontend-server-entrypoint-20261101.md) | Source-confirmed gap discovered after mixed baseline 41327590981 failed: cadence-server/src/main.rs called launcher::run, which passes no… |
| [frontend-small-parity-20261103.md](frontend-small-parity-20261103.md) | Head f82622479d691c2d100bf7d17b27f490bed8f942. |
| [frontend-soak-tail-20261114.md](frontend-soak-tail-20261114.md) | https://github.com/auenkr/cadence-rust/actions/runs/43582334396 (head 52848226a21ba35aea88e10157da31ff2bd7f168). All jobs green; verdict… |
| [frontend-soakfix-20261113.md](frontend-soakfix-20261113.md) | Branch `frontend-soakfix` (from master 7d0e189c4). Not merged. |
| [frontend-solfix-p1-20261114.md](frontend-solfix-p1-20261114.md) | Branch `frontend-solfix-p1`, based on origin/master. Scope: SOL #1–#5 and Opus LOW-1 only. No local Cargo, Go or Docker commands. |
| [frontend-solfix-p2-20261114.md](frontend-solfix-p2-20261114.md) | Commits (see `git log origin/master..frontend-solfix-p2`): |
| [frontend-transport-followups-20261101.md](frontend-transport-followups-20261101.md) | Audited master source after synchronization, 2026-11-01. These are implementation tasks, not completed behavior or test evidence. Two act… |
| [frontend-two-cluster-gate-20261103.md](frontend-two-cluster-gate-20261103.md) | Branch `t3/frontend-two-cluster-gate`. Harness: `tests/gates/run-frontend-two-cluster-gate.py`, lane `run-frontend-two-cluster-lane.sh`,… |
| [frontend-validator-config-20261103.md](frontend-validator-config-20261103.md) | Branch t3/frontend-validator-config, head bb05fb742cbfc8a6ca89a5d724e36315f86da468 |
| [frontend-worker-orchestration-20261031.md](frontend-worker-orchestration-20261031.md) | New workers use Claude Haiku 5.5 (`claude-haiku-5-5`) instead of gpt-6-luna. Running luna workers (frontend-domain-admin = integration br… |
| [frontend-workflow-api-20261031.md](frontend-workflow-api-20261031.md) | - Merged `origin/frontend-core2` into `t3/frontend-workflow-api` at |
| [frontend-workflow-residuals-20261104.md](frontend-workflow-residuals-20261104.md) | Branch `t3/frontend-workflow-residuals`, code head `8c6e48bde7457ca5c3191025b0010aedd8af9930` (plus this evidence commit). |
| [frontend-workflow-service-20261101.md](frontend-workflow-service-20261101.md) | Base: origin/frontend-domain-admin. Goal: the Rust Frontend serves the workflow API |
| [frontend-wrappers-metrics-20261103.md](frontend-wrappers-metrics-20261103.md) | Branch `frontend-wrappers`, code SHA 325c84b58. |
| [gate-graceful-deflake-20261114.md](gate-graceful-deflake-20261114.md) | Branch: `gate-graceful-deflake` |
| [history-acmanager-20261112.md](history-acmanager-20261112.md) | Branch `history-acmanager` (from master 62016da50). |
| [history-activeactive-20261112.md](history-activeactive-20261112.md) | - Go `DomainCacheEntry.GetActiveClusterInfoByClusterAttribute` ported as `DomainCacheEntry::get_active_cluster_info_by_cluster_attribute`… |
| [history-activerepl-20261113.md](history-activerepl-20261113.md) | Goal: Rust History as ACTIVE source cluster (B), Go History standby (A, all Go). |
| [history-activity-timer-status-20261109.md](history-activity-timer-status-20261109.md) | Branch: `t3/history-activity-timer-status`. |
| [history-admingate-20261113.md](history-admingate-20261113.md) | Gate rows `history-admin-*` (40 per run) drive the Go `cadence admin ...` CLI against the mixed cluster |
| [history-archgaps-20261112.md](history-archgaps-20261112.md) | Branch `history-archgaps` closes the gaps listed in `history-s3gs-20261112.md`. |
| [history-archstream-20261112.md](history-archstream-20261112.md) | Closes the remaining gaps of `history-archgaps-20261112.md` (s3store / gcloud history archivers). |
| [history-auditgaps-20261114.md](history-auditgaps-20261114.md) | Static re-check of every open gap in `history-parity-audit-20261114.md` against master 803fcab58 (no local cargo/go/docker). |
| [history-auditp0-20261111.md](history-auditp0-20261111.md) | Source: memory/archive/history-parity-audit-20261111.md (P0 list). |
| [history-auditp1-20261111.md](history-auditp1-20261111.md) | Branch `history-auditp1` (from master e50090050). Source: memory/archive/history-parity-audit-20261111.md. Nothing was compiled locally; all vali… |
| [history-batch-gap-20261115.md](history-batch-gap-20261115.md) | Branch: `history-batch-gap` |
| [history-casflake-20261113.md](history-casflake-20261113.md) | - Evidence limits: attempt 2 overwrote the artifact `history-mixed-gate-cassandra-tchannel`; only the 100-line gate.log tail of attempt 1… |
| [history-cassandra-20261111.md](history-cassandra-20261111.md) | - `rust/crates/cadence-persistence/src/history_shard_cassandra.rs`: `CassandraShardStore` (impl of `history_sql::ShardStore`), |
| [history-cassgaps-20261112.md](history-cassgaps-20261112.md) | Ported from Go nosql store / cassandra plugin: |
| [history-churnflake-20261112.md](history-churnflake-20261112.md) | Failed runs: 43061124538 (mysql), 43075206911 (cassandra). Symptom: sdk-history-churn-workflows |
| [history-crossreview-codex-20261113.md](history-crossreview-codex-20261113.md) | Reviewed base: `bf1cba71c6cdef2f528dee9beef75c164e61dda9` (`origin/master`). |
| [history-crossreview-opus-20261113.md](history-crossreview-opus-20261113.md) | Base: origin/master bd9161312. Read-only review (no cargo/go/docker). Codex review was not read. |
| [history-decfailover-20261112.md](history-decfailover-20261112.md) | Branch `history-decfailover` (from master aabe60505). |
| [history-decisiongaps-20261111.md](history-decisiongaps-20261111.md) | Branch `history-decisiongaps`. |
| [history-decisiongaps2-20261111.md](history-decisiongaps2-20261111.md) | Branch `history-decisiongaps2` (from origin/master). |
| [history-dlqprobe-20261114.md](history-dlqprobe-20261114.md) | Branch `history-dlqprobe`, validated head `d44bc772e1157b77831ca00685a161b4ac40c76a` (code commit; memory commit follows). |
| [history-dtcompleted-20261111.md](history-dtcompleted-20261111.md) | - `engine_decision.rs`: `handle_decision_task_completed` ported from Go `HandleDecisionTaskCompleted`. It covers the token check, running… |
| [history-dtcompleted2-20261111.md](history-dtcompleted2-20261111.md) | Branch `history-dtcompleted2`, code head b6cf13e6a. |
| [history-enginepkg-20261112.md](history-enginepkg-20261112.md) | Port of `go/service/history/engine/engineimpl/*_test.go` into |
| [history-enginepkg2-20261112.md](history-enginepkg2-20261112.md) | Builds on memory/history-enginepkg-20261112.md. New files in `rust/crates/cadence-history/src/engine/engine_tests/`: |
| [history-enginepkg3-20261112.md](history-enginepkg3-20261112.md) | Branch history-enginepkg3. Test-only work: no non-test Rust code changed, so no mixed gate was required. |
| [history-enginepkg4-20261112.md](history-enginepkg4-20261112.md) | Branch history-enginepkg4. Only cfg(test) code changed (harness `cache` field, `EngineExecutionCache::clear_all_for_test`), so no mixed g… |
| [history-enginepkg5-20261112.md](history-enginepkg5-20261112.md) | Branch history-enginepkg5. Only cfg(test) code changed (harness `Harness::build(configure)`, cfg(test) `QueryRegistry::raw_registry_for_t… |
| [history-enginetests-20261111.md](history-enginetests-20261111.md) | Final rust-ci (cadence-history, clippy -D warnings + tests): **success** |
| [history-esgate-20261113.md](history-esgate-20261113.md) | Branch `history-esgate` (from master e4e463056). Added: dispatch-only `.github/workflows/history-es-visibility.yml` |
| [history-esvis-20261113.md](history-esvis-20261113.md) | Branch `history-esvis`, head SHA `084d0148e08c63196cf1a4f8f8364ba6de1d3d85` (evidence commit follows it). |
| [history-eventsv2-20261112.md](history-eventsv2-20261112.md) | - `MemExecutions::conflict_resolve_workflow_execution` implemented per Go `sql_execution_store.go:551-680` (BypassCurrent / UpdateCurrent… |
| [history-exectests-20261112.md](history-exectests-20261112.md) | Branch `history-exectests` (from master 08ed59d23). Rust unit tests for the History task executors, ported from the Go suites. CI only. |
| [history-failcoord-20261113.md](history-failcoord-20261113.md) | Go: `go/service/history/failover/{coordinator,marker_notifier}.go`, handler `NotifyFailoverMarkers`/`GetFailoverInfo`, |
| [history-gap-apitypes-20261106.md](history-gap-apitypes-20261106.md) | Branch history-gap-apitypes. Added Semaphore*/RequestCancelActivityTaskFailed attrs + HistoryEvent fields, started/continued-as-new field… |
| [history-gap-apitypes2-20261107.md](history-gap-apitypes2-20261107.md) | Branch history-gap-apitypes2. |
| [history-gap-archwriters-20261107.md](history-gap-archwriters-20261107.md) | - `cadence-common/src/archiver/writer.rs`: Go filestore `historyArchiver.Archive` / `visibilityArchiver.Archive` (same file names, |
| [history-gap-decision3-20261106.md](history-gap-decision3-20261106.md) | Branch history-gap-decision3, code head ba65e607c. |
| [history-gap-engine-20261105.md](history-gap-engine-20261105.md) | Status: PARTIAL. Done = only the rows marked done. Task NOT complete. |
| [history-gap-execctx-20261105.md](history-gap-execctx-20261105.md) | Status: PARTIAL. Go files listed in the task are NOT fully ported yet. |
| [history-gap-execctx2-20261105.md](history-gap-execctx2-20261105.md) | Status: PARTIAL. Only history_builder.go is ported. |
| [history-gap-handler-20261105.md](history-gap-handler-20261105.md) | Done: `handler/handler_impl.rs` (`HandlerImpl`) ports handler.go lifecycle (start/stop/prepare_to_stop 5s delay/health) |
| [history-gap-handlerfailover-20261107.md](history-gap-handlerfailover-20261107.md) | - Handler `NotifyFailoverMarkers` / `GetFailoverInfo` ported from go handler.go using `HistoryEnv.failover_coordinator` (notify: no shutd… |
| [history-gap-misc-20261106.md](history-gap-misc-20261106.md) | Scope: ndc, replication, failover, shard, taskdlq, queuev2, task, workflowcache, query, config, resource, constants. |
| [history-gap-mutstate-20261105.md](history-gap-mutstate-20261105.md) | Branch: history-gap-mutstate. Status: IN PROGRESS, no Rust code ported yet, CI not run. |
| [history-gap-mutstate2-20261105.md](history-gap-mutstate2-20261105.md) | Branch history-gap-mutstate2 (inventory merged from history-gap-mutstate). |
| [history-gap-mutstate3-20261106.md](history-gap-mutstate3-20261106.md) | New typed Go-shaped module: `rust/crates/cadence-history/src/execution/mutable_state_builder.rs` |
| [history-gap-mutstate4-20261106.md](history-gap-mutstate4-20261106.md) | Branch `history-gap-mutstate4`. Typed `TypedMutableState` event methods in |
| [history-gap-ndcadapters-20261107.md](history-gap-ndcadapters-20261107.md) | Status: PARTIAL. `UnwiredReplicator` is no longer wired; `ndc_adapters::NdcReplicator` is (rust/crates/cadence-history/src/wiring/assembl… |
| [history-gap-ndcrepl-20261106.md](history-gap-ndcrepl-20261106.md) | Branch `history-gap-ndcrepl`. Method: read Go (non-test) and Rust side by side, function by function. |
| [history-gap-queuedeps-20261107.md](history-gap-queuedeps-20261107.md) | Goal: replace `MissingExecutorsProvider` (queue_factory.rs) by a real `QueueExecProvider`, which needs real |
| [history-gap-queueexecrows-20261107.md](history-gap-queueexecrows-20261107.md) | - `SqlHistoryTask` (wiring/persistence/queues.rs) now keeps the full decoded `pc::TransferTask` / `pc::TimerTask` |
| [history-gap-queuefactory-20261107.md](history-gap-queuefactory-20261107.md) | Done: `wiring/assembly/queue_factory.rs` adds `ExecQueueFactory` (impl `QueueFactory`) over a `QueueExecProvider`; launcher now uses it i… |
| [history-gap-queuetimeractive-20261107.md](history-gap-queuetimeractive-20261107.md) | New file `rust/crates/cadence-history/src/wiring/assembly/queue_deps_timer_active.rs`: `TimerActiveDepsImpl` (+ private `EngineTimerWorkf… |
| [history-gap-queuetransferactive-20261107.md](history-gap-queuetransferactive-20261107.md) | File: `rust/crates/cadence-history/src/wiring/assembly/queue_deps_transfer_active.rs` (registered in assembly/mod.rs). |
| [history-gap-startup-20261106.md](history-gap-startup-20261106.md) | Ported: go/service/history/service.go -> rust/crates/cadence-history/src/service.rs; lookup/*.go -> src/lookup.rs; real main.rs + src/lau… |
| [history-gap-startup2-20261107.md](history-gap-startup2-20261107.md) | - **gRPC TLS** (`rpc.tls`): launcher no longer rejects it. Uses shared `cadence_service_core::grpc_tls` (`server_tls`, `server_tls_custom… |
| [history-gap-taskinfra-20261107.md](history-gap-taskinfra-20261107.md) | Branch: history-gap-taskinfra. Rust under rust/crates/cadence-history/src/task/. |
| [history-gap-timerexec-20261106.md](history-gap-timerexec-20261106.md) | Branch history-gap-timerexec. Line-by-line audit of Go timer_active/standby/base executors vs Rust task/timer_*.rs. |
| [history-gap-timermut-20261107.md](history-gap-timermut-20261107.md) | Backed by new `wiring/assembly/engine_timer.rs` on `EngineMutableState` (not TypedMutableState): |
| [history-gap-timerq2-20261105.md](history-gap-timerq2-20261105.md) | Branch: history-gap-timerq2. Head SHA: d65c4c21bfbf3d160bc5580b20c4546a52c08a15 |
| [history-gap-timerq3-20261106.md](history-gap-timerq3-20261106.md) | Branch history-gap-timerq3, head (code) 080f29450288fc2260eebb0361b1bc7ec9861cb9. CI https://github.com/auenkr/cadence-rust/actions/runs/… |
| [history-gap-transferexec-20261106.md](history-gap-transferexec-20261106.md) | Branch: history-gap-transferexec. Go: go/service/history/task. Rust: rust/crates/cadence-history/src/task. |
| [history-gap-transfermut-20261107.md](history-gap-transfermut-20261107.md) | Branch `history-gap-transfermut`. Ports Go `transfer_active_task_executor.go` dependencies in |
| [history-gate-fix1-20261111.md](history-gate-fix1-20261111.md) | Branch history-gate-fix1 from 012fd8095. Lane: mysql, grpc, two_history=false. |
| [history-gate-fix2-20261111.md](history-gate-fix2-20261111.md) | Branch `history-gate-fix2` (from master e7a6f053f). Baseline gate run 43000919671: 166/171 (mysql, grpc, single History). |
| [history-gate-fix3-20261111.md](history-gate-fix3-20261111.md) | Branch `history-gate-fix3`, from master b80c3b716. Failing baseline: gate run 43010408023. |
| [history-gate-lanes-20261111.md](history-gate-lanes-20261111.md) | Final code head: `34700b3cdbbe2a4deefa61a50689f5605e5100d7`. Workflow: `history-mixed-gate.yml`. |
| [history-gatescen-20261112.md](history-gatescen-20261112.md) | Branch `history-gatescen` (from origin/master 1f4654821). Extends the history mixed gate |
| [history-goreplfollow-20261114.md](history-goreplfollow-20261114.md) | 1. DIAG debug logs removed (task generator, timer executor base, engine_tasks, sql_workflow_store). |
| [history-goreplgate-20261114.md](history-goreplgate-20261114.md) | Head SHA gated: b54d550ce146ee6f74f31c6058770c4501c85661 (branch history-goreplgate). |
| [history-h1-persistence-20261104.md](history-h1-persistence-20261104.md) | Branch: `history-h1-persistence`, based on `master` at `797ff0f15fde9be16a4cafd33850b2cf9c70122d`. |
| [history-h1-sql-impl-guide.md](history-h1-sql-impl-guide.md) | This guide is the implementation handoff after `history_contract.rs`. The contract takes typed snapshot/mutation inputs, including mode,… |
| [history-h1-sql-s1-20261104.md](history-h1-sql-s1-20261104.md) | Branch: `history-h1-sql-s1c`, created from `master` (`18dda13dc`). The older |
| [history-h1-sql-s1d-20261104.md](history-h1-sql-s1d-20261104.md) | Branch: `history-h1-sql-s1d`, based on `origin/master` at start of work. |
| [history-h1a-20261104.md](history-h1a-20261104.md) | Branch: `history-h1a-sql` |
| [history-h2-skeleton-20261104.md](history-h2-skeleton-20261104.md) | Branch: `history-h2-skeleton`, based on `master` at `797ff0f15`. |
| [history-h2b-20261104.md](history-h2b-20261104.md) | Branch: `history-h2b-tchannel-shards`, based on `origin/master`. |
| [history-h3a-20261104.md](history-h3a-20261104.md) | - Branch: `history-h3a-mutable-state` |
| [history-h3a2-20261104.md](history-h3a2-20261104.md) | Branch: `history-h3a2-decision-activity` |
| [history-h3a3-20261104.md](history-h3a3-20261104.md) | Implemented the decision-task add-event transition slice in Rust on branch |
| [history-h3a4-20261104.md](history-h3a4-20261104.md) | Branch: `history-h3a4-activity`, created from freshly fetched `origin/master`. |
| [history-kafka-auth-20261114.md](history-kafka-auth-20261114.md) | The relevant end-to-end lane is `.github/workflows/history-es-visibility.yml`. It runs Rust History's advanced visibility Kafka producer,… |
| [history-kafka-tls-20261114.md](history-kafka-tls-20261114.md) | Lane: `history-es-visibility.yml` with new input `kafka_tls=true` (concurrency group and artifact name include it). |
| [history-lastgaps-20261112.md](history-lastgaps-20261112.md) | Branch `history-lastgaps` (from origin/master 503889cb1). CI-only validation. |
| [history-lastp2-20261113.md](history-lastp2-20261113.md) | Branch: history-lastp2 (from master f9e9319d3, master merged). Validated head: 5d2cafdaf12b96a40f9bf6e140a33196e6bc643b (this note is com… |
| [history-markerproof-20261114.md](history-markerproof-20261114.md) | Branch history-markerproof. Commit adds info logs only (no logic change): |
| [history-markerroute-20261113.md](history-markerroute-20261113.md) | Go ref: `go/client/history/client.go` `NotifyFailoverMarkers`, `peer_resolver.go` `FromDomainID`, `common.DomainIDToHistoryShard`. |
| [history-ms-activity-20261107.md](history-ms-activity-20261107.md) | Port of go/service/history/execution/mutable_state_builder_methods_activity.go as |
| [history-ms-childsignal-20261107.md](history-ms-childsignal-20261107.md) | Ports Go `mutable_state_builder_methods_child_workflow.go` and `mutable_state_builder_methods_signal.go` as `impl TypedMutableState` in |
| [history-ms-decision-20261107.md](history-ms-decision-20261107.md) | Branch `history-ms-decision`. Files: `rust/crates/cadence-history/src/execution/mutable_state_methods_decision.rs`, `mutable_state_method… |
| [history-ms-started2-20261107.md](history-ms-started2-20261107.md) | Branch `history-ms-started2`. CI (rust-ci, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/42339101454 - su… |
| [history-ms-taskgen-20261107.md](history-ms-taskgen-20261107.md) | Go source: go/service/history/execution/mutable_state_task_generator.go. |
| [history-ms-taskwire-20261107.md](history-ms-taskwire-20261107.md) | Wired Go `e.taskGenerator.Generate*` calls into TypedMutableState methods (rust/crates/cadence-history/src/execution). |
| [history-ms-taskwire2-20261107.md](history-ms-taskwire2-20261107.md) | Branch `history-ms-taskwire2` (from master d7dfd0519). |
| [history-msbparity-20261114.md](history-msbparity-20261114.md) | Static line-level comparison (no local cargo/go/docker). **PARTIAL** — not every listed file was fully re-read, see "Not covered". |
| [history-mscodec-20261111.md](history-mscodec-20261111.md) | - Added `execution/mutable_state_bridge.rs` with snapshot-to-envelope and unchanged envelope-to-snapshot codec functions. |
| [history-msoverlay-20261111.md](history-msoverlay-20261111.md) | - Added explicit typed mutation to persistence-request conversion and typed snapshot conversion in `rust/crates/cadence-history/src/execu… |
| [history-mstxn-20261111.md](history-mstxn-20261111.md) | Branch: `history-mstxn` |
| [history-mutstate-json-20261114.md](history-mutstate-json-20261114.md) | Branch: `history-mutstate-json` |
| [history-mutstate-unification-plan-20261109.md](history-mutstate-unification-plan-20261109.md) | Audit base: `c42480cdc` (checkout HEAD when inspection began). Research only: no Rust edits, no Cargo/Go commands, no validation run. Rea… |
| [history-ndcbuilder-20261111.md](history-ndcbuilder-20261111.md) | Branch `history-ndcbuilder` (from master f5b2c2a18). |
| [history-ndccache-20261111.md](history-ndccache-20261111.md) | Branch: `history-ndccache` |
| [history-ndcgaps-20261111.md](history-ndcgaps-20261111.md) | Branch `history-ndcgaps` from master 95a558c16. All 5 items are implemented. CI: run 43052491721 (head f15d5f482, cadence-history clippy+… |
| [history-ndcpersist-20261111.md](history-ndcpersist-20261111.md) | Branch: `history-ndcpersist` |
| [history-ndcpersist2-20261111.md](history-ndcpersist2-20261111.md) | Branch: `history-ndcpersist2` (local worktree branch `ndcp2-work`) |
| [history-ndcpersist3-20261111.md](history-ndcpersist3-20261111.md) | Branch: `history-ndcpersist3` (base `9120a28ea`). Go source: `execution/context.go`, `ndc/*`. |
| [history-ndcrebuild-20261111.md](history-ndcrebuild-20261111.md) | Branch `history-ndcrebuild`. CI (rust-ci.yml, package=cadence-history): https://github.com/auenkr/cadence-rust/actions/runs/43000239931 —… |
| [history-ndctests-20261111.md](history-ndctests-20261111.md) | Branch `history-ndctests`. |
| [history-ndctests2-20261111.md](history-ndctests2-20261111.md) | Branch `history-ndctests2` (from 8e0b1064c). |
| [history-ndctxn-20261111.md](history-ndctxn-20261111.md) | Ported (Rust, wiring/assembly): |
| [history-p1b2-20261112.md](history-p1b2-20261112.md) | Code head validated: `8ac8e2ae09b8ce4686e4dddb067fe7850316e9a3` (origin/master fcd69fabc already merged; no new master commits). |
| [history-p1b3-20261112.md](history-p1b3-20261112.md) | Code head validated: `9e003a8a0fc732df18574fe7e8069c2b11917fb9` (origin/master fd4e11ce2 already merged). The memory commit on top change… |
| [history-p1b4-20261112.md](history-p1b4-20261112.md) | Head (code): b8c5d38bb (memory commit follows). Base d47de3f8d. |
| [history-p1b5-20261112.md](history-p1b5-20261112.md) | Branch `history-p1b5`. Code head validated: `7d3bc8438d42461e4c04980fca827dd3059e41de` (origin/master d47de3f8d already merged, no newer… |
| [history-p1batch-20261112.md](history-p1batch-20261112.md) | Branch `history-p1batch` from origin/master e607ced11. No cargo/go/docker run locally; CI only. |
| [history-p2gaps-20261114.md](history-p2gaps-20261114.md) | 1. Mutable-state checksum repair on engine load (engine_cache.rs load_state 3-attempt loop, engine_checksum.rs repair_workflow/repair_via… |
| [history-p2rest-20261112.md](history-p2rest-20261112.md) | Branch `history-p2rest`. Ported: load-time checksum verify + invalidateBefore (`engine_checksum.rs`), DataInconsistentCounter + call site… |
| [history-p2stubs-20261112.md](history-p2stubs-20261112.md) | \| Item \| Decision \| |
| [history-parentclose-20261111.md](history-parentclose-20261111.md) | Branch `history-parentclose` (from master 8e0b1064c). |
| [history-parity-audit-20261113-final.md](history-parity-audit-20261113-final.md) | Method and limits: package-level mapping of `go/service/history/**` to `rust/crates/cadence-history/src/**`, carried forward from |
| [history-parity-audit-20261114.md](history-parity-audit-20261114.md) | Method and limits: static analysis only (no cargo/go/docker). The per-file table below is a **name-based mapping** of every non-test Go f… |
| [history-pause-20261105.md](history-pause-20261105.md) | User requested saving progress and pausing immediately. No further migration, workers or validation should run until the user resumes. |
| [history-plan-20261104.md](history-plan-20261104.md) | Starting point: `rust/crates/cadence-history` is a stub (`not_migrated`). Go source: |
| [history-port-audit-20261105.md](history-port-audit-20261105.md) | Scope: Go `go/service/history` non-test files (189 files, 61627 LOC) vs `rust/crates/cadence-history/src` (166 files, 33151 LOC). Static… |
| [history-port-decision-20261104.md](history-port-decision-20261104.md) | Branch: `history-port-decision`; worktree: `/mnt/storage/service/wt-port-decision`. |
| [history-port-engine-20261104.md](history-port-engine-20261104.md) | Branch: history-port-engine (merged origin/master, includes decision/events/query/reset/workflowcache/execution) |
| [history-port-exec-a-20261104.md](history-port-exec-a-20261104.md) | Branch from `origin/master`: `history-port-exec-a`. |
| [history-port-ndc-20261105.md](history-port-ndc-20261105.md) | Go go/service/history/ndc/ -> rust/crates/cadence-history/src/ndc/ |
| [history-port-persist-20261104.md](history-port-persist-20261104.md) | Branch: `history-port-persist`. Worktree: `/home/auenkr/.t3/worktrees/cadence-rust/t3-cee2bc69`. |
| [history-port-queue-20261105.md](history-port-queue-20261105.md) | Branch: history-port-queue. Last green CI: https://github.com/auenkr/cadence-rust/actions/runs/41897617384 (head dc363b91afa2e44cb084a932… |
| [history-port-queuev2-20261105.md](history-port-queuev2-20261105.md) | Rust: rust/crates/cadence-history/src/queuev2/. Unported Go deps are stubbed in deps.rs |
| [history-port-repl-20261105.md](history-port-repl-20261105.md) | Rust: `rust/crates/cadence-history/src/{replication,failover}/`. Collaborators owned by other ports are small traits in `replication/deps… |
| [history-port-shard-20261105.md](history-port-shard-20261105.md) | Branch `history-port-shard`. Validation: CI only (`gh workflow run rust-ci.yml --ref history-port-shard -f package=cadence-history`). |
| [history-port-task-20261104.md](history-port-task-20261104.md) | Port of Go go/service/history/task/ and taskdlq/ into rust/crates/cadence-history/src/{task,taskdlq}. |
| [history-port-wiring-20261105.md](history-port-wiring-20261105.md) | Goal: runnable History service == go/service/history (service.go, handler/, cmd/server history wiring). No stubs on startup path. |
| [history-queueexec-assembly-20261111.md](history-queueexec-assembly-20261111.md) | Branch: `history-queueexec-assembly` (from master efbc0ea40). |
| [history-queuegaps-20261111.md](history-queuegaps-20261111.md) | Branch `history-queuegaps` (from master 8e0b1064c, merged with master e50090050). |
| [history-queuev2-gate-20261114.md](history-queuev2-gate-20261114.md) | Branch history-queuev2-gate. Workflow history-mixed-gate.yml gained inputs `queue_v2` (sets history.enableTimerQueueV2/TransferQueueV2 tr… |
| [history-reaudit2-20261112.md](history-reaudit2-20261112.md) | Branch `history-reaudit2` from origin/master f8914f198. CI only. |
| [history-reinjmetrics-20261112.md](history-reinjmetrics-20261112.md) | - ShardTaskReinjector (shard_factory.rs): Go `ReinjectHistoryTasks` write path via `GoExecutionStore::create_history_tasks` |
| [history-replgen-20261113.md](history-replgen-20261113.md) | Head SHA gated: 4b2ee2ffe2b5d56ad766616647a527476e8f51d1 (master merged). rust-ci 43407015440 (c287522) success. |
| [history-respondparity-20261114.md](history-respondparity-20261114.md) | Static line-level comparison (no local cargo/go/docker). PARTIAL scope, see "Not covered". |
| [history-restartgate-20261114.md](history-restartgate-20261114.md) | Branch `history-restartgate` (from origin/master a5b151f44). Harness only, no Rust changes. |
| [history-restartsig-20261112.md](history-restartsig-20261112.md) | Bug: history-mixed-gate (grpc, two_history=true) intermittently failed after the History crash/restart with |
| [history-retryreset-20261112.md](history-retryreset-20261112.md) | Audit items P1-1 and P1-2 of history-parity-audit-20261112.md. |
| [history-review2-codex-20261113.md](history-review2-codex-20261113.md) | Reviewed `git diff bd9161312 df0a8065f -- rust/` against the Go implementation at `df0a8065f9b96f9a4516680d02d0d1f5b619fac6` (`origin/mas… |
| [history-review2fix-20261113.md](history-review2fix-20261113.md) | Branch `history-review2fix`, head SHA `074e43a945839312e5aa4b38715591c45c4dc07c` (not merged). Source: `memory/history-review2-codex-2026… |
| [history-review3-20261113.md](history-review3-20261113.md) | Review base: `9de5851f25b0dff6c0b43ba59bf11d0886bbde42` (`origin/master`). Branch: `history-review3`, created from that base. Static revi… |
| [history-review3-sonnet-20261113.md](history-review3-sonnet-20261113.md) | Scope: Rust diffs of history-failcoord, history-ttl, history-standbyrepl, history-review2fix, history-soakfix, |
| [history-review3fix-20261114.md](history-review3fix-20261114.md) | Branch `history-review3fix`, head `bc19b456931f544ce79de88b9b8f31c2d160eb02`. Fixes the combined findings of history-review3 (Sol + Sonnet). |
| [history-runtime-20261109.md](history-runtime-20261109.md) | Base: master c42480cdc. History is incomplete. Rust Matching and Frontend are required in acceptance lanes; Go Worker remains. |
| [history-s3gate-20261113.md](history-s3gate-20261113.md) | Branch `history-s3gate`. Gate: `history-mixed-gate.yml` with `-f database=mysql -f s3_archival=true -f two_history=false` |
| [history-s3gs-20261112.md](history-s3gs-20261112.md) | Branch `history-s3gs`: Go `common/archiver/s3store` and `common/archiver/gcloud` write path (history + visibility Archive) ported; Histor… |
| [history-s3race-20261113.md](history-s3race-20261113.md) | Branch `history-s3race` (no code change). Head SHA = commit containing this file. |
| [history-soak-20261114.md](history-soak-20261114.md) | Branch `history-soakstall`, head 0e46b77c11ee2d19f9259f36b27eff63acb37f8c (= soakstall 644d71307 + origin/master d8f7888bd, which has the… |
| [history-soakfix-20261113.md](history-soakfix-20261113.md) | Branch `history-soakfix` (from 3c48ab2b0). Binary SHA256 is in each run's artifact (`rust-binaries.sha256`). |
| [history-soakstall-20261114.md](history-soakstall-20261114.md) | Branch `history-soak-tail` fixes canceled parent-update cache leases and ignored |
| [history-sqlblobs-20261104.md](history-sqlblobs-20261104.md) | - Branch: `history-sqlblobs` |
| [history-sqlblobs-adapters-20261104.md](history-sqlblobs-adapters-20261104.md) | - Added the typed `ActivityInfo` persistence contract and its sqlblobs `Struct` encoder/decoder in `rust/crates/cadence-persistence/src/h… |
| [history-standbyrepl-20261113.md](history-standbyrepl-20261113.md) | Branch `history-standbyrepl`. Problem: two-cluster gate (rust_history=true), Rust History standby fed by Go active: 3 `repl-standby-*` ro… |
| [history-standbytests-20261112.md](history-standbytests-20261112.md) | Branch `history-standbytests` (from master c4312b82d). Test-only change: **no non-test Rust code changed**, so mixed gates were not requi… |
| [history-startreset-20261114.md](history-startreset-20261114.md) | - engineimpl/start_workflow_execution.go -> engine/start_workflow_execution.rs (ID reuse policy, terminateAndStart, delayed start/cron, f… |
| [history-swap-20261113.md](history-swap-20261113.md) | Branch `history-swap`, head `daf66a84c0854193e58e2a07caaa57ccfd060c2f` (merged origin/master). |
| [history-taskwritelock-20261112.md](history-taskwritelock-20261112.md) | Follow-up to history-restartsig: every task-id allocation path now holds `ShardContext::lock_task_writes()` |
| [history-ttl-20261113.md](history-ttl-20261113.md) | `matching add task: missing TTL` is the yarpc validator error (`ValidateRequestContext`, yarpc v1.55.0 api/transport/request.go) from Go… |
| [history-txnsizelimit-20261114.md](history-txnsizelimit-20261114.md) | - common/persistence/history_manager.go L263-271: `AppendHistoryNodes` returns `TransactionSizeLimitError{"transaction size of %v bytes e… |
| [history-typed-replay-20261111.md](history-typed-replay-20261111.md) | Implemented and pushed on branch `history-typed-replay`. The typed replay code head is `04c58d462403881d3472f44a56d0694d5008dec2` (based… |
| [history-typed-replay2-20261111.md](history-typed-replay2-20261111.md) | - Added typed `ApplyEvents` dispatch in `execution/state_builder.rs` for timer transitions, child workflow transitions, request-cancel ex… |
| [history-typed-replay3-20261111.md](history-typed-replay3-20261111.md) | Branch: `history-typed-replay3` |
| [history-uncovered-20261113.md](history-uncovered-20261113.md) | Branch `history-uncovered`. Code head under test: `4945b08cfae39fa3858b3b334507fe1e7541226d`. |
| [history-v2rest-20261112.md](history-v2rest-20261112.md) | conflict_resolve direct unit test; RecordDecisionTaskStarted and RecordActivityTaskStarted variants (incl. resurrected activity); |
| [history-xrfix-20261113.md](history-xrfix-20261113.md) | Base origin/master 48b50a828. All Cargo work ran on CI only. Head SHA at gate dispatch: 8e5a3e440743a3c2d6c8000f64426be3f2e74504. |
| [master-validation-20261118.md](master-validation-20261118.md) | Branch: `master-validation-20261118`. Validated head: `d1dba9370688f19bccd137d7cfbffe8e9c72d63d` (`origin/master`; includes glibc arena c… |
| [matching-absent-query-fields-impl.md](matching-absent-query-fields-impl.md) | Source: origin/t3/matching-nil-domain-review memory/matching-nil-query-domain-source-review.md (corrected 14ccc39b6), base origin/t3/resp… |
| [matching-absent-query-result.md](matching-absent-query-result.md) | Go refs: go/service/matching/handler/handler.go QueryWorkflow (L293) and RespondQueryTaskCompleted (L326) use nil-safe getters, panic cap… |
| [matching-acceptance-evidence-readiness-20261027.md](matching-acceptance-evidence-readiness-20261027.md) | Current-source corrections: the shard-distributor production lane now passes |
| [matching-acceptance-fixes-20261030.md](matching-acceptance-fixes-20261030.md) | Base: `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c0` (origin/t3/matching-rpc-callback-timestamp, retains 6c73d9aa/565c8fc and attempt diagnos… |
| [matching-acceptance-gate-gap-update-20261026.md](matching-acceptance-gate-gap-update-20261026.md) | Updated on `master` after reviewing `5d459e13b` and the current gate sources. |
| [matching-acceptance-shutdown-consolidated-20261030.md](matching-acceptance-shutdown-consolidated-20261030.md) | Candidate: `t3/matching-acceptance-shutdown-consolidated`. |
| [matching-acceptance-status-20261027.md](matching-acceptance-status-20261027.md) | **Superseded current status:** use |
| [matching-active-continuation-20261028.md](matching-active-continuation-20261028.md) | Matching remains incomplete; History migration must not start. Source base is |
| [matching-active-validation-checkpoint-20261025.md](matching-active-validation-checkpoint-20261025.md) | Matching remains incomplete. Implementation master before this documentation |
| [matching-adaptive-control-40473213335.md](matching-adaptive-control-40473213335.md) | Run: https://github.com/auenkr/cadence-rust/actions/runs/40473213335 |
| [matching-adaptive-go-control-review-20261027.md](matching-adaptive-go-control-review-20261027.md) | Reviewed `fed7fd7fa41e9b444439d9439686ff4628d20368` read-only; no production edits and no local build, test, or runtime gate. |
| [matching-adaptive-instrumentation.md](matching-adaptive-instrumentation.md) | For `production-adaptive-scaler-probe`, Rust Matching now has opt-in boundary tracing to distinguish scaler decisions from workflow-task… |
| [matching-adaptive-probe-parity-20261026.md](matching-adaptive-probe-parity-20261026.md) | This work starts from `origin/master` at `4a66ba20c5b3a45e58ab643742ef982ae5dbc497`. It incorporates the SDK completion probe change from… |
| [matching-adaptive-qps-source-audit-20261027.md](matching-adaptive-qps-source-audit-20261027.md) | Scope: bounded source comparison of Go adaptive scaling/QPS accounting and Rust `cadence-matching` at source SHA `7d072c254` (tag `matchi… |
| [matching-adaptive-routing-coverage-20261026.md](matching-adaptive-routing-coverage-20261026.md) | This is test harness and memory documentation only. It does not alter Go or Rust production code, configuration defaults, SDK assertions,… |
| [matching-adaptive-run-40373145351.md](matching-adaptive-run-40373145351.md) | - Failed Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40373145351 |
| [matching-adaptive-scaler-result.md](matching-adaptive-scaler-result.md) | Rust: rust/crates/cadence-matching/src/matching_scaler.rs (R). Go: go/service/matching/tasklist/adaptive_scaler.go (G). |
| [matching-adaptive-trace-extractor.md](matching-adaptive-trace-extractor.md) | Use `python3 tests/extract-matching-adaptive-trace.py <downloaded-log> [<another-log> ...] -o <output.json>` to extract `matching_adaptiv… |
| [matching-adaptive-two-go-control-limitation-20261027.md](matching-adaptive-two-go-control-limitation-20261027.md) | Do not add an opt-in two-Go-Matching mode in this subtask. The current |
| [matching-background-next.md](matching-background-next.md) | Active Matching; user explicitly requires continuing to complete all roles, one |
| [matching-cache-acceptance-review.md](matching-cache-acceptance-review.md) | Draft 66006fd0f remains unintegrated. Parent reviewed Go common/cache/domainCache.go and common/backoff/retrypolicy.go/retry.go against c… |
| [matching-cache-source-error-classification-20261027.md](matching-cache-source-error-classification-20261027.md) | `MatchingDomainCache::get_domain` and `get_domain_by_id` previously reduced every `DomainMetadataSourceError` to `DomainCacheError::Sourc… |
| [matching-candidate-repair-20261030.md](matching-candidate-repair-20261030.md) | The candidate starts from the exact reviewed parity tip |
| [matching-candidate-source-closure-review-20261029.md](matching-candidate-source-closure-review-20261029.md) | **Status: open.** Independent static review of remote `t3/matching-startup-candidate`: `git ls-remote` returned `8cc72372ad605dcdce4f03e8… |
| [matching-cannot-port-review-20261024.md](matching-cannot-port-review-20261024.md) | Source list: the 41 CANNOT-PORT rows in `memory/matching-go-test-map-20261023.md` (section "CANNOT-PORT (41)" and the per-test map). Read… |
| [matching-cassandra-acceptance-20261024.md](matching-cassandra-acceptance-20261024.md) | The Cassandra mixed Go/Rust production acceptance lane now passes on a standard GitHub Actions runner. The Rust binary was built on CI in… |
| [matching-cassandra-adapter-progress.md](matching-cassandra-adapter-progress.md) | Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-semaphore-durable` |
| [matching-cassandra-lane-diagnosis-20261024.md](matching-cassandra-lane-diagnosis-20261024.md) | --- |
| [matching-cassandra-parity-corrections-20261028.md](matching-cassandra-parity-corrections-20261028.md) | Source review: matching-native-cassandra-source-review-20261028.md. |
| [matching-cassandra-protocol-parity-20261021.md](matching-cassandra-protocol-parity-20261021.md) | - `go/common/config/loader.go` loads YAML with `uconfig.Expand(os.LookupEnv)` before populating `config.NoSQL`. The Docker Cassandra temp… |
| [matching-cassandra-rate-result.md](matching-cassandra-rate-result.md) | Date: 2026-10-25. Branch t3/mw-cassandra-rate (base t3/matching-integration-20261025). Source-only analysis; no cargo run, no gate dispat… |
| [matching-cassandra-raw-domain-20261029.md](matching-cassandra-raw-domain-20261029.md) | On base af4efc32b, Cassandra `SelectDomain` binds the original `*string` to a UUID column; pinned gocql `Marshal` dereferences it and `Pa… |
| [matching-cassandra-ttl-expiry-audit-20261021.md](matching-cassandra-ttl-expiry-audit-20261021.md) | Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-semaphore-durable` |
| [matching-cassandra-validation-runbook.md](matching-cassandra-validation-runbook.md) | Run the prepared sequence only after the shared build lane is released. Keep it |
| [matching-channel-state-attempt-diagnostics-20261030.md](matching-channel-state-attempt-diagnostics-20261030.md) | Candidate source branch: `t3/matching-channel-state-attempt-diagnostics` |
| [matching-checkpoint-20261030.md](matching-checkpoint-20261030.md) | Matching is still INCOMPLETE. Prior orchestration thread stopped (Codex usage limit, then 429s). |
| [matching-child-history-diagnostics-20261027.md](matching-child-history-diagnostics-20261027.md) | Extended the existing Go SDK failure diagnostic in `tests/go-sdk/mixed_matching.go`. |
| [matching-ci-evidence-audit-20261026.md](matching-ci-evidence-audit-20261026.md) | Read-only Actions audit and final-lane checklist. No workflow was dispatched, no running run was polled in a loop, and no local Cargo, Go… |
| [matching-ci-scheduler-evidence-20261029.md](matching-ci-scheduler-evidence-20261029.md) | **Status correction:** The earlier entry below records the then-live observation. Run 40841353614 later completed successfully; see the t… |
| [matching-client-partition-scope.md](matching-client-partition-scope.md) | Decision: NO unported piece is needed by the Matching process. Load balancers and partition-config provider are History/Frontend-only (de… |
| [matching-clippy-recovery-20261030.md](matching-clippy-recovery-20261030.md) | Matching remains incomplete. No mixed acceptance pass is credited. |
| [matching-clock-rate-next.md](matching-clock-rate-next.md) | Current uncommitted rate_limiter.rs ports Cadence common/clock/ratelimiter.go plus |
| [matching-cold-canonical-metric-attribution-20261027.md](matching-cold-canonical-metric-attribution-20261027.md) | Go `go/service/matching/handler/handler.go` resolves `h.domainName(request.DomainUUID)` before creating the handler metric context and be… |
| [matching-cold-domain-metric-probe-20261027.md](matching-cold-domain-metric-probe-20261027.md) | The regression in `rust/crates/cadence-matching/src/matching_cold_domain_metric_tests.rs` |
| [matching-cold-domain-publication-race-20261027.md](matching-cold-domain-publication-race-20261027.md) | The Matching domain-name resolver now publishes a cache discovery through |
| [matching-completion-audit-20261024.md](matching-completion-audit-20261024.md) | > Current validation status (2026-10-25): this document retains historical |
| [matching-completion-requirements-20261024.md](matching-completion-requirements-20261024.md) | > Current validation status (2026-10-25): this document retains historical |
| [matching-concurrent-completion-20261027.md](matching-concurrent-completion-20261027.md) | Parent correction: the public add RPCs DO expose `partition_config` (consumed |
| [matching-concurrent-go-parity-20261027.md](matching-concurrent-go-parity-20261027.md) | Source reviewed: `go/service/matching/handler/engine_integration_test.go`, |
| [matching-config-next.md](matching-config-next.md) | The durable matcher now reads `matching.numTasklistWritePartitions` and |
| [matching-config-property-audit-20261021.md](matching-config-property-audit-20261021.md) | Scope: every field constructed by pinned Go `go/service/matching/config/config.go` |
| [matching-configured-assembly-final-reconciliation-20261029.md](matching-configured-assembly-final-reconciliation-20261029.md) | Bounded source review against frozen runtime candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; this documentation follows it without… |
| [matching-continuation-20261025.md](matching-continuation-20261025.md) | Matching remains incomplete; production candidate is pinned at `27d3ff5f60f0604553824420da0cf61026eda468` on `matching-final-candidate-20… |
| [matching-continuation-20261027-0934.md](matching-continuation-20261027-0934.md) | Matching remains incomplete; do not begin History migration. |
| [matching-continuation-checkpoint-20261029-final-workspace.md](matching-continuation-checkpoint-20261029-final-workspace.md) | Frozen candidate remains bf9f3d9387a72c4ed26b4be0a85025e65fd485a1 on matching-native-final-20261029. Master holds evidence-only followups. |
| [matching-current-config-reconciliation-20261027.md](matching-current-config-reconciliation-20261027.md) | Static source audit only. Compared `go/service/matching/config/config.go` `NewConfig`, property definitions/defaults in `go/common/dynami… |
| [matching-current-ignore-reconciliation-20261027.md](matching-current-ignore-reconciliation-20261027.md) | Read-only source and assertion audit. Reconciles A1 in |
| [matching-current-parity-audit-20261020.md](matching-current-parity-audit-20261020.md) | This read-only audit updates the older production-gap checklist for the current |
| [matching-current-progress-20261031.md](matching-current-progress-20261031.md) | Matching remains incomplete. History migration has not started. This checkpoint saves current work on master without promoting the unacce… |
| [matching-db-lock-next.md](matching-db-lock-next.md) | Source gap before current working-tree correction: queue state mutex spans ID rollover |
| [matching-db06-workspace-checkpoint-20261030.md](matching-db06-workspace-checkpoint-20261030.md) | Frozen source: `db06adaa3ce183c59b85fc6b3c6029bc900b10f3`, branch `t3/matching-native-http2-diagnostics`. |
| [matching-deflake-result.md](matching-deflake-result.md) | Cause: timing tightness, not a production race. Run 40192622680 hit Elapsed on a 500ms bound; rerun 40193747635 passed. |
| [matching-delivery-delay-audit-40476007015-20261027.md](matching-delivery-delay-audit-40476007015-20261027.md) | This bounded source audit compared Go Matching's async backlog dispatch and |
| [matching-delivery-next.md](matching-delivery-next.md) | Ported Go's per-task source-domain SQL row keys with |
| [matching-dependency-oracles-plan-20261027.md](matching-dependency-oracles-plan-20261027.md) | Supplemental, focused CI gate; NOT a substitute for the ten full mixed lanes. |
| [matching-dependency-parity-review-20261025.md](matching-dependency-parity-review-20261025.md) | Independent source review only; no build, test, or runtime gate was run. This |
| [matching-discovery-contract.md](matching-discovery-contract.md) | Scope: production peer discovery and bootstrap/configuration needed to feed Rust Matching. This is an audit artifact, not a claim of comp… |
| [matching-domain-cache-ci-fix-20261030.md](matching-domain-cache-ci-fix-20261030.md) | Base: `origin/t3/matching-startup-abort-regression` at |
| [matching-domain-cache-error-parity-20261027.md](matching-domain-cache-error-parity-20261027.md) | Commit: `ef53987ee1176a4a4bfb5b321c9e376590fbccec` (`fix(matching): preserve domain cache error types`). |
| [matching-domain-cache-uuid-oracle-20261027.md](matching-domain-cache-uuid-oracle-20261027.md) | Status: original-Go UUID spelling oracle and the complete `cadence-matching` CI workflow passed on Actions. |
| [matching-domain-cache-uuid-review-20261027.md](matching-domain-cache-uuid-review-20261027.md) | - Reviewed oracle head `16f437bce` and its test source at `b93b162061cb32b62caecc22673241579edbec30`. |
| [matching-domain-metadata-sqlite.md](matching-domain-metadata-sqlite.md) | `rust/crates/cadence-persistence/src/matching_domain_metadata.rs` adds a |
| [matching-domain-uuid-result.md](matching-domain-uuid-result.md) | - Branch t3/mw-domain-uuid, CI-validated head 385ae6f2f680ece7f88f4d79b5963ec021e27417. CI: https://github.com/auenkr/cadence-rust/action… |
| [matching-dropin-review-20261031.md](matching-dropin-review-20261031.md) | Independent read-only review of c508c66d found the ten mixed lanes real (all eleven runs verified success at that SHA) but deployment par… |
| [matching-durable-restart-result.md](matching-durable-restart-result.md) | Failure: poll of partition /2 after SIGKILL restart returned durable-0 (root backlog) instead of durable-2. |
| [matching-dynamicconfig-audit-20261020.md](matching-dynamicconfig-audit-20261020.md) | Go startup in `go/common/dynamicconfig/dynamicconfigfx/fx.go` selects modern |
| [matching-dyncfg-key-matrix-20261023.md](matching-dyncfg-key-matrix-20261023.md) | Go: `go/service/matching/config/config.go` `NewConfig` + `constants.go` defaults/filters (filters shown = the getter used in NewConfig). |
| [matching-entrypoint-source-reconciliation-20261029.md](matching-entrypoint-source-reconciliation-20261029.md) | Bounded source review of the named legacy ignored rows at frozen candidate |
| [matching-error-matrix-20261023.md](matching-error-matrix-20261023.md) | Go: `go/service/matching/handler/{handler,engine,context}.go`, `go/common/types/mapper/proto/errors.go`. |
| [matching-failover-callback-integration-20261027.md](matching-failover-callback-integration-20261027.md) | Parent follow-up: added `matching_failover_callback_tests.rs`, registered by |
| [matching-failure-coverage-20261030.md](matching-failure-coverage-20261030.md) | Base: immutable Matching candidate `22c695d65af367a0b49b605877f72ec33fa2beb8` (the code SHA associated with CI run `41028655670`; validat… |
| [matching-final-acceptance-audit-20261026.md](matching-final-acceptance-audit-20261026.md) | Read-only source and evidence audit from `origin/master` / worktree base |
| [matching-final-acceptance-ledger-20261026.md](matching-final-acceptance-ledger-20261026.md) | Reconciled against current source pin `10692b2d5a5b912da051a80ca15c30f8236484dd` (master). The prior RPC matrix and gate plan remain usef… |
| [matching-final-candidate-20261026.md](matching-final-candidate-20261026.md) | Candidate branch: `matching-final-candidate` |
| [matching-final-gap-list-20261025.md](matching-final-gap-list-20261025.md) | Base `origin/t3/matching-integration-20261025`. Read-only, docs only: no cargo/go/fmt, no CI, no gate. Matching is NOT complete. |
| [matching-final-gate-plan-20261025.md](matching-final-gate-plan-20261025.md) | Nothing here was dispatched. No local Cargo/Go. Base: `t3/matching-integration-20261025`. |
| [matching-final-lanes-20261025.md](matching-final-lanes-20261025.md) | Workspace rust-ci 40188279906 success on fab70808b (not clean_target; fresh-target run still owed). |
| [matching-final-parity-20261024.md](matching-final-parity-20261024.md) | > Historical audit: its original blockers below are not the current backlog. |
| [matching-final-query-wire-review-20261027.md](matching-final-query-wire-review-20261027.md) | Read-only source review requested alongside |
| [matching-final-rpc-acceptance-matrix-20261025.md](matching-final-rpc-acceptance-matrix-20261025.md) | Source pin: Rust implementation `6af476941031193317fdd6c44070e9785e6384b9`, current master `fd1f47e10514a9d0a058937bf1354a819e46756f` (do… |
| [matching-final-signoff-review-20261025.md](matching-final-signoff-review-20261025.md) | Matching is INCOMPLETE. This review does not mark completion. |
| [matching-fixed-config-gc-final-review-20261027.md](matching-fixed-config-gc-final-review-20261027.md) | Read-only source audit to close the three named rows in `matching-current-config-reconciliation-20261027.md`: fixed task range size, one-… |
| [matching-format-remediation-20261025.md](matching-format-remediation-20261025.md) | - Source commit: `e0e897b7960ab60f484239c9518b9d2e1fc6de8e`. |
| [matching-gate-artifact-review-20261027.md](matching-gate-artifact-review-20261027.md) | Use after each of the ten serial gate runs in |
| [matching-gate-evidence-collector-20261027.md](matching-gate-evidence-collector-20261027.md) | Use the downloaded artifact's root directory (the directory with commit.txt): |
| [matching-gate-readiness-20261027.md](matching-gate-readiness-20261027.md) | Scope: dispatch and artifact-path review only. No gate was dispatched and no |
| [matching-gc-next.md](matching-gc-next.md) | Prior pushed checkpoint: 0d0952ff8 (per-owner ack pump and stopped-owner cleanup). |
| [matching-go-error-parity-ci.md](matching-go-error-parity-ci.md) | - rust-ci.yml package=cadence-matching on t3/matching-go-error-parity head 1b1b37da6a0c81bfe04a0a92191c3e942b08799f: run 40185780201 http… |
| [matching-go-parity-audit-v2-20261022.md](matching-go-parity-audit-v2-20261022.md) | This is a source audit of the committed integration branch `origin/t3/matching-integration-20261021`, checked out at `f17c25d95` (merge c… |
| [matching-go-parity-audit-v3-20261023.md](matching-go-parity-audit-v3-20261023.md) | Base: `origin/master` @ `8212a2db0` (merge-all + crate split). Read-only source audit: no cargo, no build, no gate was run for this docum… |
| [matching-go-parity-audit-v4-20261023.md](matching-go-parity-audit-v4-20261023.md) | Base: `origin/master` @ `92668eb2f`. Read-only source audit. No cargo, no build, no gate was run for this document; no code was edited. |
| [matching-go-parity-fixes-20261022.md](matching-go-parity-fixes-20261022.md) | Implements the Rust changes in `memory/matching-audit-verification-20261022.md` |
| [matching-go-test-function-map-20261027.md](matching-go-test-function-map-20261027.md) | Read-only review. No source changes, nothing run locally (Cargo/Go only on CI). This does NOT claim whole-service completion. |
| [matching-go-test-inventory-20261023.md](matching-go-test-inventory-20261023.md) | Branch: `matching-go-tests-port-20261023` (from origin/master a028331fb). |
| [matching-go-test-inventory-port2-20261023.md](matching-go-test-inventory-port2-20261023.md) | Branch: `matching-go-tests-port2-20261023` (from origin/master `ff6d4b9f1`). |
| [matching-go-test-inventory-port3-20261023.md](matching-go-test-inventory-port3-20261023.md) | Branch: `matching-go-tests-port3-20261023` (from origin/master `92668eb2f`). |
| [matching-go-test-map-20261023.md](matching-go-test-map-20261023.md) | Branch `matching-test-inventory-final-20261023` (from origin/master `d03e99474`). Read-only on code; nothing was compiled or run here. |
| [matching-grpc-probe-connection-diagnostic-20261030.md](matching-grpc-probe-connection-diagnostic-20261030.md) | The queued-activity candidate is `565c8fc91893fae1a81cd8e47f10f2ee4744c046` |
| [matching-handler-test-inventory-20261023.md](matching-handler-test-inventory-20261023.md) | Branch `matching-handler-tests-20261023`. Go spec: go/service/matching/handler/*_test.go. |
| [matching-heap-profile-20261115.md](matching-heap-profile-20261115.md) | Branch: `matching-heap-profile`, based on `3a6a5a44fcb20fe871b6093526d447d01417a0ff`. |
| [matching-historical-differences-audit-20261025.md](matching-historical-differences-audit-20261025.md) | Source-only reconciliation on candidate production tree `27d3ff5f60f0604553824420da0cf61026eda468` (checkout `4b26f1ef166ebcdcb15a2b86c04… |
| [matching-history-outbound-transport-20261021.md](matching-history-outbound-transport-20261021.md) | The audit item saying production Rust Matching rejects |
| [matching-history-routing-implementation.md](matching-history-routing-implementation.md) | Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/t3-83afa06b` (source-only |
| [matching-history-timeout-audit.md](matching-history-timeout-audit.md) | The original audit below omitted production timeout and retry wrappers. Do not use its unbounded-call or retry-removal conclusions. See `… |
| [matching-history-wire-reactivation-20261027.md](matching-history-wire-reactivation-20261027.md) | Source `47df125830183fc06c9980dbc428ac9ac1b26254` repairs the verifier's |
| [matching-history-wrapper-correction.md](matching-history-wrapper-correction.md) | The earlier timeout audit traced only the raw redirect client and is incomplete for production. |
| [matching-http-listener.md](matching-http-listener.md) | The earlier claim that fragmented TLS ClientHello records should successfully |
| [matching-idle-next.md](matching-idle-next.md) | Prior pushed checkpoint: cbd02ab7c (333 tests / 24 compatibility stages). |
| [matching-ignored-rpc-audit-20261025.md](matching-ignored-rpc-audit-20261025.md) | Audited at `6af476941`. |
| [matching-ignored-tests-resolution-20261027.md](matching-ignored-tests-resolution-20261027.md) | - Cause hypothesis: fixture History peer was 127.0.0.1:1 (unreachable), so the sync-matched task's RecordDecisionTaskStarted (Go engine.g… |
| [matching-ignored-tests-result.md](matching-ignored-tests-result.md) | Base: t3/matching-integration-20261025 (aa268a8d3). Gap-list item 3 and 7 were stale: the named tests are already un-ignored on this base. |
| [matching-inbound-metrics.md](matching-inbound-metrics.md) | Go's inbound Matching handler uses operation scopes from `go/common/metrics/defs.go`. `MatchingAddTaskScope` is an internal task-manager… |
| [matching-independent-review-20261025.md](matching-independent-review-20261025.md) | Candidate reviewed: `27d3ff5f60f0604553824420da0cf61026eda468`. This checkout is parent `af0609dbe676fe09dae62be3735ed73fd12745c0`; the c… |
| [matching-integration-20261027.md](matching-integration-20261027.md) | Focused run https://github.com/auenkr/cadence-rust/actions/runs/40464209349 |
| [matching-integration-control-20261027.md](matching-integration-control-20261027.md) | Integrated Go cache oracle and review, Go Worker startup readiness coordination, |
| [matching-integration-evidence-20261025.md](matching-integration-evidence-20261025.md) | - URL: https://github.com/auenkr/cadence-rust/actions/runs/40177106445 |
| [matching-integration-plan-20261020.md](matching-integration-plan-20261020.md) | This is a read-only integration review. No candidate branch was merged, no |
| [matching-isolation-dispatch-next.md](matching-isolation-dispatch-next.md) | Following verified reporting checkpoint5484f8ac8, implements group-aware durable |
| [matching-isolation-next.md](matching-isolation-next.md) | Continuation after scoped dynamic config commit f4bb9a5d5. In-progress change: |
| [matching-isolation-parity-audit-20261027.md](matching-isolation-parity-audit-20261027.md) | Audited current source `9e719ab6e6c96d8c64d4880868a18ea602bad2ef` on |
| [matching-lane-progress-20261027.md](matching-lane-progress-20261027.md) | Master 6af476941 included: go-error-parity, runid, respond-query, domain-uuid, ignored-tests/poller, absent-query, nil-tasklist test, tch… |
| [matching-late-domain-name-probe-20261027.md](matching-late-domain-name-probe-20261027.md) | Extended the existing `--matching-cold-domain-metric-probe` fixture. After |
| [matching-limiter-next.md](matching-limiter-next.md) | Native queue owners now enforce Go's task-list dispatch rate. This connects the |
| [matching-list-partitions-domain-cache-deadline-20261030.md](matching-list-partitions-domain-cache-deadline-20261030.md) | The claim that Rust `ListTaskListPartitions` is missing inbound deadline |
| [matching-list-partitions-trace-review-a534baae.md](matching-list-partitions-trace-review-a534baae.md) | The opt-in trace is activated end to end for the PostgreSQL production lane: |
| [matching-longpoll-config-source-audit-20261025.md](matching-longpoll-config-source-audit-20261025.md) | Audited at `6af476941031193317fdd6c44070e9785e6384b9` before this note. |
| [matching-luna-dispatch-20261030.md](matching-luna-dispatch-20261030.md) | Matching remains incomplete. Two new T3 delegates were accepted as running by |
| [matching-luna-integration-handoff-20261030.md](matching-luna-integration-handoff-20261030.md) | User reduced concurrency to ONE worker. Retain integration task |
| [matching-m3-port.md](matching-m3-port.md) | The isolated Rust sink is `cadence_server::matching_metrics_m3::M3MetricSink`. |
| [matching-maintenance-live-config.md](matching-maintenance-live-config.md) | This patch wires the existing acknowledgement and idle interval settings into |
| [matching-maintenance-next.md](matching-maintenance-next.md) | Prior verified checkpoint: 0e7dbae0e (327 workspace tests, 23 compatibility stages). |
| [matching-master-merge-20261025.md](matching-master-merge-20261025.md) | Master 00160c9da: merged go-error-parity, activity-runid-regression, respond-query-task-parity, domain-uuid, ignored-tests, absent-query… |
| [matching-master-sync-20261029.md](matching-master-sync-20261029.md) | User requested save/progress synchronization to remote master, then switching |
| [matching-master-validation-6af476941.md](matching-master-validation-6af476941.md) | Inspected 2026-10-25. Runs below target the exact SHA shown in each gate artifact. Links are authoritative run records. CI was neither di… |
| [matching-membership-audit-20261023.md](matching-membership-audit-20261023.md) | Branch `matching-membership-parity-20261023` (from master a028331fb). Rows: audit v3 "Ringpop bootstrap and membership", |
| [matching-membership-next.md](matching-membership-next.md) | Whole-service goal remains active; do not call single-node parity complete. |
| [matching-membership-standby-regressions-20261027.md](matching-membership-standby-regressions-20261027.md) | UNVERIFIED: nothing was run locally. The parent dispatches the Actions gate (`gh workflow run rust-ci.yml --ref <branch> -f package=caden… |
| [matching-merged-fatal-partition-review-20261027.md](matching-merged-fatal-partition-review-20261027.md) | Scope: source audit of merged commits `d9e190d3d` and `b07c80398` at `967d4a6e7`. No Cargo, Go, formatting, build, or test command was ru… |
| [matching-metrics-emission-comparison-20261024.md](matching-metrics-emission-comparison-20261024.md) | - Branch: `t3/matching-metrics-emission-parity` |
| [matching-metrics-matrix-20261023.md](matching-metrics-matrix-20261023.md) | Base: origin/matching-candidate-20261023 (4f5fb6722). Source audit, not a runtime parity claim. |
| [matching-metrics-migration-next-20261024.md](matching-metrics-migration-next-20261024.md) | Primary-source review on integration candidate b67072ec2 (2026-10-24). |
| [matching-metrics-migration-parity-20261024.md](matching-metrics-migration-parity-20261024.md) | Implementation recovered on `t3/matching-metrics-parity` from the focused |
| [matching-metrics-prometheus.md](matching-metrics-prometheus.md) | Date: 2026-10-20 |
| [matching-metrics-runtime-next.md](matching-metrics-runtime-next.md) | The Rust Matching metrics sinks now have a production configuration path through |
| [matching-mysql-ack-persistence-audit-20261020.md](matching-mysql-ack-persistence-audit-20261020.md) | Audit of the claim that "Go persists the acknowledgement inside |
| [matching-mysql-gate-harness-20261020.md](matching-mysql-gate-harness-20261020.md) | Branch: `codex/matching-task-store-abstraction` (base `4bc9873b9`). |
| [matching-mysql-gc-result.md](matching-mysql-gc-result.md) | Failure: only mainDecision kept 1 task row with persisted ackLevel 2 for 90s; every other queue was clean. |
| [matching-mysql-production-mixed-run-20261020.md](matching-mysql-production-mixed-run-20261020.md) | The first production MySQL mixed run finished with `EXIT=0` and |
| [matching-mysql-production-wiring-20261020.md](matching-mysql-production-wiring-20261020.md) | This source-only checkpoint follows merge commit `8d8c47854`, which merged the accepted runtime branch through `4e1fedba4`. The later run… |
| [matching-mysql-validation-20261020.md](matching-mysql-validation-20261020.md) | Source branch: `codex/matching-task-store-abstraction` |
| [matching-name-readthrough-20261027.md](matching-name-readthrough-20261027.md) | Commit `9bca9b856` (`fix(matching): read through domains by name in listing RPCs`) |
| [matching-native-acceptance-ledger-20261029.md](matching-native-acceptance-ledger-20261029.md) | Latest continuation save: `fa82a5b417d793d7e05767b7d5496b1d58150827` was the |
| [matching-native-activation-worker-20261028.md](matching-native-activation-worker-20261028.md) | Base `47743a1f066fc87ff0f179171de52f454daedbd5`. Matching remains incomplete. Configured assembly now calls `attach_live_native_registry`… |
| [matching-native-active-audit-test-evidence-20261029.md](matching-native-active-audit-test-evidence-20261029.md) | Reviewed the locally retained full Rust test log for Actions run |
| [matching-native-adapters-integration-checkpoint-20261028.md](matching-native-adapters-integration-checkpoint-20261028.md) | Matching remains incomplete; the native adapters are not production dispatch. |
| [matching-native-add-activity-20261028.md](matching-native-add-activity-20261028.md) | Unverified implementation in `matching_engine_native.rs`: activity and decision adds share the native queue/forwarding path. Activity own… |
| [matching-native-add-decision-20261028.md](matching-native-add-decision-20261028.md) | `matching_engine_native.rs` now contains an additive native decision add queue flow: byte-preserving task/list data, checked domain UUID… |
| [matching-native-cassandra-backend-20261028.md](matching-native-cassandra-backend-20261028.md) | Status: implemented, **unverified** (no Cargo run; parent owns CI). |
| [matching-native-cassandra-source-review-20261028.md](matching-native-cassandra-source-review-20261028.md) | Scope: `rust/crates/cadence-persistence/src/matching_cassandra_native.rs` (N), |
| [matching-native-checkpoint-20261029.md](matching-native-checkpoint-20261029.md) | Documentation-only state on shared `master`. Matching remains incomplete; no |
| [matching-native-ci-resume-20261028.md](matching-native-ci-resume-20261028.md) | Matching remains incomplete. Production adoption, final workspace validation, mixed Go/Rust SDK workflows and failure/restart/recovery ev… |
| [matching-native-codec-source-review-20261028.md](matching-native-codec-source-review-20261028.md) | The purported confirmed Int64Value defect below is withdrawn: pinned prost |
| [matching-native-codecs-combined-checkpoint-20261028.md](matching-native-codecs-combined-checkpoint-20261028.md) | Matching remains incomplete and production dispatch has not changed. |
| [matching-native-config-filters-20261028.md](matching-native-config-filters-20261028.md) | Implemented additive byte-name typed accessors in |
| [matching-native-configured-assembly-20261028.md](matching-native-configured-assembly-20261028.md) | Matching remains incomplete and production continues to use the legacy durable registry. `assemble_configured` constructs a byte-store re… |
| [matching-native-control-context-20261028.md](matching-native-control-context-20261028.md) | Added `NativeMatchingControlContext` in |
| [matching-native-deadline-cancellation-20261028.md](matching-native-deadline-cancellation-20261028.md) | Go `getOrCreateTaskListManager` passes the handler context to shard admission and starts an accepted manager with `context.Background()`.… |
| [matching-native-decision-response-20261028.md](matching-native-decision-response-20261028.md) | Updated `rust/crates/cadence-matching/src/thrift_matching_native.rs` to encode |
| [matching-native-delivery-pipeline-review-20261028.md](matching-native-delivery-pipeline-review-20261028.md) | Reviewed `rust/crates/cadence-matching/tests/native_delivery_pipeline.rs` against the public native actors and `go/proto/internal/uber/ca… |
| [matching-native-delivery-pipeline-tests-20261028.md](matching-native-delivery-pipeline-tests-20261028.md) | Added `rust/crates/cadence-matching/tests/native_delivery_pipeline.rs`: SQLite byte store, durable queue, writer, reader, sync matcher an… |
| [matching-native-dispatch-20261028.md](matching-native-dispatch-20261028.md) | Status: UNVERIFIED. Source written without any local Cargo/rustfmt/Go run; parent CI must compile, format and run it. Not registered in `… |
| [matching-native-dispatch-integration-tests-20261028.md](matching-native-dispatch-integration-tests-20261028.md) | Added to `matching_dispatch_native.rs` test module (scripted `NativeMatchingApi`, `NativeForwardState` with Service parent): |
| [matching-native-engine-20261028.md](matching-native-engine-20261028.md) | `matching_engine_native.rs` is an unwired foundation, not an RPC engine. It |
| [matching-native-engine-context-20261028.md](matching-native-engine-context-20261028.md) | Status: implementation added, **unverified**. No local Cargo, Go, rustfmt, build, |
| [matching-native-engine-control-20261028.md](matching-native-engine-control-20261028.md) | Scope: `matching_engine_control_native.rs` implements native cores for DescribeTaskList, GetTaskListsByDomain, CancelOutstandingPoll, Res… |
| [matching-native-engine-poll-20261028.md](matching-native-engine-poll-20261028.md) | Unverified: no Cargo, rustfmt, Go validation, CI dispatch, commit or push per assignment. `matching_engine_poll_native.rs` is a private c… |
| [matching-native-engine-query-20261028.md](matching-native-engine-query-20261028.md) | `matching_engine_query_native.rs` is an unregistered private child of `matching_engine_native.rs`. It implements queue selection, sticky… |
| [matching-native-error-boundary-audit-20261029.md](matching-native-error-boundary-audit-20261029.md) | Matching remains incomplete. This bounded audit reconciles the legacy ignored |
| [matching-native-fatal-shutdown-20261028.md](matching-native-fatal-shutdown-20261028.md) | MatchingFatal now retains SqlRunIdBytesError as its own variant. native_fatal_sinks supplies native writer/reader callbacks using the exi… |
| [matching-native-followup-audit-20261029.md](matching-native-followup-audit-20261029.md) | Audited source at `b728447489bf81f08c846ba43417be035ecb8538`, independently of the source-domain correction. No additional concrete handl… |
| [matching-native-forward-client-20261028.md](matching-native-forward-client-20261028.md) | File: `rust/crates/cadence-matching/src/matching_forward_native.rs` (new; not registered in `lib.rs`, parent must add `pub mod matching_f… |
| [matching-native-handler-integration-20261028.md](matching-native-handler-integration-20261028.md) | Scope: added `rust/crates/cadence-matching/src/matching_handler_native.rs` only, without registering the module or changing the launcher.… |
| [matching-native-handoff-checkpoint-20261028.md](matching-native-handoff-checkpoint-20261028.md) | Matching remains the current service and is incomplete. Do not start History migration or claim SDK compatibility from additive component… |
| [matching-native-history-control-20261028.md](matching-native-history-control-20261028.md) | Raw-byte (GoString) equivalents of legacy `mutable_state`, `describe_execution`, |
| [matching-native-history-control-grpc-tests-20261028.md](matching-native-history-control-grpc-tests-20261028.md) | UNVERIFIED: written without local Cargo; needs CI (rust-ci.yml, -p cadence-matching). |
| [matching-native-history-start-client-20261028.md](matching-native-history-start-client-20261028.md) | Status: implemented, NOT compiled or tested (no local Cargo by rule; parent owns CI). |
| [matching-native-history-tchannel-20261028.md](matching-native-history-tchannel-20261028.md) | Status: implemented, NOT built/tested (no local Cargo; parent owns CI). No production completion claim. |
| [matching-native-host-20261028.md](matching-native-host-20261028.md) | Status: implementation is **unverified**. No local Cargo, formatting, Go, build or test commands were run. The parent must register `matc… |
| [matching-native-host-admission-20261028.md](matching-native-host-admission-20261028.md) | Status: implementation source updated; **unverified**. No Cargo, rustfmt, Go, build, test, or check command was run locally. Parent CI ow… |
| [matching-native-http2-diagnostics-20261030.md](matching-native-http2-diagnostics-20261030.md) | Base: `a47e4032d94134b250ce0670cbd5f791d32b285b`. Integrated diagnostic sources `e566e025314e2a836798742ed0bc48f4c039e6cd` and `9d0ccb61b… |
| [matching-native-identifier-runid-checkpoint-20261028.md](matching-native-identifier-runid-checkpoint-20261028.md) | The previous focused common/API/persistence/proto CI **passed** at b2fccee5c: |
| [matching-native-ignore-closure-20261029.md](matching-native-ignore-closure-20261029.md) | Scope: ignored Rust assertions that bear on the frozen native Matching |
| [matching-native-integration-20261029.md](matching-native-integration-20261029.md) | Matching remains incomplete. Continue Matching only. |
| [matching-native-integration-checkpoint-20261028.md](matching-native-integration-checkpoint-20261028.md) | Native add/activity and poll cores were registered in 59c82e795; CI https://github.com/auenkr/cadence-rust/actions/runs/40684693575 concl… |
| [matching-native-isolation-runtime-20261028.md](matching-native-isolation-runtime-20261028.md) | Implemented in `rust/crates/cadence-matching/src/matching_runtime_native.rs`. |
| [matching-native-launcher-adoption-20261028.md](matching-native-launcher-adoption-20261028.md) | Production remains on the configured legacy durable queue path in `launcher.rs` (`MatchingTaskStoreSource::ConfiguredDefaultStore`). Nati… |
| [matching-native-live-slot-20261028.md](matching-native-live-slot-20261028.md) | Source change on base 9e21e168a: `NativeServiceSlot` stores only a weak `cadence_proto::native::NativeMatchingApi`, publishes after real… |
| [matching-native-membership-20261028.md](matching-native-membership-20261028.md) | Added byte-based `Ring::lookup_bytes` / `lookup_n_bytes`; existing UTF-8 entry points delegate to these. Raw membership owner/owns/remote… |
| [matching-native-mysql-backend-20261028.md](matching-native-mysql-backend-20261028.md) | `MySqlTaskStore` now implements `MatchingTaskStoreBytes` in |
| [matching-native-partition-cache-20261028.md](matching-native-partition-cache-20261028.md) | Status: written, NOT compiled or run (no local Cargo by policy). Parent must register and run CI. |
| [matching-native-partition-next.md](matching-native-partition-next.md) | Continuation after verified dispatch metadata commit 73e1dcc6a. No service role |
| [matching-native-persistence-faults-20261028.md](matching-native-persistence-faults-20261028.md) | Added `MatchingTaskStoreBytes` error injection in `rust/crates/cadence-matching/src/matching_fault_injection_native.rs`, privately regist… |
| [matching-native-persistence-metering-20261028.md](matching-native-persistence-metering-20261028.md) | Added `matching_persistence::limit_task_store_bytes` over `MatchingTaskStoreBytes`. Operations mirror the legacy rate-limited task store:… |
| [matching-native-poller-audit-correction-20261031.md](matching-native-poller-audit-correction-20261031.md) | At source 2c3bc4a9ab50a824a89b96bac33e40309ef587ef, the poller audit's claimed production timestamp/status mismatches are not established… |
| [matching-native-poller-qps-20261028.md](matching-native-poller-qps-20261028.md) | New files (not yet registered in `cadence-common/src/lib.rs`; parent must add |
| [matching-native-postgres-artifact-checklist-20261029.md](matching-native-postgres-artifact-checklist-20261029.md) | Use this for the frozen candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` (`matching-native-final-20261029`) and only the PostgreSQL l… |
| [matching-native-postgres-backend-20261028.md](matching-native-postgres-backend-20261028.md) | Files: `rust/crates/cadence-persistence/src/matching_postgres_native.rs` |
| [matching-native-postgres-discovery-diagnosis-20261030.md](matching-native-postgres-discovery-diagnosis-20261030.md) | Run [40808511931](https://github.com/auenkr/cadence-rust/actions/runs/40808511931) |
| [matching-native-production-assembly-20261028.md](matching-native-production-assembly-20261028.md) | Configured Matching now constructs the native runtime using the already fault-injected and rate-limited byte task store, limited byte sem… |
| [matching-native-production-seams-20261028.md](matching-native-production-seams-20261028.md) | Scope: read-only source audit of the requested Rust Matching launcher, registries, handler, gRPC/Thrift dispatch, and tonic status wrappe… |
| [matching-native-protobuf-codecs-20261028.md](matching-native-protobuf-codecs-20261028.md) | The worker's incomplete state below is superseded in source: parent added |
| [matching-native-query-20261028.md](matching-native-query-20261028.md) | Status: additive source written, NOT compiled or tested (no local Cargo by project rule). No runtime claim; CI validation pending by parent. |
| [matching-native-query-cancellation-20261028.md](matching-native-query-cancellation-20261028.md) | Scope: `rust/crates/cadence-matching/src/matching_query_native.rs` and `matching_engine_poll_native.rs`. `matching_engine_query_native.rs… |
| [matching-native-query-rendezvous-20261028.md](matching-native-query-rendezvous-20261028.md) | Scope: `matching_query_native.rs` and `matching_engine_query_native.rs`. Go references: `service/matching/tasklist/matcher.go` `OfferQuer… |
| [matching-native-query-selection-20261028.md](matching-native-query-selection-20261028.md) | Scope: `rust/crates/cadence-matching/src/matching_query_native.rs`; `matching_engine_query_native.rs` and `matching_engine_poll_native.rs… |
| [matching-native-queue-20261028.md](matching-native-queue-20261028.md) | New file: `rust/crates/cadence-persistence/src/matching_native_queue.rs` (parent must add `pub mod matching_native_queue;` to lib.rs). No… |
| [matching-native-queue-source-review-20261028.md](matching-native-queue-source-review-20261028.md) | Scope: read-only review of `rust/crates/cadence-persistence/src/matching_native_queue.rs` (`DurableTaskQueueBytes`) against `matching_que… |
| [matching-native-reader-20261028.md](matching-native-reader-20261028.md) | New file: `rust/crates/cadence-matching/src/matching_reader_native.rs`. No Cargo, rustfmt, Go or tests ran; parent owns CI. Legacy `match… |
| [matching-native-registry-20261028.md](matching-native-registry-20261028.md) | Status: **UNVERIFIED**. Written without local Cargo; the parent must add |
| [matching-native-registry-admission-20261028.md](matching-native-registry-admission-20261028.md) | The runtime installs its raw async shard ownership callback into a shared registry slot after configured or no-op executor setup; attachi… |
| [matching-native-registry-integration-20261028.md](matching-native-registry-integration-20261028.md) | Registered `matching_registry_native` in the Matching crate. Compared its native queue, store, writer, reader, dispatcher, sync matcher,… |
| [matching-native-registry-routing-20261028.md](matching-native-registry-routing-20261028.md) | Implemented source, not locally built or tested (per host policy): |
| [matching-native-request-metrics-20261028.md](matching-native-request-metrics-20261028.md) | Implementation (unverified; no Cargo, formatting, Go, build, test, CI, commit or push was run in this worker): |
| [matching-native-resume-state-20261029.md](matching-native-resume-state-20261029.md) | Current documentation-only master before this edit was |
| [matching-native-rpc-and-query-forward-checkpoint-20261028.md](matching-native-rpc-and-query-forward-checkpoint-20261028.md) | All thirteen native service trait methods are implemented in source, including required semaphore dispatch through a host integration cal… |
| [matching-native-runid-20261028.md](matching-native-runid-20261028.md) | Implemented additive backend resolution methods on `TaskDataBytes` in |
| [matching-native-runtime-adapters-20261028.md](matching-native-runtime-adapters-20261028.md) | Only `rust/crates/cadence-matching/src/matching_runtime_native.rs` was edited |
| [matching-native-runtime-admission-20261028.md](matching-native-runtime-admission-20261028.md) | Base 9e21e168a. Matching remains incomplete and native production activation is not established. This change exposes `MatchingRuntime::na… |
| [matching-native-runtime-lifecycle-20261028.md](matching-native-runtime-lifecycle-20261028.md) | - `rust/crates/cadence-matching/src/matching_runtime.rs:124-132` adds runtime-owned native registry callback state and a joined native me… |
| [matching-native-runtime-owner-20261028.md](matching-native-runtime-owner-20261028.md) | Added MatchingRuntime::with_native_queues(registry, service). It accepts an existing in-memory/runtime metadata owner before listeners st… |
| [matching-native-scaler-20261028.md](matching-native-scaler-20261028.md) | Added `matching_scaler_native.rs`: raw GoString partition/group maps, sustained scaling and isolation balancing, owner-scoped loop, cache… |
| [matching-native-scaler-attachment-20261028.md](matching-native-scaler-attachment-20261028.md) | Implementation: `rust/crates/cadence-matching/src/matching_scaler_runtime_native.rs` exposes `attach_native_scaler`, `config_provider`, `… |
| [matching-native-scaler-review-20261028.md](matching-native-scaler-review-20261028.md) | Scope: `matching_scaler_native.rs`, compared with Go `adaptive_scaler.go`, `isolation_balancer.go`, and the legacy Rust scaler and regist… |
| [matching-native-scaler-runtime-20261028.md](matching-native-scaler-runtime-20261028.md) | This work adds a production native isolation provider attachment in |
| [matching-native-scheduled-activity-20261028.md](matching-native-scheduled-activity-20261028.md) | The earlier worker reported that generated type shapes required a local Cargo build. The actual source prerequisite was the missing go/id… |
| [matching-native-scoped-metrics-20261028.md](matching-native-scoped-metrics-20261028.md) | Matching remains incomplete. This change scopes scaler observations per root key and emits the existing MatchingMetrics scaler gauges, sc… |
| [matching-native-semaphore-cancellation-20261028.md](matching-native-semaphore-cancellation-20261028.md) | Owned source changes: `rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs` and the semaphore service review at `rust/c… |
| [matching-native-semaphore-cassandra-20261028.md](matching-native-semaphore-cassandra-20261028.md) | Added `rust/crates/cadence-persistence/src/semaphores_cassandra_native.rs` as a child module of `cassandra.rs`, with a `SemaphorePersiste… |
| [matching-native-semaphore-cassandra-review-20261028.md](matching-native-semaphore-cassandra-review-20261028.md) | - Native adapter: `rust/crates/cadence-persistence/src/semaphores_cassandra_native.rs`. |
| [matching-native-semaphore-configured-stores-20261028.md](matching-native-semaphore-configured-stores-20261028.md) | Added open_configured_semaphore_stores returning legacy and native trait handles over one CassandraSemaphorePersistence and one configure… |
| [matching-native-semaphore-identifiers-20261028.md](matching-native-semaphore-identifiers-20261028.md) | Implemented only the owned, unregistered primitives: |
| [matching-native-semaphore-manager-20261028.md](matching-native-semaphore-manager-20261028.md) | Status: implementation and source review recorded; **unverified**. No local Cargo, Go, formatting, or build commands were run. Parent mus… |
| [matching-native-semaphore-service-20261028.md](matching-native-semaphore-service-20261028.md) | UNVERIFIED: source only; no Cargo, formatter, build, test, CI, or mixed-cluster run in this task. The new `matching_semaphore_service_nat… |
| [matching-native-semaphore-store-20261028.md](matching-native-semaphore-store-20261028.md) | Added `rust/crates/cadence-persistence/src/semaphores_native.rs` with the coordinated `SemaphorePersistenceBytes` seven-operation interfa… |
| [matching-native-service-20261028.md](matching-native-service-20261028.md) | The assigned `matching_service_native.rs` currently contains the service shell, |
| [matching-native-service-control-entrypoints-20261028.md](matching-native-service-control-entrypoints-20261028.md) | Implemented the seven requested inherent methods in |
| [matching-native-service-rpc-completion-20261028.md](matching-native-service-rpc-completion-20261028.md) | Scope: `rust/crates/cadence-matching/src/matching_service_native.rs` only, plus this note. This is the implementation subtask for the nat… |
| [matching-native-shard-routing-20261028.md](matching-native-shard-routing-20261028.md) | Scope: `matching_shard_distributor.rs` now decodes WatchNamespaceState shard keys and encodes GetShardOwner shard keys as protobuf bytes… |
| [matching-native-shard-runtime-20261028.md](matching-native-shard-runtime-20261028.md) | Partial implementation only; configured shard distributor is NOT ready for native launcher adoption. No local Cargo, formatting, builds o… |
| [matching-native-shared-router-20261028.md](matching-native-shared-router-20261028.md) | Implemented shared live router storage on NativeQueueRegistry, shared by its clones. Runtime::with_shard_router publishes the discovered… |
| [matching-native-source-domain-audit-20261029.md](matching-native-source-domain-audit-20261029.md) | Matching remains incomplete. Read-only source audit at |
| [matching-native-spectator-transport-tests-20261028.md](matching-native-spectator-transport-tests-20261028.md) | Extended the raw scripted tonic/HTTP2 peer test in `rust/crates/cadence-matching/src/matching_shard_distributor.rs`. The peer decodes `Ra… |
| [matching-native-sqlite-backend-20261028.md](matching-native-sqlite-backend-20261028.md) | Implemented in `rust/crates/cadence-persistence/src/matching_sqlite_native.rs` |
| [matching-native-standby-20261028.md](matching-native-standby-20261028.md) | `rust/crates/cadence-matching/src/matching_standby_native.rs` (new, not in lib.rs yet). |
| [matching-native-startup-independent-review-20261029.md](matching-native-startup-independent-review-20261029.md) | Read-only source review, no validation run. At review time local and remote master were `9a79103bb8cdeb191850452b10b965a02962b3dc`; froze… |
| [matching-native-status-20261028.md](matching-native-status-20261028.md) | File: `rust/crates/cadence-matching/src/matching_native_status.rs` (new; needs `mod matching_native_status;` in lib.rs, parent-owned). |
| [matching-native-status-reconciliation-20261029.md](matching-native-status-reconciliation-20261029.md) | At source pin `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`, the ignored test `matching_rpc_non_durable_describe_reports_task_id_block_and_e… |
| [matching-native-status-served-tests-20261028.md](matching-native-status-served-tests-20261028.md) | UNVERIFIED: written without compiling or running (no local Cargo; parent owns formatter/CI). |
| [matching-native-store-interface-20261028.md](matching-native-store-interface-20261028.md) | Added `matching_native_task_store.rs` as an additive persistence contract. It |
| [matching-native-store-selection-20261028.md](matching-native-store-selection-20261028.md) | ConfiguredMatchingStores now exposes `native_task_store` alongside the legacy view. SQLite, MySQL, PostgreSQL and Cassandra constructors… |
| [matching-native-sync-20261028.md](matching-native-sync-20261028.md) | New file `rust/crates/cadence-matching/src/matching_sync_native.rs`: `NativeSyncMatcher` / `NativeSyncTask`, |
| [matching-native-thrift-decision-adapters-20261028.md](matching-native-thrift-decision-adapters-20261028.md) | Parent review corrected the missing FromValue import, raw empty default, |
| [matching-native-thrift-dispatch-20261028.md](matching-native-thrift-dispatch-20261028.md) | Added a private child of `thrift_matching.rs` exposing `dispatch_native_with_context`. All ten methods in the pinned MatchingService Thri… |
| [matching-native-thrift-extra-20261028.md](matching-native-thrift-extra-20261028.md) | Parent review update: the module is registered in lib.rs. Wrong-typed scalar |
| [matching-native-thrift-responses-20261028.md](matching-native-thrift-responses-20261028.md) | New `rust/crates/cadence-matching/src/thrift_matching_native_responses.rs` |
| [matching-native-thrift-tcp-worker-20261028.md](matching-native-thrift-tcp-worker-20261028.md) | Added `native_transport_compat.rs` with a fake native API attached to production HTTP Thrift and TChannel listeners on real bounded TCP c… |
| [matching-native-tonic-20261028.md](matching-native-tonic-20261028.md) | Parent review corrected the include path to |
| [matching-native-transport-adoption-20261028.md](matching-native-transport-adoption-20261028.md) | `Matching::with_native_service` now attaches one byte-valued `NativeMatchingApi` to the existing listener handle. `grpc_matching` selects… |
| [matching-native-transport-integration-plan-20261028.md](matching-native-transport-integration-plan-20261028.md) | Parent evidence update: the downloaded raw-string artifact from run 40578298264 |
| [matching-native-wire-edge-checkpoint-20261028.md](matching-native-wire-edge-checkpoint-20261028.md) | Matching remains incomplete. Production handlers still use the UTF-8 model. |
| [matching-native-wire-edge-oracle-20261028.md](matching-native-wire-edge-oracle-20261028.md) | Status: **implemented, unverified**. The Go oracle and native Rust integration |
| [matching-native-worker-limit-checkpoint-20261028.md](matching-native-worker-limit-checkpoint-20261028.md) | The native semaphore manager worker terminated on its provider usage limit. It left `rust/crates/cadence-matching/src/matching_semaphore_… |
| [matching-native-writer-20261028.md](matching-native-writer-20261028.md) | New `rust/crates/cadence-matching/src/matching_writer_native.rs`: `NativeTaskWriter`, a port of `matching_writer.rs` |
| [matching-nil-query-domain-source-review.md](matching-nil-query-domain-source-review.md) | Source-derived only; no code/CI run. Paths under go/. Supersedes the first version of this note. |
| [matching-nil-tasklist-result.md](matching-nil-tasklist-result.md) | Go: handler.go QueryWorkflow uses nil-safe getters; engine.go QueryWorkflow -> tasklist.NewIdentifier(domain, "", Decision). newTaskListN… |
| [matching-open-gaps-20261024-v2.md](matching-open-gaps-20261024-v2.md) | Supersedes `memory/archive/matching-open-gaps-20261024.md` (base `e9b8e9a0e`). |
| [matching-openfeature-runtime-next-20261024.md](matching-openfeature-runtime-next-20261024.md) | This remains required even after provider polling and Unleash evaluation are complete. Current launcher registers the provider but Matchi… |
| [matching-orchestration-checkpoint-20261029-2203.md](matching-orchestration-checkpoint-20261029-2203.md) | Matching remains incomplete. No candidate source accepted onto master. No |
| [matching-orchestrator-checkpoint-20261030.md](matching-orchestrator-checkpoint-20261030.md) | Matching remains incomplete. Use one Codex gpt-6-luna worker, normal service tier; high effort for hard tasks, medium/low for simple task… |
| [matching-orchestrator-source-reconciliation-20261030.md](matching-orchestrator-source-reconciliation-20261030.md) | Matching remains incomplete. No mixed acceptance pass is established. |
| [matching-owner-order-oracle-20261024.md](matching-owner-order-oracle-20261024.md) | The former `engine_apis_partition_order_owners_and_lookup_errors_match_go` test |
| [matching-partition-deadline-metrics-audit-20261027.md](matching-partition-deadline-metrics-audit-20261027.md) | Read-only source audit, no Cargo/Go run. Supersedes the "ctx deadline -> Internal context deadline exceeded" row in `matching-partition-e… |
| [matching-partition-deadline-metrics-impl-20261027.md](matching-partition-deadline-metrics-impl-20261027.md) | Implements the fixes from matching-partition-deadline-metrics-audit-20261027.md (Go retry.go Do/prevErr verified: first attempt always ru… |
| [matching-partition-error-parity-resolution-20261027.md](matching-partition-error-parity-resolution-20261027.md) | Source reading only. No Cargo/Go/CI run; parent owns CI. Resolves open item 3 of `matching-persistence-deviation-resolution-20261027.md`. |
| [matching-partition-notification-deadline-20261021.md](matching-partition-notification-deadline-20261021.md) | Source-only follow-up to review of `d30f2ef85a65f34fd0e478d1c8345f97f3fcefe5`. |
| [matching-partitions-next.md](matching-partitions-next.md) | Closes the "partition count is a constant 1" gap noted in |
| [matching-pause-handoff-20261025.md](matching-pause-handoff-20261025.md) | The user-authorized one-time timer fired at 2026-10-25 10:40 UTC and was disabled |
| [matching-peer-callers-result.md](matching-peer-callers-result.md) | No code changed, so no CI run. |
| [matching-peer-lifecycle.md](matching-peer-lifecycle.md) | Implemented a transport-independent async `MatchingPeerProvider` boundary in |
| [matching-persistence-compat-audit-20261023.md](matching-persistence-compat-audit-20261023.md) | Branch `matching-persistence-compat-20261023` (from origin/master a028331fb). |
| [matching-persistence-deviation-resolution-20261027.md](matching-persistence-deviation-resolution-20261027.md) | Review only. Method: source reading. No Cargo, Go, CI or source edit. Follow-up to |
| [matching-persistence-production-audit-20261019.md](matching-persistence-production-audit-20261019.md) | Read-only source audit for Matching storage and recovery. Root reference is |
| [matching-persistence-qps-20261021.md](matching-persistence-qps-20261021.md) | Implemented shared host/cluster persistence QPS enforcement for Matching: |
| [matching-persistence-test-inventory-20261023.md](matching-persistence-test-inventory-20261023.md) | Branch `matching-persistence-tests-20261023`. Not run locally (no cargo); CI result at bottom. |
| [matching-pollers-next.md](matching-pollers-next.md) | Prior RPC/mixed SDK checkpoint ad9ebf44a is pushed. Added common::poller::Manager |
| [matching-post-tls-final-validation-plan-20261025.md](matching-post-tls-final-validation-plan-20261025.md) | Status: planning only. No workflow was dispatched and no Cargo/Go command was |
| [matching-postgres-41034380907-diagnosis.md](matching-postgres-41034380907-diagnosis.md) | - Run: https://github.com/auenkr/cadence-rust/actions/runs/41034380907 |
| [matching-postgres-activity-owner-crash-recovery-20261030.md](matching-postgres-activity-owner-crash-recovery-20261030.md) | Supplemental acceptance-harness change based on frozen production candidate |
| [matching-postgres-failure-41108824183-20261031.md](matching-postgres-failure-41108824183-20261031.md) | https://github.com/auenkr/cadence-rust/actions/runs/41108824183 is terminal, conclusion failure, exact head `2c3bc4a9ab50a824a89b96bac33e… |
| [matching-postgres-gate-progress-20261031.md](matching-postgres-gate-progress-20261031.md) | Branch `t3/matching-overlay-reentrancy-fix`. Matching is NOT complete; 0/10 mixed lanes accepted. |
| [matching-postgres-range-shard-filter-20261030.md](matching-postgres-range-shard-filter-20261030.md) | Based on consolidated process-evidence candidate `c22fdcf0a5a913ab67473e96cb63e1d15cd1d320`. |
| [matching-postgres-readiness-ci-checkpoint-20261030.md](matching-postgres-readiness-ci-checkpoint-20261030.md) | - Workflow: `rust-ci` (`workflow_dispatch`) |
| [matching-postgres-readiness-diagnosis-41108824183.md](matching-postgres-readiness-diagnosis-41108824183.md) | Source pin: `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`. Production binary SHA256: `bc45dd2dc600599fdd2f1135a58a918c878bb4f9ca93eaac5692f6… |
| [matching-postgres-readiness-shutdown-boundary-20261031.md](matching-postgres-readiness-shutdown-boundary-20261031.md) | - Source pin supplied for this investigation: `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`. |
| [matching-postgres-run-41123877578-20261031.md](matching-postgres-run-41123877578-20261031.md) | Failure, but readiness now passes: the reentrant overlay-lock fix removed the stall. Rust Matching started, SDK idle recovery and owner-c… |
| [matching-postgres-runtime-parity-audit-20261020.md](matching-postgres-runtime-parity-audit-20261020.md) | Scope: source-only implementation against pinned Go behavior from `55c4ae9ad`. |
| [matching-postgres-task-store-plan.md](matching-postgres-task-store-plan.md) | - Go's PostgreSQL plugin uses the same SQL `sqlTaskStore` operation layer as MySQL and SQLite, with PostgreSQL-specific statements and `s… |
| [matching-probe-scenarios-20261023.md](matching-probe-scenarios-20261023.md) | Status: **written, not run.** Validated only with `node --check`, `python3 -m py_compile` and |
| [matching-production-binary-gate.md](matching-production-binary-gate.md) | Source inspected: ac2c3c4fb (no production edits since ec059ca8e). Matching remains incomplete. |
| [matching-production-domain-cache-wiring.md](matching-production-domain-cache-wiring.md) | This is a bounded production integration checkpoint, not Matching completion. |
| [matching-production-gap-audit-20261018.md](matching-production-gap-audit-20261018.md) | Read-only source audit at checkout `691f750e7037eee004cc98417d3957613c8cc9bd`. |
| [matching-production-main-assembly-20261020.md](matching-production-main-assembly-20261020.md) | Source-only implementation on `t3/matching-runtime-owner`, rebased as |
| [matching-production-wiring-plan-20261019.md](matching-production-wiring-plan-20261019.md) | Source audit was corrected against accepted root `a3745a7d7` (the `b1a7f52e6` |
| [matching-progress-20261026.md](matching-progress-20261026.md) | User reduced the worker limit to **two Luna workers**. Keep all Cargo, Go, |
| [matching-progress-checkpoint-20261027-1530.md](matching-progress-checkpoint-20261027-1530.md) | Matching is incomplete. This checkpoint supersedes stale current-status claims |
| [matching-progress-snapshot-20261027.md](matching-progress-snapshot-20261027.md) | Candidate v5: tag `matching-acceptance-20261027-v5`, SHA 8d7dac38e. Ledger: matching-v5-final-acceptance-ledger-20261027.md. |
| [matching-qps-next.md](matching-qps-next.md) | Ported Go `common/stats`'s EMA fixed-window QPS tracker and wired it into the |
| [matching-query-next.md](matching-query-next.md) | Closes the "query delivery" item of Matching gate 4 for the **durable** matcher |
| [matching-rate-mixed.md](matching-rate-mixed.md) | `tests/run-compatibility.py --artifacts-dir .../matching-limiter-full |
| [matching-rate-next.md](matching-rate-next.md) | Next Matching work after pending rewrite checkpoint. Nothing in this note claims |
| [matching-raw-blob-20261028.md](matching-raw-blob-20261028.md) | Implemented additive byte-preserving SQL matching blob models and codecs in |
| [matching-raw-byte-string-progress-20261027.md](matching-raw-byte-string-progress-20261027.md) | The user explicitly selected **Match Go for raw bytes** on 2026-10-27. Invalid |
| [matching-raw-domain-source-order-20261029.md](matching-raw-domain-source-order-20261029.md) | Follow-up to 06f4094a8: raw cache keys are checked before persistence and remain byte-exact. On a miss the rate-limited source now receiv… |
| [matching-raw-json-20261028.md](matching-raw-json-20261028.md) | Added `GoString::go_json_string()` in `rust/crates/cadence-common/src/go_string.rs`. |
| [matching-raw-json-blob-checkpoint-20261028.md](matching-raw-json-blob-checkpoint-20261028.md) | Matching remains incomplete. Exact raw-byte parity is required. |
| [matching-raw-lookup-metrics-regression-20261029.md](matching-raw-lookup-metrics-regression-20261029.md) | At master `7828475bc`, added an inspecting source test in `matching_persistence.rs`. It blocks the raw domain lookup after entry, observe… |
| [matching-raw-protobuf-oracle-progress-20261028.md](matching-raw-protobuf-oracle-progress-20261028.md) | Matching remains incomplete. The user requires exact Go raw-byte behavior. |
| [matching-raw-runid-actions-oracle-20261025.md](matching-raw-runid-actions-oracle-20261025.md) | Added a manual-only workflow at `.github/workflows/matching-raw-runid-oracle.yml`. It runs on `ubuntu-latest`, has a 90-minute job timeou… |
| [matching-raw-runid-and-describe-boundary-20261025.md](matching-raw-runid-and-describe-boundary-20261025.md) | Source-only audit on master `61d510302` (Rust tree includes implementation `6af476941`). No Cargo, Go, formatter, CI, oracle, or mixed-cl… |
| [matching-reader-adaptive-trace-20261027.md](matching-reader-adaptive-trace-20261027.md) | This change adds opt-in tracing at the durable backlog reader boundary. It is limited to task lists whose parsed root is `production-adap… |
| [matching-reader-fatal-invariant-fix-20261027.md](matching-reader-fatal-invariant-fix-20261027.md) | No Cargo/CI run; nothing compiled or tested. |
| [matching-reader-fatal-wiring-complete-20261027.md](matching-reader-fatal-wiring-complete-20261027.md) | No Cargo/CI run yet; parent must dispatch `rust-ci.yml -f package=cadence-matching` (clippy + tests). Nothing staged/committed. |
| [matching-reader-next.md](matching-reader-next.md) | Active service: Matching. User reconfirmed service-by-service gates: validate Rust |
| [matching-readiness-diagnostic-current-status-20261030.md](matching-readiness-diagnostic-current-status-20261030.md) | The trace candidate is `a534baae8c921e0ec8109527c6bc3184937b0323` on |
| [matching-readiness-diagnostic-followup-41108824183.md](matching-readiness-diagnostic-followup-41108824183.md) | This source-only follow-up is based on the candidate ancestry containing |
| [matching-readiness-diagnostic-snapshot-20261030.md](matching-readiness-diagnostic-snapshot-20261030.md) | The readiness failure path now writes a separate |
| [matching-readiness-list-partitions-trace-20261030.md](matching-readiness-list-partitions-trace-20261030.md) | Source review only; no runtime, Cargo, Go, formatter, CI, or PR action. The active PostgreSQL run 41039044942 remains owned by its parent… |
| [matching-readiness-process-evidence-20261030.md](matching-readiness-process-evidence-20261030.md) | Candidate branch: `t3/matching-proc-evidence-consolidated`. Base: `be7016ef2d04dcb00fda946ddb67e3fbf15bae09` (`origin/t3/matching-accepta… |
| [matching-readiness-stall-41108824183.md](matching-readiness-stall-41108824183.md) | Artifact root: `/mnt/storage/service/ci-evidence/matching-postgres-41108824183.AY5Gf8/matching-production-postgres/`. |
| [matching-readiness-stall-root-cause-20261031.md](matching-readiness-stall-root-cause-20261031.md) | Source: GDB stacks in run 41114430171 (tag validation/matching-stall-stack-7b46ef884). Tokio workers blocked in std Mutex::lock on `Dynam… |
| [matching-recovery-evidence-consolidated-20261030.md](matching-recovery-evidence-consolidated-20261030.md) | Matching remains incomplete. Frozen source c760e1c57 on t3/matching-recovery-evidence-consolidated consolidates the Clippy atomic rename,… |
| [matching-refresh-fanout-parity-20261027.md](matching-refresh-fanout-parity-20261027.md) | This source pass found no concrete Go/Rust divergence in partition-config |
| [matching-refresh-final-review-20261027.md](matching-refresh-final-review-20261027.md) | Reviewed committed `94276d20fe09cc8dc79b678e3791533349caacad` as a read-only review. No source changes, Cargo/Go/rustfmt commands, CI dis… |
| [matching-refresh-retry-error-fix-20261027.md](matching-refresh-retry-error-fix-20261027.md) | Confirmed source mismatch: Go task_list_manager.go RefreshTaskListPartitionConfig |
| [matching-refresh-typed-history-status-tests-20261025.md](matching-refresh-typed-history-status-tests-20261025.md) | Branch: `t3/matching-refresh-error-tests` |
| [matching-resume-checkpoint-20261027.md](matching-resume-checkpoint-20261027.md) | Matching remains incomplete. Continue Matching before any History migration. |
| [matching-resume-checkpoint-20261029-ci40853270019.md](matching-resume-checkpoint-20261029-ci40853270019.md) | This is a documentation/evidence checkpoint only. Matching remains incomplete; |
| [matching-resume-schedule-20261027.md](matching-resume-schedule-20261027.md) | User requested an automatic trigger to resume this same T3 thread at 1:55 a.m. IST, including recovery after a usage-limit interruption. |
| [matching-retry-next.md](matching-retry-next.md) | Prior startup checkpoint 595638987 is pushed. Active service remains Matching. |
| [matching-rewrite-next.md](matching-rewrite-next.md) | Prior background checkpoint bf85c5226 is pushed. Active service remains Matching. |
| [matching-ringpop-removal-20261030.md](matching-ringpop-removal-20261030.md) | Investigated the production Ringpop lane in retained run |
| [matching-rpc-error-audit-29bdd2b04-20261025.md](matching-rpc-error-audit-29bdd2b04-20261025.md) | Method: source-only compare of `rust/crates/cadence-matching/src/grpc_matching.rs` (+ `thrift_matching.rs` code map: |
| [matching-rpc-next.md](matching-rpc-next.md) | Implemented opt-in Matching::with_durable_queues. AddDecisionTask and |
| [matching-rpc-quotas-20261021.md](matching-rpc-quotas-20261021.md) | Source implementation added after audit `359c5bb09`. This was source-only: |
| [matching-rss-allocator-experiment-20261115.md](matching-rss-allocator-experiment-20261115.md) | - Parent finding: [matching-rss-slope-20261115.md](matching-rss-slope-20261115.md); baseline is run 43629533136 (Rust Matching 7.281 MB/h… |
| [matching-rss-slope-20261115.md](matching-rss-slope-20261115.md) | - Soak: https://github.com/auenkr/cadence-rust/actions/runs/43629533136 |
| [matching-rssfe-20261114.md](matching-rssfe-20261114.md) | - **rust-fe lane (Rust Frontend + Go Matching): 100% query timeouts / "Connection reset" on CancelOutstandingPoll.** |
| [matching-runid-raw-parity.md](matching-runid-raw-parity.md) | Scope: Go Add handlers never validate `WorkflowExecution.RunID`; Rust eagerly uuid-parsed it (grpc_matching.rs decision/activity add) and |
| [matching-runid-syncmatch-result.md](matching-runid-syncmatch-result.md) | - Branch t3/mw-runid-syncmatch (base origin/t3/matching-integration-20261025, no code changes needed). |
| [matching-runtime-owner.md](matching-runtime-owner.md) | Go reference inspected in `go/service/matching/service.go`: |
| [matching-scaler-next.md](matching-scaler-next.md) | Prior increments: `a9531588d`, `ca062c861`, `d501f3779`, `79960a154` |
| [matching-semaphore-ci-scope-40912963402.md](matching-semaphore-ci-scope-40912963402.md) | Authoritative Actions inspection and downloaded `rust-ci-logs` artifact: |
| [matching-semaphore-next.md](matching-semaphore-next.md) | Prior increments: `a9531588d`, `ca062c861`, `d501f3779`, `79960a154`, |
| [matching-semaphore-rpc-audit-20261024.md](matching-semaphore-rpc-audit-20261024.md) | Scope: the Matching gRPC `AcquireSemaphore` handler (History calls it) compared with Go. Branch `matching-semaphore-rpc-20261024`, from `… |
| [matching-semaphore-startup-tests-20261029.md](matching-semaphore-startup-tests-20261029.md) | Source trace: Go `handler/handler.go` AcquireSemaphore replaces an engine error with the caller context error and increments the semaphor… |
| [matching-session-handoff-20261028.md](matching-session-handoff-20261028.md) | Matching is NOT complete. Continue Matching only; do not begin History migration. All 13 native RPC methods have source implementations,… |
| [matching-shard-distributor-audit-20261019.md](matching-shard-distributor-audit-20261019.md) | Source-only comparison of Go Matching's shard-distributor executor/resolver wiring and the current Rust Matching runtime. No build or tes… |
| [matching-shard-distributor-candidate-review.md](matching-shard-distributor-candidate-review.md) | Review target: branch `t3/matching-shard-distributor-candidate`, commits `8569a9f88` and `9a791267a`, rebased onto accepted root `160fcc6… |
| [matching-shard-distributor-client-contract-20261019.md](matching-shard-distributor-client-contract-20261019.md) | Scope: typed server configuration and the standalone Rust outbound executor/spectator assignment owner. This document records behavior fr… |
| [matching-shard-routing-contract.md](matching-shard-routing-contract.md) | Source audit of Go `common/membership/sharddistributorresolver.go`, `tasklist_differentiator.go`, `cmd/server/cadence/server.go:307-327`,… |
| [matching-shutdown-stage-diagnostics.md](matching-shutdown-stage-diagnostics.md) | Base: 6c73d9aa447171d4eeda37d71dd0a955d0996a9b. Context: memory/matching-shutdown-failure-41094318642.md (commit 822bda9c4, branch origin… |
| [matching-shutdown-trace-postgres-candidate-20261030.md](matching-shutdown-trace-postgres-candidate-20261030.md) | Candidate base: `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c` (includes the timestamped RPC callback probe and its assertions). Integrated so… |
| [matching-source-cache-persistence-reconciliation-20261029.md](matching-source-cache-persistence-reconciliation-20261029.md) | Read-only source review of frozen runtime candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`; checkout `5330521f1` differs only in the… |
| [matching-source-composition-20261021.md](matching-source-composition-20261021.md) | Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/t3-db1d9740`, branch |
| [matching-source-integration-20261030.md](matching-source-integration-20261030.md) | Integrated as one Matching-only source candidate on the current `origin/master` |
| [matching-source-reconciliation-20261030.md](matching-source-reconciliation-20261030.md) | This audit uses Git object ancestry and tree diffs; the previous count-reconciliation |
| [matching-sql-fatal-fixture-20261026.md](matching-sql-fatal-fixture-20261026.md) | The failing subprocess case was not reaching `TaskWriter`. The fixture launched `cadence-matching`, whose `main.rs` restricts its service… |
| [matching-sql-fatal-launcher-regression-20261025.md](matching-sql-fatal-launcher-regression-20261025.md) | - Isolated branch began at `2b66caf3edad528db2327b7c40907b0833e9f826`. |
| [matching-sql-malformed-domain-uuid-design.md](matching-sql-malformed-domain-uuid-design.md) | Builds on `memory/matching-nil-query-domain-source-review.md` (14ccc39b6). No code, no CI, **all tests unrun**. Paths: Rust under `rust/c… |
| [matching-sql-multidatabase-routing-20261021.md](matching-sql-multidatabase-routing-20261021.md) | Source inspected in this worktree after merging `e219c4730` (merge commit |
| [matching-stack-diagnostic-dispatch-20261031.md](matching-stack-diagnostic-dispatch-20261031.md) | Terminal failure at `7b46ef884`. The failure was NOT the readiness stall: the `Mixed Go and Rust validation` step failed in `tests/run-go… |
| [matching-stack-diagnostic-validation-20261031.md](matching-stack-diagnostic-validation-20261031.md) | Separate candidate branch `t3/matching-stall-stack-validation` now pins `7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6`. Production Rust sourc… |
| [matching-standby-error-exhaustion-tests-20261027.md](matching-standby-error-exhaustion-tests-20261027.md) | UNVERIFIED: no local Cargo, Go, rustfmt, build, or test commands were run. The parent owns GitHub Actions dispatch and validation. |
| [matching-standby-failover-query.md](matching-standby-failover-query.md) | User explicitly requires the WHOLE Matching service, with tests, before moving |
| [matching-startup-next.md](matching-startup-next.md) | Prior writer checkpoint 0fc071855 is pushed; read matching-writer-next.md. |
| [matching-startup-stage-diagnostics-20261030.md](matching-startup-stage-diagnostics-20261030.md) | Base: `origin/t3/postgres-gate-diagnostic` at |
| [matching-statsd-integrated-validation-20261030.md](matching-statsd-integrated-validation-20261030.md) | Matching remains incomplete. Frozen source 2c3bc4a9a on t3/matching-statsd-integrated-validation includes reviewed StatsD sender parity p… |
| [matching-statsd-review-checkpoint-20261030.md](matching-statsd-review-checkpoint-20261030.md) | Matching is incomplete; no mixed lane is accepted for the current source. |
| [matching-sticky-next.md](matching-sticky-next.md) | Closes the "sticky availability" item of Matching gate 4 for the durable |
| [matching-storage-next.md](matching-storage-next.md) | Active service remains Matching. Do not move to History/Frontend/Worker yet. |
| [matching-subsystem-audit-2-20261023.md](matching-subsystem-audit-2-20261023.md) | Base: origin/audit/matching-subsystem-20261023 (0e4e0c606). Read-only: no builds or tests were run. |
| [matching-subsystem-audit-20261023.md](matching-subsystem-audit-20261023.md) | Base: origin/matching-candidate-20261023 (4f5fb6722). Read-only; no builds or tests were run. |
| [matching-sync-forward-trace-20261023.md](matching-sync-forward-trace-20261023.md) | Question (from `memory/evidence/matching-forward-source-domain-20261023/notes.md`): does the |
| [matching-sync-forwarding-next.md](matching-sync-forwarding-next.md) | Prior increments: `a9531588d` (sync match), `ca062c861` (local ancestor sync |
| [matching-sync-raw-source-regression-adc203f5-20261029.md](matching-sync-raw-source-regression-adc203f5-20261029.md) | Based on d9ac938ba, added a native engine test exercising a forwarded activity sync match against a loopback History gRPC service. The so… |
| [matching-target-cleanup-20261027.md](matching-target-cleanup-20261027.md) | - Worktree inventory source: `git worktree list --porcelain`. |
| [matching-task-persistence-deep-audit-20261027.md](matching-task-persistence-deep-audit-20261027.md) | Scope: Go `service/matching/tasklist/{task_reader,task_writer,task_gc,task_completer}.go`, `common/messaging/ackManager.go` |
| [matching-task-store-abstraction.md](matching-task-store-abstraction.md) | Worktree: `/home/ubuntu/.t3/worktrees/cadence-rust/matching-task-store-abstraction` |
| [matching-tchannel-tls-result.md](matching-tchannel-tls-result.md) | Result: Go does NOT serve TLS on the TChannel inbound; no Rust change needed. |
| [matching-terra-orchestration-20261029.md](matching-terra-orchestration-20261029.md) | Matching remains incomplete. Do not begin History migration. This note records |
| [matching-test-parity-gaps-20261023.md](matching-test-parity-gaps-20261023.md) | Branch: `matching-liveness-tests-20261023` (from origin/master 92668eb2f). |
| [matching-testfix-20261024.md](matching-testfix-20261024.md) | - The two failures in crate-split-20261023/test-matching.log (matching_reader_local_poller_wins_after_forward_error, matching_registry re… |
| [matching-thrift-audit-resolution-20261027.md](matching-thrift-audit-resolution-20261027.md) | Source-only; no Cargo/Go run. Edited only `rust/crates/cadence-matching/src/thrift_matching.rs` (not committed; CI unrun). |
| [matching-thrift-decoder-review-20261027.md](matching-thrift-decoder-review-20261027.md) | Scope: uncommitted `thrift_matching.rs` (normalize, nil-request path), `grpc_matching.rs::thrift_nil_request`, |
| [matching-thrift-error-matrix-20261024.md](matching-thrift-error-matrix-20261024.md) | Scope: how each Matching error kind reaches the TChannel/Thrift wire. Compares Go exception types and fields against Rust. Companion to t… |
| [matching-thrift-full-decode-parity-20261027.md](matching-thrift-full-decode-parity-20261027.md) | Source-only, nothing compiled or run (no local Cargo/Go). Not committed/staged. CI unrun. |
| [matching-thrift-native.md](matching-thrift-native.md) | All ten pinned MatchingService methods and Meta::health now dispatch directly to |
| [matching-thrift-poll-routing-fixture-20261025.md](matching-thrift-poll-routing-fixture-20261025.md) | - Prior CI: https://github.com/auenkr/cadence-rust/actions/runs/40176622622 |
| [matching-throttled-log-20261021.md](matching-throttled-log-20261021.md) | ThrottledLogRPS (`matching.throttledLogRPS`) remains not implemented in Rust. |
| [matching-throttled-log-next.md](matching-throttled-log-next.md) | On branch `t3/matching-runtime-owner` at the task start (`c253324be`), Matching's Rust gRPC decision and activity poll handlers had no eq… |
| [matching-tlm-test-inventory-20261023.md](matching-tlm-test-inventory-20261023.md) | Go source: `go/service/matching/tasklist/task_list_manager_test.go` (2086 lines, 28 `Test` functions). |
| [matching-tls-collector-retention-20261027.md](matching-tls-collector-retention-20261027.md) | The compact Matching gate collector now retains the safe TLS proof emitted by |
| [matching-tls-runtime-evidence-20261027.md](matching-tls-runtime-evidence-20261027.md) | The final collector review identified that generated config redaction and |
| [matching-twonode-20261024.md](matching-twonode-20261024.md) | Result: PASS on GitHub Actions (workflow .github/workflows/matching-mixed-gate.yml; runs on push to branch matching-twonode-20261024 or w… |
| [matching-typed-sql-runid-validation-20261025.md](matching-typed-sql-runid-validation-20261025.md) | Reviewed and integrated source `21c16e33b6f150ade87acbc968f8fd523191574d`. |
| [matching-unleash-lifecycle-next-20261024.md](matching-unleash-lifecycle-next-20261024.md) | Current master polling implements bootstrap, periodic feature fetch, snapshot |
| [matching-v4-name-probe-throttle-20261027.md](matching-v4-name-probe-throttle-20261027.md) | Run https://github.com/auenkr/cadence-rust/actions/runs/40482436756 failed at |
| [matching-v6-ignored-tests-audit-20261027.md](matching-v6-ignored-tests-audit-20261027.md) | Candidate: tag matching-acceptance-20261027-v6, SHA 3055d0c6c. Read-only static audit; no tests, Cargo or Go run. Not a completion declar… |
| [matching-v6-lifecycle-persistence-audit-20261027.md](matching-v6-lifecycle-persistence-audit-20261027.md) | Candidate: `matching-acceptance-20261027-v6`, 3055d0c6c. Source reading only; no |
| [matching-v6-rpc-audit-20261027.md](matching-v6-rpc-audit-20261027.md) | Candidate: tag `matching-acceptance-20261027-v6`, SHA `3055d0c6c0c8932c14e12714b498b82569cfe0b5`. |
| [matching-v7-adaptive-offer-recovery-evidence-guide-20261027.md](matching-v7-adaptive-offer-recovery-evidence-guide-20261027.md) | Read-only mapping of current probes and collectors for the adaptive, active-offer, |
| [matching-v7-database-evidence-review-guide-20261027.md](matching-v7-database-evidence-review-guide-20261027.md) | Use with the v7 ledger and readiness map. This is a reviewer checklist for the |
| [matching-v7-final-acceptance-ledger-20261027.md](matching-v7-final-acceptance-ledger-20261027.md) | Matching remains incomplete. This ledger supersedes the current-status claims |
| [matching-v7-transport-discovery-evidence-guide-20261027.md](matching-v7-transport-discovery-evidence-guide-20261027.md) | Parent follow-up: the TLS retention gap below is addressed in source by |
| [matching-v7-two-node-dispatch-review-20261027.md](matching-v7-two-node-dispatch-review-20261027.md) | The `matching-mixed-gate.yml` workflow input `extra_args` defaults to |
| [matching-wire-field-matrix-20261023.md](matching-wire-field-matrix-20261023.md) | Scope: every Matching RPC on gRPC (proto) and Thrift. Branch `matching-wire-fields-20261023`. |
| [matching-workspace-evidence-40803510954.md](matching-workspace-evidence-40803510954.md) | Run [40803510954](https://github.com/auenkr/cadence-rust/actions/runs/40803510954) completed successfully for source `4d1007f3bdd0a6d6679… |
| [matching-workspace-oracles-20261031.md](matching-workspace-oracles-20261031.md) | Workspace validation remains in progress at source `2c3bc4a9ab50a824a89b96bac33e40309ef587ef`: |
| [matching-workspace-pass-postgres-dispatch-20261031.md](matching-workspace-pass-postgres-dispatch-20261031.md) | https://github.com/auenkr/cadence-rust/actions/runs/41107169881 completed successfully at exact source `2c3bc4a9ab50a824a89b96bac33e40309… |
| [matching-writer-live-config.md](matching-writer-live-config.md) | Cherry-picked candidate `c3266b87d81c317f3d2b0d96cfcadc674a5b2fe8` as local commit `c46058356` to wire the matching writer's live configu… |
| [matching-writer-next.md](matching-writer-next.md) | Status: A standalone bounded Rust actor primitive is implemented and tested. |
| [matching-writer-shutdown-race-20261026.md](matching-writer-shutdown-race-20261026.md) | Starting point: `origin/t3/sqlite-writer-fatal-exit` at |
| [matching-writer-transient-retry-fix-20261027.md](matching-writer-transient-retry-fix-20261027.md) | - `go/service/matching/tasklist/task_writer.go:95-97`: `throttleRetry` = `persistenceOperationRetryPolicy` + `WithRetryableError(persiste… |
| [matching-zero-dispatch-recovery-20261027.md](matching-zero-dispatch-recovery-20261027.md) | `matching_reader_zero_dispatch_recovers_same_durable_task_after_ttl` now verifies the |
| [merge-notes-20261023.md](merge-notes-20261023.md) | Merged in order with --no-ff onto origin/master: matching-mixed-gates, twonode, query-parity, http-tls, backlog-forward-v3, metrics-parit… |
| [migration-inventory.md](migration-inventory.md) | Public gRPC adapters: 57 / 57. |
| [monorepo-layout.md](monorepo-layout.md) | Since the 2026-10-14 move commit, the repo root holds three directories: |
| [mysql-final-40153901033-restart-diagnosis.md](mysql-final-40153901033-restart-diagnosis.md) | Candidate under diagnosis: `27d3ff5f60f0604553824420da0cf61026eda468`. |
| [orchestration-2026-10-19-live.md](orchestration-2026-10-19-live.md) | Parent starts at f6e6363d7. Matching remains incomplete; no later service work |
| [orchestration-2026-10-19-resume.md](orchestration-2026-10-19-resume.md) | Main source remains 139757411. Matching is incomplete; History has not started. |
| [orchestration-active.md](orchestration-active.md) | Main6977c326d now integrates timer core a1f802896 (worker8e4e993eb) and static/file discovery b73abb5ed+c735426a3+6977c326d (worker056ee6… |
| [perf-history-throughput.md](perf-history-throughput.md) | Baseline report: `Benchmark/reports/final-master-20261115.md` on `t3/matching-benchmark-harness`. |
| [performance-resume-20261118.md](performance-resume-20261118.md) | - Starting source/master: `571fb2a0862bca59bb799f7baae9518d26c0e7b4`; clean checkout, fetched origin. No implementation changes yet. |
| [performance-status-20261119.md](performance-status-20261119.md) | Covers merges 0a74e0940..56b58c6c1. |
| [platform-ci-speedup.md](platform-ci-speedup.md) | \| Workflow \| n \| median wall \| notes \| |
| [port-ledger-summary.md](port-ledger-summary.md) | This ledger audits all Go source files and top-level AST symbols, including |
| [progress-2026-10-21-pause.md](progress-2026-10-21-pause.md) | Codex limit reached; all workers now Claude Haiku 5.5 / Sonnet 5.5. All workers stopped at pause. |
| [progress-2026-10-23-0500.md](progress-2026-10-23-0500.md) | Matching is still NOT complete. History/Frontend/Worker untouched. All workers stopped; containers stopped (except pre-existing local-pos… |
| [progress.md](progress.md) | - **2026-10-31 — Frontend Rust bootstrap skeleton.** Added the |
| [project-goals.md](project-goals.md) | Rewrite the Cadence server (Go, under `go/`) in Rust (under `rust/`). |
| [repl-flake-p2b-20261115.md](repl-flake-p2b-20261115.md) | Branch: `repl-flake-p2b` |
| [replication-next.md](replication-next.md) | The Rust ReplicateEventsV2 adapter still returns success without applying any |
| [resume.md](resume.md) | Saved from shared `master` at `fa82a5b417d793d7e05767b7d5496b1d58150827`; |
| [review-c06aa9b1e-domain-quota-probe.md](review-c06aa9b1e-domain-quota-probe.md) | Review of `c06aa9b1e70632e216a2bf868c89b06d0c44fc0e` against prior finding |
| [review-cold-domain-readthrough-20261027.md](review-cold-domain-readthrough-20261027.md) | Scope: review of `grpc_matching.rs::domain_id_by_name` and `with_domain_cache`, |
| [review-matching-cold-cache-metrics-20261027.md](review-matching-cold-cache-metrics-20261027.md) | Reviewed source commit `c2e631b4383a2b61313654d47588da727f058bdb` (branch head `005b7849fbab2db3d4cd1b499831ec222626ac81`). Focused Actio… |
| [ringpop-bootstrap-next.md](ringpop-bootstrap-next.md) | Implemented `cadence_common::ringpop_bootstrap_selection` for Matching as a |
| [ringpop-bootstrap-provider.md](ringpop-bootstrap-provider.md) | Implemented a separate Rust API in `cadence_server::ringpop_discovery` for |
| [ringpop-direct-ping.md](ringpop-direct-ping.md) | Pinned implementation reference: `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0/swim/ping_sender.go`, `ping_handler.go`, `no… |
| [ringpop-disseminator-oracle.md](ringpop-disseminator-oracle.md) | `python3 tests/generate-ringpop-member-primitives.py` runs temporary tests inside |
| [ringpop-disseminator-verification.md](ringpop-disseminator-verification.md) | Applied the candidate `6c71b6774e55f439bc3647387fb544694277226d` and repair |
| [ringpop-healer-contract.md](ringpop-healer-contract.md) | Reference: `github.com/uber/ringpop-go@v0.10.0/swim` (`node.go`, |
| [ringpop-healer-validation-lane.md](ringpop-healer-validation-lane.md) | Prepared on 2026-10-19 after rebasing the healer/audit chain onto accepted |
| [ringpop-inbound-join-ping.md](ringpop-inbound-join-ping.md) | Date: 2026-10-18. This slice adds stateful inbound `/protocol/join` and |
| [ringpop-join-group.md](ringpop-join-group.md) | Status: implemented, focused Rust and actual pinned-Go peer comparisons pass. |
| [ringpop-join-harness-fix.md](ringpop-join-harness-fix.md) | `tests/run-ringpop-outbound-join-compat.py` previously reused |
| [ringpop-join-refutation.md](ringpop-join-refutation.md) | Current branch started at parent `9fb9a46cc7636e0bbd083d6eb45ebb40fbb63e29`; candidate reviewed: `9770e3870f1b6994ad6014c1e4e05e0a496732a`. |
| [ringpop-json-request-boundary.md](ringpop-json-request-boundary.md) | Source-only review at main c64587bdf; relay candidate in t3-6f14baa6 remains unintegrated. This note is a parity work item, not passing i… |
| [ringpop-matching-production-wiring.md](ringpop-matching-production-wiring.md) | Read-only source audit for the next Matching slice after `ringpop-swim-runtime`. |
| [ringpop-member-primitives-oracle.md](ringpop-member-primitives-oracle.md) | `tests/generate-ringpop-member-primitives.py` copies the pinned |
| [ringpop-member-primitives.md](ringpop-member-primitives.md) | Date: 2026-10-18. This slice adds deterministic, pure Rust functions in |
| [ringpop-memberlist-update-oracle.md](ringpop-memberlist-update-oracle.md) | `tests/generate-ringpop-memberlist-update.py` copies the pinned |
| [ringpop-outbound-ping-req.md](ringpop-outbound-ping-req.md) | This task adds the outbound half of Ringpop v0.10.0 `swim/ping_request_sender.go` to the Rust matching provider. Keep it within Matching;… |
| [ringpop-provider-next.md](ringpop-provider-next.md) | Research checkpoint: 2026-10-18. This document plans the missing production |
| [ringpop-state-transitions.md](ringpop-state-transitions.md) | This slice adds `cadence_common::ringpop_state_transitions`, a deterministic |
| [ringpop-swim-runtime.md](ringpop-swim-runtime.md) | Candidate branch `t3/matching-swim-runtime` starts at accepted Matching source |
| [ringpop-tchannel-next.md](ringpop-tchannel-next.md) | - Added `cadence_server::ringpop_tchannel`, an injectable async handler API. The |
| [ringpop-wire-contract.md](ringpop-wire-contract.md) | Scope: exact wire-level contract for Ringpop's SWIM endpoints used by Cadence production |
| [schedule-context-20261102.md](schedule-context-20261102.md) | - Rust `schedule_flow.rs::api_call` serialized the workflow RPC request then called `WorkflowApi::call` with `CallerContext::default()`.… |
| [server-jemalloc-default-20261115.md](server-jemalloc-default-20261115.md) | Branch: `server-jemalloc-default` |
| [service-map.md](service-map.md) | Upstream protobuf service inventory: **13 services, 164 methods**. Exact methods |
| [service-migration-plan.md](service-migration-plan.md) | User requires completing one of the four Go service roles, validating it with |
| [shard-distributor-mixed-gate-20261025.md](shard-distributor-mixed-gate-20261025.md) | Investigation against master base `6af476941031193317fdd6c44070e9785e6384b9` and the retained artifact at `/mnt/storage/service/ci-eviden… |
| [soak-rss-anon-20261115.md](soak-rss-anon-20261115.md) | Branch: `soak-rss-anon`, based on `23b4caa6f7c0063912eaaa8bce2948a2af4460d4`. |
| [source-domain-boundary-go-oracle-2026-10-29.md](source-domain-boundary-go-oracle-2026-10-29.md) | This change adds optional Go overlays to the existing `go_domain_cache_uuid_oracle` rust-ci dispatch. No local Go, Cargo, rustfmt, or gat… |
| [statsd-sender-parity-20261030.md](statsd-sender-parity-20261030.md) | Reference: Go's pinned `cactus/go-statsd-client` `sender_buffered.go` at |
| [storage-recovery-20261019.md](storage-recovery-20261019.md) | Disk pressure started at 111 MB free on `/dev/sda1` (96 GB filesystem, 100% used). Before cleanup, there were no active `cargo`, `rustc`,… |
| [twocluster-alldb-20261114.md](twocluster-alldb-20261114.md) | Branch twocluster-alldb, head 5e486319f19b59db0baa3ff555dd724e71aeeee4. |
| [unleash-repository-backoff-parity-20261024.md](unleash-repository-backoff-parity-20261024.md) | Source baseline: `ae442535c1d4ed708daeb38face1ab0e9d84f88a`. The original `t3/unleash-repository-backoff` branch was merged into isolated… |
| [worker-skip-use-go-worker-20261101.md](worker-skip-use-go-worker-20261101.md) | The user decided NOT to port the Worker service to Rust. The cluster will run the original Go Worker (go/service/worker) alongside the Ru… |
