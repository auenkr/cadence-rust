# Matching current parity reconciliation (2026-10-25)

Read-only audit at `6af476941031193317fdd6c44070e9785e6384b9` (master). This reconciles older gap-list claims against current source and git ancestry; it is not a full Go source sweep or a service sign-off. No Cargo/Go/fmt, CI dispatch, or mixed gate was run.

## Ancestry and stale claims

The following previously cited fixes are ancestors of current master: semaphore row-skip `071544885`, lifecycle `a574fe5f3`, non-durable Describe fixes `c460d1d5e`, and semaphore deadline `4940e5809` (`git merge-base --is-ancestor` returned success for each). The old recommendation to merge these branches is stale. At the current head, the semaphore loader follows page tokens in `matching_semaphore.rs:247-265`; `Acquire` accepts and enforces the caller deadline while startup is loading at `:349-378`; non-durable Describe and lifecycle changes are already integrated. Verify current tests in CI before treating those commits as validated on this head.

Other withdrawn historical claims:

| Old claim | Current evidence | Disposition |
|---|---|---|
| `matching.errorInjectionRate` is Rust-only alias | `launcher.rs:767-769`; Go constant `go/common/dynamicconfig/dynamicproperties/constants.go:2514-2519`, client read `go/client/clientfactory.go:183-185` | **Closed / false alarm.** This is a valid Go key and must remain. |
| Raw batch accessor returns validated default rather than raw input | `matching_reader.rs:1123-1138` tests raw 0/-5 and validated read fallback; Go task reader validates at `go/service/matching/tasklist/task_reader.go:108-116,300-308` | **Closed as production gap.** Getter shape differs internally, but actual read behavior matches the Go fallback. |
| Semaphore paging/deadline unimplemented | `matching_semaphore.rs:247-265,349-378` | **Closed in current production source.** Old ignored-test references no longer prove a gap; inspect whether each exact test is ignored before counting. |
| ListTaskListPartitions Go owner order absent | `memory/matching-final-signoff-review-20261025.md` records Go ring oracle CI `40168414544` for baseline/add/remove/identity/address replacement/empty-ring/root-child-empty/decision-activity. Current master descends from prior review lineages, but this audit did not trace that exact test tree to this SHA. | **Likely closed; exact current-head CI evidence unverified.** |

## Current actionable matrix

| Rank | Finding | Classification | Evidence / next action |
|---|---|---|---|
| 1 | Matching SDK/mixed-service final evidence is not complete at this head. | **Unverified acceptance blocker**, not a source bug | `memory/matching-final-lanes-20261025.md` records green lanes on older pins, Go-baseline failures, missing per-lane final binary hashes, and outstanding reruns. Run the serial required lanes on one final SHA, inspect artifacts and record SHA256, run clean-target CI. No prior green lane substitutes for current-pin evidence. |
| 2 | `decision_started_after_long_poll_window_still_returns_task` remains ignored in `matching_longpoll_expiry_parity.rs:212+`; its oracle scenario is documented as Rust expiring the in-flight History start at the task-list long-poll window while Go allows that start to complete. | **Probable real behavior gap; not independently reproduced here** | Compare Go `go/service/matching/tasklist/task_list_manager.go` long-poll/start path and Rust dispatch path; run the Go control, fix only if confirmed, unignore/add regression test, then configstore mixed lane. This is directly relevant to the recorded configstore timeout but causality remains unproven. |
| 3 | Poller manager parity tests are now active (no `ignore` marker): `poller_history_callback_fires_on_start_and_end_with_identity` and `poller_cancel_reports_whether_the_poll_is_known` in `matching_partial_upgrade_b.rs:299-366`. | **Closed at source/test level; CI unverified at this head** | Do not carry old “poller API differences” forward without a fresh CI failure. |
| 4 | Ignored tests remain for config-key completeness (`matching_partial_upgrade_a.rs:80`), non-durable ack/task-id description (`matching_rpc_tests.rs:1015`), membership shutdown (`:1496`), and various injected engine/manager failures (`matching_rpc_tests.rs:4893+`, `matching_engine_go_tests_b.rs:495`, `_c.rs:156+`). | **Unverified; mixed real gaps and test-seam-only** | Establish runtime reachability and observable RPC behavior before calling production bugs. Config key table should compare each Go `config/config.go` setting to module reads. Error injection tests are only parity blockers if error mapping cannot already be exercised at the public boundary. Membership shutdown path needs a lifecycle/RPC behavior test. |
| 5 | `matching_semaphore_new_manager_validates_params` and `matching_semaphore_scan_threads_page_tokens` appeared in older ignored inventory. | **Stale assertion unless current test still ignored** | Production scan token loop is present at `matching_semaphore.rs:247-265`. Inspect exact attributes/test results; no new implementation task justified by old note alone. |
| 6 | `matching_rpc_non_durable_describe_reports_task_id_block_and_empty` and semaphore reject-new-bucket-while-shutting-down tests remain marked parity gaps. | **Unverified potential runtime differences** | Compare Go `DescribeTaskList` non-durable status/ack manager and membership shutdown ordering to current `grpc_matching.rs`/registry behavior. Need a caller-visible proof; do not infer from absence of Go internal object alone. |
| 7 | Remaining intentional-architecture rows (no Go generic `handleErr`, no Go Handler object, no executor object) and inaccessible private callback/failure injection tests. | **Test seam / architecture differences** | Existing historical audit `matching-historical-differences-audit-20261025.md` correctly rejects object-shape differences as API breaks absent a wire/runtime mismatch. Keep them as coverage limits, not production blockers. |
| 8 | Configstore, Cassandra rate, PostgreSQL metadata/GC, shard-distributor and active-offer lanes have evidence bound to previous SHAs or harness failures. | **Unverified mixed-cluster evidence** | Consult `memory/matching-final-lanes-20261025.md`: current-pin rerun and SHA evidence are outstanding; Cassandra owner agreement remains unconfirmed. No source-only audit can close these. |

## Ignored test census

Current `rg -n 'parity gap' rust/crates/cadence-matching/tests` finds 15 marker occurrences (count includes marker text inside a test body). The prior 41-test inventory is stale and must not be repeated. The long-poll case above is the clearest candidate runtime behavior gap. The other live ignored rows need classification by production reachability, RPC observability, and test-seam constraints; the existence of `#[ignore]` alone does not prove a behavioral difference. Several old rows named in the 2025 gap list are now unignored or have production implementations (semaphore paging/deadline and poller cancellation/history callback).

## Recommended next implementation/evidence tasks

1. Resolve long-poll expiry with paired Go/Rust control and a focused regression; record whether the configstore failure is causally related.
2. Audit current Go config keys against Rust runtime reads; produce a key-by-key table rather than retaining the old broad ignored test as a production gap.
3. Prove or dismiss non-durable Describe task-id/ack status and semaphore bucket rejection during shutdown using externally observable tests.
4. Complete the final-SHA serial mixed lanes and clean-target CI with binary hashes; separately resolve Go baseline/harness failures and Cassandra owner agreement.

This reconciliation makes no completion claim. Matching remains incomplete until source gaps and required mixed Go/Rust failure, restart, and recovery evidence are closed on a pinned final candidate.
