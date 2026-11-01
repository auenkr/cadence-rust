# Frontend progress synchronized to master — 2026-11-01

User requested syncing current progress with master. Merged frontend-integration-20261101 through 71c3c273334836a9634cc2c509f4c2cdcf22671c into master without conflicts. This preserves the integrated workflow, domain/admin/schedule work, synchronous gRPC start hook, caller headers, CI fixes, mixed-gate process-survival assertions and evidence notes. Frontend remains incomplete; merging progress is not acceptance evidence.

Latest observations before merge:

- cadence-frontend baseline CI 41327559111 succeeded at b967e1442cfc431c1d8253df17044755febeb685.
- Combined cadence-matching CI https://github.com/auenkr/cadence-rust/actions/runs/41328113275 remains in progress at 71c3c273334836a9634cc2c509f4c2cdcf22671c.
- Mixed gate https://github.com/auenkr/cadence-rust/actions/runs/41327590981 remains in progress at b967e1442cfc431c1d8253df17044755febeb685; Rust/Go builds passed and runtime gate began. No mixed compatibility conclusion yet.
- Two Luna medium/default workers own replication wire fixtures/validation and Frontend Ringpop lifecycle in separate prepared worktrees. Their unfinished changes are not part of this synchronization.
- Source audit confirms the HTTP router captures state before workflow hook installation (launcher.rs router construction versus later grpc_state/tchannel_state installation); handlers.rs HTTP start/signal-with-start use execution_store directly. Shared production operation routing across HTTP remains a required follow-up; moving hook installation alone will not fix handlers that never consult it.

No local Cargo or Go validation ran. Existing untracked memory/progress-dashboard.html was preserved untouched. Master push automatically requests rust-ci; final acceptance still requires fresh clippy followed by workspace tests and mixed failure/restart/recovery evidence with binary SHA256. Retain Go History/Worker and Rust Matching.

Explicitly dispatched fresh uncached workspace validation (`clean_target=true`, no package restriction): https://github.com/auenkr/cadence-rust/actions/runs/41328313463 at master head 0c1d90d733d4fc8b125013603c06b65b044fd428. Result pending. Deleted merged frontend-integration-20261101 locally and remotely after verifying it was merged into origin/master and no worktree used it.

## CI compile fix

Combined cadence-matching run https://github.com/auenkr/cadence-rust/actions/runs/41328113275 FAILED at 71c3c273334836a9634cc2c509f4c2cdcf22671c. Three E0599 errors came from the new workflow_hook_tests invoking generated gRPC methods without the generated WorkflowApi trait in scope. Fixed by importing `cadence_proto::api::workflow_api_server::WorkflowApi as _` (the shared workflow-hook trait already uses the WorkflowApi name). Commit 2ebb61117 contains the one-line fix; no production behavior changed and no local Cargo ran. `git diff --check` passed.

Replacement fresh workspace run: https://github.com/auenkr/cadence-rust/actions/runs/41328465892 at 2ebb61117. Pending actual conclusion. Earlier master validation is superseded by this source fix; branch concurrency cancels obsolete master runs.

Replication wire run 41327938225 also FAILED compilation (including missing FromThrift for FailoverMarkerAttributes); its code has not been integrated into master. The assigned replication wire worker owns completing models/converters and pinned fixtures.

Downloaded `format-all-patch` from 41328113275 to /tmp/frontend-ci-format-41328113275. Applied its two grpc.rs hunks after `git apply --check`; `git diff --check` passed. Commit f2b312fee contains only this CI-produced formatting (no local Cargo/rustfmt). Fresh uncached workspace validation for that exact code: https://github.com/auenkr/cadence-rust/actions/runs/41328560112. Prior master runs are superseded by branch concurrency. Mixed gate 41327590981 was verified still executing the Gate step during this continuation; do not restart or claim a terminal outcome from elapsed time.

Subsequent terminal results: workspace 41328560112 FAILED Clippy `field_reassign_with_default` in the new grpc.rs workflow-hook test helper. Fixed via AppState struct initialization in d746df256. Focused cadence-matching replacement https://github.com/auenkr/cadence-rust/actions/runs/41328932873 is pending. Run focused validation before spending another clean-workspace build on test-helper fixes; final clean workspace acceptance remains required. Mixed baseline 41327590981 also FAILED; exact source and binary SHA256 plus runtime failures are now recorded in frontend-mixed-gate-20261101.md.

## Master synchronization checkpoint — 22:45 IST

User requested synchronizing progress with master again. Fetched origin; local master and origin/master both pointed to `1a08f2da6c54a16a864990d85ac0339b2ce7025b` with a clean working tree. This includes the user's committed progress dashboard, preserved unchanged. The integrated server entrypoint, explicit Rust Matching/Go History endpoints and gRPC history hook are already on master. The replication routing research memo is also already present on master; no duplicate cherry-pick is needed.

Verified CI results:

- Workspace run https://github.com/auenkr/cadence-rust/actions/runs/41329766357 **SUCCESS**, head `2eae7a705b1faa2536fdcbd783dbb0324e99ad41`. This validates the integrated code before subsequent documentation/dashboard commits. It is not fresh clean-target or mixed-cluster acceptance.
- Replication fixtures run https://github.com/auenkr/cadence-rust/actions/runs/41330300887 **SUCCESS**, head `f6946d2db15233f9cec74fefe99d0fa8dc3fb2be` on `t3/replication-wire-fixtures`. The branch remains separate pending its worker's final converter/fixture audit and formatting-artifact review. Its earlier expanded-fixture compile failure was corrected by scoping the fixture helper.
- Ringpop replacement run https://github.com/auenkr/cadence-rust/actions/runs/41330331495 **IN PROGRESS** when checked, head `079139dc4c88ce18c19def1e7ef5e9431410ad65`. Latest pushed branch checkpoint is `c9d28e588cf666be2c7d9cdf7a4bbcc6522c6b2f`. Prior run 41329817593 failed the peer self-eviction test. Keep this unfinished implementation separate until the worker finishes and its replacement validation is reviewed.

No new implementation, local Cargo/Go validation, mixed gate, or worker was started for this synchronization. Preserve both active branches and their worktrees. Next integration must retain the complete replication models and current history response fields, then validate the combined master. Frontend remains incomplete; the corrected mixed gate still needs the Ringpop lifecycle integration and binary-SHA evidence.

## Replication integration and lifecycle review continuation

Replication worker completed and pushed `f7949b059`. Verified CI https://github.com/auenkr/cadence-rust/actions/runs/41330541065 **SUCCESS** at code head `1bc05691ab778e96e289a39e173104f29618b762` (`cadence-thrift`). Merged `t3/replication-wire-fixtures` into master without conflicts. Static downstream audit found the existing `service-core/src/admin.rs` domain replication response initializer needed the new `sync_shard_status: None` field; fixed and pushed with integrated master `69c71fc50`. Combined workspace CI is required; the package run does not cover service-core. Existing history response changes were retained.

Review of the still-unmerged Ringpop implementation found detached listener ownership: its spawned TChannel listener's JoinHandle is awaited inside a JoinSet wrapper, so cancelling the wrapper drops/detaches the inner listener instead of closing it. Sent this concrete issue to the ongoing Ringpop worker, requesting explicit abort/join ownership on shutdown/error paths and a real port-rebind test. Do not integrate or run the corrected mixed gate until this is resolved. Membership-only self-eviction tests do not prove listener teardown.

The finished replication worker slot was replaced by one Luna medium/default worker for the bounded gRPC async hook routing task documented in frontend-transport-followups-20261101.md. Task suffix `fd99446e-44ff-4326-ba6f-111bc0bc2dbe`; owns grpc.rs and its task memory only. Ringpop worker remains owner of launcher/membership. Two workers total; no local Cargo/Go validation or mixed gate was started.

Ringpop follow-up `c0e558cd617f47f55c844a0538bf19dcd41bb2d7` retains the real listener handle separately from its completion receiver and explicitly aborts/joins it at teardown. It adds a real bound-port rebind regression test. CI https://github.com/auenkr/cadence-rust/actions/runs/41331009654 failed Clippy on the regression test's unused `mut listener_task` at launcher.rs:2544; tests did not run. The ongoing Ringpop worker owns its correction and validation. Do not substitute the earlier successful run for this new source. Master run 41330882487 was rechecked live in workspace Test after Clippy success; no duplicate run was dispatched.

Combined master run https://github.com/auenkr/cadence-rust/actions/runs/41330882487 is now **SUCCESS**, exact code head `69c71fc50711c28c5b6b1000af66ebe8c402b1e0`. This establishes workspace Clippy/tests for the merged replication model/fixture slice and service-core initializer fix, not clean-target final acceptance or mixed parity. Later master changes through `31b079e06` are the mixed-gate startup-order correction and documentation. Ringpop's lint/format correction has replacement CI https://github.com/auenkr/cadence-rust/actions/runs/41331347247, head `c6c011c3b4fc8573b3419032760944ff9b23d861`, verified in progress. Both workers remain assigned; the mixed gate awaits Ringpop's completed fix.
