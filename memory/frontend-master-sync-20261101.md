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
