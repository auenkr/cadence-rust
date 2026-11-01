# Frontend progress synchronized to master — 2026-11-01

User requested syncing current progress with master. Merged frontend-integration-20261101 through 71c3c273334836a9634cc2c509f4c2cdcf22671c into master without conflicts. This preserves the integrated workflow, domain/admin/schedule work, synchronous gRPC start hook, caller headers, CI fixes, mixed-gate process-survival assertions and evidence notes. Frontend remains incomplete; merging progress is not acceptance evidence.

Latest observations before merge:

- cadence-frontend baseline CI 41327559111 succeeded at b967e1442cfc431c1d8253df17044755febeb685.
- Combined cadence-matching CI https://github.com/auenkr/cadence-rust/actions/runs/41328113275 remains in progress at 71c3c273334836a9634cc2c509f4c2cdcf22671c.
- Mixed gate https://github.com/auenkr/cadence-rust/actions/runs/41327590981 remains in progress at b967e1442cfc431c1d8253df17044755febeb685; Rust/Go builds passed and runtime gate began. No mixed compatibility conclusion yet.
- Two Luna medium/default workers own replication wire fixtures/validation and Frontend Ringpop lifecycle in separate prepared worktrees. Their unfinished changes are not part of this synchronization.
- Source audit confirms the HTTP router captures state before workflow hook installation (launcher.rs router construction versus later grpc_state/tchannel_state installation); handlers.rs HTTP start/signal-with-start use execution_store directly. Shared production operation routing across HTTP remains a required follow-up; moving hook installation alone will not fix handlers that never consult it.

No local Cargo or Go validation ran. Existing untracked memory/progress-dashboard.html was preserved untouched. Master push automatically requests rust-ci; final acceptance still requires fresh clippy followed by workspace tests and mixed failure/restart/recovery evidence with binary SHA256. Retain Go History/Worker and Rust Matching.
