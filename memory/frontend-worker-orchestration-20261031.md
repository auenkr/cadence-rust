
## Model update (2026-10-31, user)
New workers use Claude Haiku 5.5 (`claude-haiku-5-5`) instead of gpt-6-luna. Running luna workers (frontend-domain-admin = integration branch, frontend-core2, worker-core) finish their current runs; do not cancel. Haiku is weaker: give narrow tasks with exact Go file pointers and CI done conditions.

## Stop checkpoint (2026-10-31, user asked to stop)
All workers cancelled; their work is pushed on branches below (NOT merged to master, branches merge-conflict in cadence-persistence/domains.rs, cadence-service-core/handlers.rs, cadence-matching/matching_task_store.rs):
- `worker-core` 34e988a92 (2026-10-31 23:15:02 +0530) docs(worker): record scheduler backfill and policy checkpoint
- `frontend-domain-admin` 622948bd9 (2026-10-31 23:16:46 +0530) fix(persistence): read through Go domain row on Frontend cache miss
- `frontend-core2` f6b628777 (2026-10-31 21:59:53 +0530) docs(frontend): record successor handoff status
- `t3/frontend-workflow-api` d7523e66c (2026-10-31 23:01:11 +0530) docs(frontend): record registration audit and cutover decision

Integration plan: `frontend-domain-admin` is the Frontend integration branch (already merged core2 + workflow-api once); merge later commits of the others into it, get package CI green, then merge to master.
Per-branch handoff files: memory/frontend-domain-admin-*.md, frontend-core2-*.md, frontend-workflow-api-*.md, worker-core-*.md (on each branch).
Not done: durable domain store/replication queue, Admin DLQ/membership, schedule wiring, typed backend for all workflow methods, wrapper chain in registered service, remaining Worker modules, mixed Go/Rust gates (Rust Frontend + Go History + Rust Matching), SDK/failure/restart evidence, parity audits. Haiku workers (claude-haiku-5-5) stop after one chunk; re-send "continue with next gap" or use luna for hard design work.

## Integration branch consolidation — 2026-11-01
`frontend-domain-admin` is now the single Frontend integration branch: merged
origin/frontend-core2, origin/t3/frontend-workflow-api and origin/master with no
textual conflicts. Fixed a latent compile error (`uuid::Uuid::now_v7` needs the
`v7` feature in cadence-service-core; it only passed in workspace builds via
feature unification) — commit "service-core: enable uuid v7 feature".
CI (rust-ci.yml, clippy + tests):
- cadence-frontend: https://github.com/auenkr/cadence-rust/actions/runs/41312002581 head a1573d540 — success
- cadence-matching (pre-fix): run 41312418696 — failure (now_v7 feature)
- cadence-matching (post-fix): https://github.com/auenkr/cadence-rust/actions/runs/41312675691 — success
Note: concurrent dispatches on one ref cancel each other (concurrency group); run sequentially.
Remaining gaps (unchanged): register handler_workflow::Handler as production
WorkflowService (gRPC+Thrift, see "Registration audit"), typed History/Matching
WorkflowBackend, ~20 Admin methods, scheduler wiring, mixed gate with SHA256.
