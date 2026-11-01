
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
