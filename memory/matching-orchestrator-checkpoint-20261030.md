# Matching orchestrator checkpoint — 2026-10-30

Matching remains incomplete. Use one Codex gpt-6-luna worker, normal service tier; high effort for hard tasks, medium/low for simple tasks. No local Cargo/Go validation.

## Current source and validation

Request-trace candidate: `a534baae8c921e0ec8109527c6bc3184937b0323`, branch `t3/diagnose-matching-gate-failure`.
Fresh package-unset workspace CI https://github.com/auenkr/cadence-rust/actions/runs/41040870450 is terminal success. The current worker must verify artifact identity, clean target and Clippy/test exit files before PostgreSQL dispatch.

Previous verified workspace source `3a958668c040b289b495f9b76b2209969db7e2a0` passed run 41037584598: 1829 passed, 28 ignored, Clippy and tests exit 0. Its mixed PostgreSQL run https://github.com/auenkr/cadence-rust/actions/runs/41039044942 failed before SDK/restart/recovery assertions. Earlier mixed run 41034380907 also failed readiness. Neither proves service acceptance.

## Corrected diagnosis

Matching startup completed in retained 41039044942 artifacts. First real readiness RPC is MatchingAPI/ListTaskListPartitions, not gRPC health. Direct RPC and Frontend readiness timed out; precise blocked request stage remains unknown. Ringpop PING logging requires Ready; later ring removals align with teardown. An opening-admission log alone does not prove startup stalled. Both admission gates retain updates without subscribers. Rust output goes to regular files; ordinary pipe saturation is ruled out. Go and Rust cold domain-cache lookups both use cache-owned three-second persistence timeouts, independent of inbound RPC deadline; that is not a proven parity defect.

Current source enables opt-in capped request-stage tracing in PostgreSQL workflow via CADENCE_MATCHING_LIST_PARTITIONS_TRACE=1. Trace activation through Python/Node/both Rust launches was reviewed. Next mixed artifacts should distinguish admission, domain lookup/refresh, configuration and owner lookup. No production behavior fix is yet established for readiness failure.

## Single active dispatch owner

Delegate task `node:delegated-task:command%3Amcp%3A00759db9-9211-4f57-857f-fe99d7eccac0%3Adelegate-task%3A60f52c1a-6964-498f-acee-9c61bd6579d9` owns artifact verification and PostgreSQL dispatch. Do not dispatch a competing gate or poll the running delegate; completion resumes parent. At last Actions list inspection no new production run had appeared. Do not infer stopped work from that snapshot.

## Remaining completion work

Resolve readiness from request-stage evidence, then validate all ten native mixed lanes serially on one immutable source, recording binary SHA256 and actual nested readiness/SDK/restart/recovery exits. Reconcile final Go parity and ignored tests. Integrate validated changes and evidence before declaring Matching complete. Do not migrate History yet.

Separate supplemental queued-activity owner-crash scenario remains unvalidated on `t3/matching-pg-observer-fix` at `f009e8d70763e0a30a0b933646f25131d180709d`. It covers durable activity row, killed owner, history counts, queue cleanup and sampled restarted-member partition participation; it is not integrated into the active diagnostic candidate. Preserve all unmerged branches/evidence. Latest bounded cleanup found no safe deletions.
