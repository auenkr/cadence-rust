# Frontend domain visibility (branch fe-domain-visible)

## Root cause 1 (fixed): Frontend wrote domains only to memory
`rust/crates/cadence-matching/src/launcher.rs` cloned `state` into `grpc_state`/`history_state`/`tchannel_state` and
built the legacy router `app` BEFORE the configured stores (`domain_metadata_persistence`, `history_node_store`,
`visibility_store`) and the `PersistedFrontendDomainManager` were installed on `state`. Transports therefore kept the
in-memory `InMemoryDomainStore` manager: domains were never written to Go's `domains` table / `domain_metadata`, so Go
History said `failed to get domain name from domainID`. Fix: re-clone the states and router after installation
(commit 167dad6ee). Visibility/history readers were also missing from the cloned states.
Evidence (gate 41336551192, rust_ref=fe-domain-visible head 2e21f21ab): MySQL dump shows fe-gate-t-*/fe-gate-g-* rows
(shard 54321, thriftrw, 211B) and domain_metadata.notification_version=6. sdk-start-echo and sdk-start-signal-query now PASS.
CI: rust-ci 41334740502 (cadence-matching) success.

NOTE: `frontend-mixed-gate.yml` defaults `rust_ref=fe-integ`; pass `-f rust_ref=<branch>` or the gate builds the wrong sources
(runs 41335190999 / 41335924738 were invalid for that reason).

## Next distinct blockers (gate 41336551192)
- Workflows start but decisions never complete: echo workflow TIMED_OUT, query/describe-open/list-open deadline-exceeded.
- Rust Frontend gRPC DescribeDomain read `state.domains` directly (not the persisted manager) so Go-created domains
  (cadence-system, cadence-batcher, default) returned not-found to Go workers; changed to `domain_manager` (grpc.rs).

## Final state of this chunk
- rust-ci (cadence-matching) https://github.com/auenkr/cadence-rust/actions/runs/41338414827 success at the grpc.rs fix head.
- Mixed gate https://github.com/auenkr/cadence-rust/actions/runs/41338967394 (rust_ref=fe-domain-visible): FAILED overall, but
  domain visibility is fixed: 23 GATE ok incl. sdk-describe-domain, sdk-worker-start, sdk-start-echo, sdk-start-signal-query.
- Still failing (next distinct blocker, NOT domain related): after start, no decision task completes
  (sdk-complete-echo "history not found", describe-closed TIMED_OUT) and later gRPC Frontend calls
  (query/describe-open/list-open/list-closed/signal) hit client deadline-exceeded; CLI tchannel list-open fails.
  Rust Frontend/Matching logs contain no RPC-level diagnostics, so the hang point (Frontend->Matching poll, Frontend->History,
  or blocking MySQL call on the async runtime for visibility reads) is unproven. Next step: add request-level tracing to Frontend
  gRPC handlers (poll/start/list) and dump matching task_list/tasks tables in the lane before container removal.
- sdk-list-closed therefore not yet passing; likely downstream of the same hang (no workflow ever closes normally).

## fe-hang findings (2026-11-02)
- Root cause of decisions never completing: Go SQL `history_node.txn_id` is stored multiplied by -1; Rust readers (MySQL/Postgres) did not revert it, so `read_history_branch` skipped all rows (`txn_id < last_txn_id`) -> EntityNotExists. Fix: negate on read in matching_mysql.rs / matching_postgres.rs. Gate 41371984343: complete-echo, describe-closed, signal, start-signal-query pass.
- Query hang: Frontend rate-limit wrapper (wrappers/ratelimited.rs) decoded the RespondQueryTaskCompleted token as a workflow TaskToken ("missing field workflowType", surfaced as undeclared InternalServiceError). Fix: use deserialize_query_token. workflow_api.rs fallback error now includes the message.
- list/describe-open timeouts: visibility reader used the default `cadence` DB (`Table cadence.executions_visibility doesn't exist`); fix opens a separate MySqlTaskStore for `visibilityStore` datastore (cadence_visibility) in matching_task_store.rs (MySQL only; Postgres still TODO).
- Gate 41374006882 (head after message change) exposed both; fix gate run: 41376086311.
- Still open: IPv6 `[::1]` gRPC dial from Go worker (Rust Frontend gRPC bind).
- Gate runs on fe-hang: ...41371984343 (txn fix verified), 41374006882, 41376086311.
- Further fixes: visibility-only MySQL connect for `visibilityStore` (no task-schema validation); gRPC ListOpen/ListClosed routed to the SQL visibility hook (previously served from an in-memory list); visibility time units fixed (request ns->us, response us->ns).
- **Gate 41377753898 on fe-hang (head after commit "gRPC list-open/closed via visibility hook") = SUCCESS: all SDK scenarios pass** (complete-echo, describe-closed, query, describe-open, list-open, signal, restart handshake, signal-after-restart, complete-after-restart, list-closed). Earlier: 41376712096 (list failing), 41376086311 (startup fail), 41374006882 (query token), 41371984343 (txn_id).
- TODO: remove temporary *-TRACE eprintln code; rust-ci (clippy/test) for persistence/matching/frontend; Postgres separate visibility store; IPv6 `[::1]` gRPC bind check.
