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
