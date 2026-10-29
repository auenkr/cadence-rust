# Native Matching add source-domain audit — 2026-10-29

Matching remains incomplete. Read-only source audit at
`7c4e1ee9d5470b424709a20d9a2393274cadb87a`; no local Cargo/Go validation.
This is a specific remaining behavior gap, not a new mixed-gate requirement.

Go `service/matching/handler/engine.go::AddActivityTask` copies the original
SourceDomainUUID into TaskInfo.DomainID. `tasklist/task_list_manager.go::AddTask`
looks up that exact string with domainCache.GetDomainByID before attempting
sync match. Lookup errors are returned. A known inactive source persists only
when not forwarded; a forwarded inactive source returns remote-sync-match failure.

Rust `matching_engine_native.rs::add_task` parses source_id into Uuid, builds
`source = GoString::from(source_domain.to_string())`, and passes the canonical
value to sync matching and forwarded requests. Its DecisionAddContext callback
accepts Uuid, and `matching_engine_context_native.rs::domain_active` converts
that to canonical text before consulting `active`. `active` defaults to true
when its in-memory lookup fails. Thus activity source-domain aliases lose the
original cache lookup key; missing source metadata can be treated as active.
The request target-domain wrapper check does not check SourceDomainUUID.

Existing Go UUID-spelling oracle and native domain-cache regressions establish
that exact cache keys matter even when persistence parses equivalent UUIDs.
Existing native cross-domain tests cover distinct valid canonical IDs and
inactive-source persistence, but do not assert source-domain alias/cache errors.
No new end-to-end source-domain oracle was executed in this audit.

Required follow-up after the pending metric fixes are merged and master is
synced: retain raw source bytes for cache/activity checks, live sync-match
History requests and forwarding; resolve source metadata through the same
error-preserving cache as Go. UUID conversion is a persistence boundary, not
cache identity. Add direct handler regressions for canonical active/inactive,
missing source metadata, uppercase/compact aliases and malformed raw sources,
checking Go-visible status/error and no erroneous task dispatch. Review enqueue
and forwarding field semantics separately. Validate on CI and mixed SDK gates.

Native-only ignored-test scan found no #[ignore] in Matching native source or
native integration tests. Persistence matching_postgres_native.rs retains one
live PostgreSQL oracle ignore, now explicitly invoked by the supplemental
matching dependency-oracles workflow (pending execution). This inventory does
not excuse historical parity-test exclusions or prove native coverage of all
Go behavior.
