# Matching failover callback integration follow-up (2026-10-27)

Parent follow-up: added `matching_failover_callback_tests.rs`, registered by
matching_runtime.rs. It calls production start_domain_cache with the actual
registry, proves catch-up leaves a pre-existing poll blocked, refreshes local,
active-active and stale failovers without release, then observes a newer global
failover release that poll without unloading its manager. It owns and shuts
down the real runtime/cache worker. This is unverified source until CI runs;
the initial worker review below is historical.

This is a source-review follow-up to the callback/catch-up row in
`matching-current-ignore-reconciliation-20261027.md`. No Cargo command, test,
Go command, formatter, or runtime gate was run for this note.

## Result

No production defect was confirmed. `MatchingRuntime::start_domain_cache`
registers the production catch-up and changed callbacks. Catch-up stores the
maximum failover notification version before invoking `changed`; `changed`
therefore does not release pollers on registration. On later notifications,
the callback advances the baseline and delegates selection to
`notified_failover_domains`, then calls the registry's
`release_domain_failover_polls` for selected UUIDs.

The requested runtime integration assertions were not added in this pass. The
existing named runtime test only covers the selector helper. Existing registry
tests independently exercise poll release, but that does not prove callback
registration and refresh are connected end-to-end. Accordingly, this gap
remains open and no integration pass is claimed.

The relevant seams are available for a follow-up: `MatchingDomainCache` accepts
a `DomainMetadataSource` and `DomainCacheClock`; `MatchingRuntime::with_durable_queues_and_domain_cache`
starts the real cache callback registration; and the runtime owns the registry
used for durable queues. A complete fixture still needs to combine a mutable
source and deterministic clock with a blocked poll registered against a valid
global domain UUID, then drive cache refreshes through registration baseline,
new global/non-active-active notification, and filtered/stale updates. Its
assertions should observe the blocked poll result and must not reimplement the
selector predicate.

## Evidence boundary

Read-only source inspection covered `matching_runtime.rs`,
`matching_domain_cache.rs`, and the existing runtime/registry tests. CI run
40533448281 was already in progress on the parent's immutable checkout; this
work did not dispatch or modify it. No local validation result, CI conclusion,
or mixed-cluster evidence is asserted here.
