# Matching completion requirements (current acceptance checklist)

> Current validation status (2026-10-25): this document retains historical
> observations below. The authoritative integrated-source tracker is
> [final candidate evidence](evidence/matching-final-candidate-20261024/README.md),
> pinned to `27d3ff5f60f0604553824420da0cf61026eda468`. Workspace strict clippy
> and tests, PostgreSQL, Thrift and Cassandra lanes passed that candidate.
> MySQL run 40153901033 is running; configstore, shard-distributor and
> active-offer candidate lanes remain pending. Matching is not signed off.
> Owner ordering including membership changes is covered by the executable
> Go/Rust oracle preserved in the candidate workspace evidence. Metrics
> comparison evidence is preserved there too, with the representative-path
> limits in `memory/matching-metrics-emission-comparison-20261024.md`.
> Startup root-to-child and production watcher lifecycle tests are integrated;
> older statements that they are unexecuted or absent are superseded.


This is the completion contract, not a sign-off. Matching remains incomplete.
The original scope is every Go Matching observable behavior and its required
infrastructure; the other services remain Go in compatibility gates.

## Required evidence

| Requirement | Primary source / evidence needed | Current disposition |
| --- | --- | --- |
| All 13 internal RPCs | `go/proto/internal/uber/cadence/matching/v1/service.proto`: decision/activity poll and add, query and query completion, cancellation, describe, partition listing, domain task-list listing, partition update/refresh, semaphore acquire. Compare successful values, validation, typed errors, deadlines, cancellation, forwarding and version handling. | Implementations exist; final audit must map each to current tests and mixed traffic. Green workspace checks alone are insufficient. |
| Matching task lifecycle | Go matcher, forwarder, tasklist/ack manager, reader/writer/scaler, domain failover, sticky lists, isolation, rates, ownership changes. Include synchronous and persisted delivery and no duplicate completion. | Extensive ports exist; stale ignored seam tests must be classified by actual replacement behavior, not counted blindly. |
| Persistence | Go schemas and blobs on MySQL, PostgreSQL and Cassandra; leases, conditional writes, pagination, GC, restart and crash recovery. | Candidate27d3ff5f: PostgreSQL40108645569 and Cassandra40153493941 passed, with source, binary hashes and recovery outputs preserved in final-candidate evidence. MySQL extended40153901033 remains running. SQLite behavior is exercised in fresh workspace40108643448. |
| Transport/API compatibility | Go SDK and Go Frontend/History/Worker communicate with only Rust Matching, through supported gRPC and Thrift/TChannel paths. Preserve gRPC/HTTP TLS modes supported by Go; Go does not implement TChannel TLS. | Candidate PostgreSQL gRPC40108645569 and Thrift/TChannel40153127238 passed with Go Frontend/History/Worker and Rust Matching. Source, binary hashes and probe outputs preserved. gRPC/HTTP TLS are separately covered; Go TChannel is plain. |
| Configuration | File/configstore/OpenFeature typed defaults, precedence, filters, reload/live reads, invalid config behavior; client and persistence fault injection are separate canonical Go keys. | Typed live reads, Unleash evaluation/repository/lifecycle/backoff and production file watcher reload/error-retention/shutdown are integrated and covered by candidate workspace40108643448. Candidate configstore mixed lane remains pending; older40086531679 is historical scenario evidence. |
| Discovery and ownership | Ringpop lookup/ordering and membership changes, shard-distributor registration/heartbeat/rebalance, task-list partitions and remote forwarding. | Candidate workspace40108643448 passed executable Go handler/resolver vs Rust owner/order fixtures for baseline, add/remove, identity/address replacement and empty rings. Root startup-to-remote-child notification passed. Candidate real shard-distributor lane remains pending. |
| Lifecycle | Idle unload, SIGTERM drain, SIGKILL/restart, task recovery, domain metadata/GC, resource cleanup and shutdown deadlines. | Candidate PostgreSQL, Cassandra and Thrift evidence includes restart/crash/recovery checks. Final MySQL extended recovery lane remains running; candidate configstore/shard-distributor and active-offer lanes remain required. |
| Metrics | Go names, types, tags, rollups and emission conditions, migration policy defaults/overrides and allowlist validation. | Candidate workspace40108643448 passed actual Go/Rust metric series comparisons for default/both/name-on/name-off policies and metered GetTasks empty/row/BadRequest cases; original JSON preserved under final-candidate metrics. This is representative-path evidence, not every emitter/value/error capture; see matching-metrics-emission-comparison-20261024.md. |
| Clean final validation | Fresh uncached GitHub Actions target, strict workspace clippy then workspace tests; exact commit and exit files. Gates record binary SHA256, source, commands and conclusions. | Candidate27d3ff5f fresh uncached workspace40108643448 passed strict clippy then workspace tests. Exit files and source preserved. Focused formatting passed; advisory workspace format check returned1 for existing unrelated debt. Final mixed lanes still in progress. |

## Sign-off discipline

For every row, record actual source SHA, run URL, command/scenario and result in
memory. If a gate tests an older source, explain its limits rather than inheriting
its green status across implementation changes. A documentation-only descendant
can reference the unchanged tested implementation with that relationship explicit.
Review failed and ignored tests against primary Go behavior. Internal architecture
may differ, but observable behavior must remain the same; do not add unnecessary
mock-only APIs or label a real branch unneeded just because it lacks a test seam.

After integration, repeat the mixed lanes on one candidate, inspect artifacts,
reconcile the final parity audit and only then consider Matching complete.

## Resume checkpoint (2026-10-24)

Master resumed at e499ac04d. AGENTS.md already contains the user requirement that
all Cargo commands, including formatting, run only on GitHub Actions.
Luna medium/default workers live-reader-resume-v2, unleash-lifecycle-v3 and
matching-metrics-emission-v3 remain active; no duplicate workers were started.

Configstore run https://github.com/auenkr/cadence-rust/actions/runs/40086025472
(source 1fc9ee1fd) failed after successful membership convergence: the configstore
empty-poll probe directly targeted the primary although Ringpop assigned its
random task list to the secondary. The probe now resolves the partition owner
and polls that host, preserving the expiry and empty-response assertions and
recording the owner. `node --check tests/matching-service-compat.cjs` passed.
This is a fixture correction; a successful rerun is still required.

## Integrated runtime reader checkpoint

Merged `t3/matching-runtime-provider-reads` as `9d8f10768`. Worker source
`bb08503dc3eded7704410736ba70b86825c5248c` passed package run40086417167 and
fresh workspace run40086744572. Reconciled the earlier live-reader branch; only
the explicit-type, owned `PropertyValues` design without Deref was integrated.
Combined-source fresh CI: https://github.com/auenkr/cadence-rust/actions/runs/40087325751
(pending when recorded).

Configstore run40086531679 passed sourcee0aeae9e8, with seeded expiry2000ms
and measured2008ms; compact evidence is now preserved under
`memory/evidence/matching-configstore-acceptance-20261024/`.
Shard-distributor run40087044393 tests source14953932f and remains active.
These mixed runs predate runtime-reader integration and must not establish final
integrated-source acceptance. Unleash lifecycle and actual metrics comparison
workers remain active. No local Cargo commands were run.

Rechecked the ignored root-start notification test: its claim that production
lacks a start-time sender is stale. `matching_registry.rs` starts bounded
`notify_read_partitions` at root startup (currently around line1526). The ignored
test itself is still an unimplemented placeholder. Existing mixed partition
probes do not by themselves prove this specific startup push; retain the need
for direct startup-notification evidence rather than treating implementation
inspection as behavioral validation.

## Lifecycle integration and review checkpoint

Merged Unleash lifecycle as ae442535c. Integrated fresh workspace run40089398117
and MySQL extended probe run40089419226 passed that source. MySQL evidence is
preserved under memory/evidence/matching-mysql-probes-acceptance-20261024/;
it includes query standby forwarding, partition owner agreement, three-partition
durable restart/drain and default AutoConfigHint observations.

Do not merge t3/unleash-repository-backoff code4f02ea1ab as final parity despite
its passing CI: review found FetchBackoff.delay never consumes skips. Go consumes
skips on ticker events; Rust would retain an earlier skip delay across later304,
network and decode failures. New worker unleash-fetch-review-v5 owns correction
and actual HTTP-sequence verification, plus reentrant account lookup and bounded
hostname handling. Existing getpwuid is not thread-safe and its fallback identity
shape differs from Go. Metrics and completion-audit workers remain active.
