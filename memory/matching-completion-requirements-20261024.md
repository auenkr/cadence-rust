# Matching completion requirements (current acceptance checklist)

This is the completion contract, not a sign-off. Matching remains incomplete.
The original scope is every Go Matching observable behavior and its required
infrastructure; the other services remain Go in compatibility gates.

## Required evidence

| Requirement | Primary source / evidence needed | Current disposition |
| --- | --- | --- |
| All 13 internal RPCs | `go/proto/internal/uber/cadence/matching/v1/service.proto`: decision/activity poll and add, query and query completion, cancellation, describe, partition listing, domain task-list listing, partition update/refresh, semaphore acquire. Compare successful values, validation, typed errors, deadlines, cancellation, forwarding and version handling. | Implementations exist; final audit must map each to current tests and mixed traffic. Green workspace checks alone are insufficient. |
| Matching task lifecycle | Go matcher, forwarder, tasklist/ack manager, reader/writer/scaler, domain failover, sticky lists, isolation, rates, ownership changes. Include synchronous and persisted delivery and no duplicate completion. | Extensive ports exist; stale ignored seam tests must be classified by actual replacement behavior, not counted blindly. |
| Persistence | Go schemas and blobs on MySQL, PostgreSQL and Cassandra; leases, conditional writes, pagination, GC, restart and crash recovery. | Cassandra passed source 2432047f8 in run 40079714508; MySQL two-node passed older source 95ba26b852ee in run 40071731050. These are historical. PostgreSQL passed run 40084530178 at1dd23e8a5 after fixture isolation and port fixes; preserved memory/evidence/matching-postgres-acceptance-20261024. Earlier pre-crash failures remain recorded. |
| Transport/API compatibility | Go SDK and Go Frontend/History/Worker communicate with only Rust Matching, through supported gRPC and Thrift/TChannel paths. Preserve gRPC/HTTP TLS modes supported by Go; Go does not implement TChannel TLS. | Thrift/TChannel run40085137140 passed atbfa303eeb; source/binary/scenarios in memory/evidence/matching-thrift-acceptance-20261024. Final integrated-source run still required. |
| Configuration | File/configstore/OpenFeature typed defaults, precedence, filters, reload/live reads, invalid config behavior; client and persistence fault injection are separate canonical Go keys. | Unleash evaluation merged d4c52e059; live reads are active. SDK registration/metrics/conditional-poll lifecycle assigned to unleash-lifecycle-v3. Configstore lane40085504501 is pending. Full provider parity is not yet established. |
| Discovery and ownership | Ringpop lookup/ordering and membership changes, shard-distributor registration/heartbeat/rebalance, task-list partitions and remote forwarding. | Root startup notification code exists around matching_registry.rs:1526; ignored test comment claiming absence is stale. Production `shard-distributor` and `mysql-probes` lanes need current-source evidence. |
| Lifecycle | Idle unload, SIGTERM drain, SIGKILL/restart, task recovery, domain metadata/GC, resource cleanup and shutdown deadlines. | Historical gates cover many scenarios; PostgreSQL crash checkpoint currently blocks acceptance. |
| Metrics | Go names, types, tags, rollups and emission conditions, migration policy defaults/overrides and allowlist validation. | Migration policy merged ae8dc7c48, fresh workspace run40085303328 passed. Metrics follow-up worker is reconciling event name/kind/tag checks. Live emitted Go/Rust comparison remains required. |
| Clean final validation | Fresh uncached GitHub Actions target, strict workspace clippy then workspace tests; exact commit and exit files. Gates record binary SHA256, source, commands and conclusions. | Package run40083216641 passed at49fa0bf81; fresh workspace run40085303328 passed atae8dc7c48. Both predate later Unleash/live-reader work. Full final run required after all behavior changes. No local Cargo allowed. |

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
