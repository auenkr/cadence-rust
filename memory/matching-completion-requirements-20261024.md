# Matching completion requirements (current acceptance checklist)

This is the completion contract, not a sign-off. Matching remains incomplete.
The original scope is every Go Matching observable behavior and its required
infrastructure; the other services remain Go in compatibility gates.

## Required evidence

| Requirement | Primary source / evidence needed | Current disposition |
| --- | --- | --- |
| All 13 internal RPCs | `go/proto/internal/uber/cadence/matching/v1/service.proto`: decision/activity poll and add, query and query completion, cancellation, describe, partition listing, domain task-list listing, partition update/refresh, semaphore acquire. Compare successful values, validation, typed errors, deadlines, cancellation, forwarding and version handling. | Implementations exist; final audit must map each to current tests and mixed traffic. Green workspace checks alone are insufficient. |
| Matching task lifecycle | Go matcher, forwarder, tasklist/ack manager, reader/writer/scaler, domain failover, sticky lists, isolation, rates, ownership changes. Include synchronous and persisted delivery and no duplicate completion. | Extensive ports exist; stale ignored seam tests must be classified by actual replacement behavior, not counted blindly. |
| Persistence | Go schemas and blobs on MySQL, PostgreSQL and Cassandra; leases, conditional writes, pagination, GC, restart and crash recovery. | Cassandra passed source 2432047f8 in run 40079714508; MySQL two-node passed older source 95ba26b852ee in run 40071731050. These are historical. PostgreSQL run 40080485161 failed pre-crash persistence; diagnostic rerun 40083288791 is pending. |
| Transport/API compatibility | Go SDK and Go Frontend/History/Worker communicate with only Rust Matching, through supported gRPC and Thrift/TChannel paths. Preserve gRPC/HTTP TLS modes supported by Go; Go does not implement TChannel TLS. | Production workflow has `thrift` lane; final source-bound run required after integration. |
| Configuration | File/configstore/OpenFeature typed defaults, precedence, filters, reload/live reads, invalid config behavior; client and persistence fault injection are separate canonical Go keys. | OpenFeature live reads and Unleash evaluation are active implementation tasks. Polling is merged, not provider parity by itself. Configstore production lane must pass on final candidate. |
| Discovery and ownership | Ringpop lookup/ordering and membership changes, shard-distributor registration/heartbeat/rebalance, task-list partitions and remote forwarding. | Root startup notification code exists around matching_registry.rs:1526; ignored test comment claiming absence is stale. Production `shard-distributor` and `mysql-probes` lanes need current-source evidence. |
| Lifecycle | Idle unload, SIGTERM drain, SIGKILL/restart, task recovery, domain metadata/GC, resource cleanup and shutdown deadlines. | Historical gates cover many scenarios; PostgreSQL crash checkpoint currently blocks acceptance. |
| Metrics | Go names, types, tags, rollups and emission conditions, migration policy defaults/overrides and allowlist validation. | Luna metrics worker active. Need actual comparison evidence, not only a copied allowlist. |
| Clean final validation | Fresh uncached GitHub Actions target, strict workspace clippy then workspace tests; exact commit and exit files. Gates record binary SHA256, source, commands and conclusions. | Package run 40083216641 pending for 49fa0bf81. Full final run required after all behavior changes. No local Cargo allowed. |

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
