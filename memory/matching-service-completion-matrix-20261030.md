# Matching service completion: requirement-to-evidence map (2026-10-30)

Review pin: `90e02d4ddb0dd9d45b7dcbef56ea412651c785f9`, coherent source based
on `a47e4032d` / `565c8fc`. This is a requirements/evidence index, not a
parity pass. Matching remains incomplete. No mixed lane is currently credited
as passed. Workspace run
[41103249766](https://github.com/auenkr/cadence-rust/actions/runs/41103249766)
at `a47e4032d` was last recorded in progress; its outcome is unknown here and
was not inspected for this review. This review ran no CI or local validation.

## Explicit completion requirements

| AGENTS.md requirement | Existing lane/evidence that addresses it | Current uncovered proof |
|---|---|---|
| Go Matching behavior and its required workflow interactions | SDK compatibility assertions are carried by the ten mixed lanes listed in `matching-acceptance-evidence-readiness-20261027.md`; lane-specific coverage includes query/standby, backlog forwarding, active offer from Go History, partition listing, adaptive scaling, and durable task handling. | No same-pin mixed result is accepted. Run the SDK scenarios against Rust Matching with Go Frontend, History, and Worker, and review their actual assertions/results. Finish the Go-to-Rust Matching parity audit on that candidate. |
| Persistence and recovery | PostgreSQL, MySQL, and Cassandra lanes exercise the Go-compatible stores; their lane checklists require persisted tasks/ownership and restart or takeover observations. Active-offer checks Go History delivery with persistence bypass assertions. | No current-pin result establishes backend interoperability, persisted state recovery, or the full lane-specific persistence assertions. A prior PostgreSQL attempt on the older frozen candidate failed before SDK scenarios: Go Frontend could not discover/serve Matching; Rust shutdown and DB cleanup were also unverified (`matching-native-resume-state-20261029.md`). Do not count it as a service pass. |
| Transports | Production Thrift lane selects Thrift Matching with TChannel History; TLS lane verifies the live Rust gRPC listener and certificate; ordinary production lanes exercise gRPC. | No accepted mixed result on the coherent pin establishes these transport paths. For TLS, handshake evidence must identify the actual Rust listener and accompany SDK/restart/recovery evidence. |
| Configuration | Configstore lane selects and seeds configstore mode; production lanes also exercise auto-config hints and runtime configuration used by their scenario. | No current-pin mixed outcome proves configured assembly and SDK behavior. Config parsing/unit evidence alone is not the configured service path. |
| Discovery and routing | Real shard-distributor lane runs the Go shard-distributor against etcd and requires assignments, routing, heartbeat expiry, and cleanup; two-node MySQL/Cassandra and adaptive lanes exercise membership/ownership and takeover. | No current-pin result establishes external discovery, multi-member routing, or owner takeover. The old PostgreSQL failure is concrete evidence of a discovery/readiness failure on that old candidate, not proof of its cause or current-source behavior. |
| Lifecycle, failure, restart, and recovery | Common gate args enable Matching restart and two-node failure recovery; scenario assertions require observed drain/restart, owner crash/takeover, durable task results, and idle recovery where applicable. TLS/configstore/Thrift/shard lanes carry these checks too. | Flags are not evidence. Each selected run must show the scenario completed, observed process/owner transitions, restored work, clean shutdown, and database/container cleanup. None is accepted for the current coherent pin. |
| Final source validation before service completion | `rust-ci.yml` supports fresh-target workspace Clippy followed by workspace tests; ten mixed workflows are mapped in `matching-acceptance-evidence-readiness-20261027.md`. | Need a successful fresh uncached workspace run and all ten serial mixed lanes on one immutable final SHA; preserve run URL, head/artifact identity, conclusion, scenario results, topology, and Rust binary SHA256. Then record parity-audit closure before starting History. |

## Required mixed lanes (same immutable final pin)

1. Production validation: PostgreSQL.
2. Production validation: MySQL probes.
3. Production validation: Thrift Matching / TChannel History.
4. Production validation: configstore.
5. Production validation: real shard distributor / etcd.
6. Two-node MySQL mixed gate.
7. Cassandra mixed gate.
8. Corrected adaptive production lane (not single-node diagnostic or Go control).
9. Active-offer mixed gate.
10. Production TLS mixed gate.

Use the established exact dispatch/artifact review instructions in
`matching-acceptance-evidence-readiness-20261027.md`; this table deliberately
does not duplicate them. For each lane, verify Go Frontend/History/Worker,
Rust Matching, no Go Matching, SDK result, actual restart/failure/recovery
outcomes, cleanup, exact head and artifact SHA, and Rust binary SHA256.

Already-reviewed stale query, refresh, and failover coverage claims are not
reopened as gaps here. Their focused source/test reviews do not substitute for
the required mixed service-completion lanes.
