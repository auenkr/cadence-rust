# Frontend graceful two-History gate deflake — 2026-11-14

Branch: `gate-graceful-deflake`  
Final head: `d1e90d644767284b62cde29ab05339605b7dd5d7`

## Change

The Cassandra graceful two-History gate originally used four History shards. Ringpop assigned all four to one Rust History host, so graceful failover completed but no failover marker crossed hosts (master run [43521606210](https://github.com/auenkr/cadence-rust/actions/runs/43521606210)).

In `tests/gates/run-frontend-two-cluster-gate.py`, graceful mode now uses 16 History shards in both generated Rust persistence configuration and the Go development configuration, including the Cassandra configuration derived from it. Other gate modes retain four shards.

Before starting graceful failover, the gate calls Go CLI `admin history_host describe --had <address> --pf` for both Rust History hosts and waits until each response has a nonempty `shardIDs` list. It records the result as `grace-both-hosts-own-shards` and aborts before failover if ownership is not established.

Two harness mistakes were caught and corrected by CI: the CLI command group is `history_host`, and the Go CLI JSON field is serialized as `shardIDs`. CI logs from the failed attempts showed that both hosts did in fact own shards, so the later failure was only in the gate parser.

## Final CI evidence

All runs below used head `d1e90d644767284b62cde29ab05339605b7dd5d7` and completed successfully.

| Gate | Run | Conclusion | Gate evidence |
|---|---|---|---|
| Graceful MySQL (`two_cluster=true`, `rust_history=true`, `rust_history_side=active`, `two_history_graceful=true`) | [43528860806](https://github.com/auenkr/cadence-rust/actions/runs/43528860806) | success | `grace-both-hosts-own-shards` passed; `GATE SUMMARY failed: []` |
| Graceful PostgreSQL (same inputs) | [43528860299](https://github.com/auenkr/cadence-rust/actions/runs/43528860299) | success | `grace-both-hosts-own-shards` passed; `GATE SUMMARY failed: []` |
| Graceful Cassandra (same inputs) | [43528861098](https://github.com/auenkr/cadence-rust/actions/runs/43528861098) | success | `grace-both-hosts-own-shards` passed; `GATE SUMMARY failed: []` |
| Plain two-cluster Rust History MySQL regression (`two_cluster=true`, `rust_history=true`) | [43528860637](https://github.com/auenkr/cadence-rust/actions/runs/43528860637) | success | `GATE SUMMARY failed: []` |

## Failed attempts and diagnosis

The first dispatches on `0264de26a4e3182cc3849778ab35989510215f5d` failed because the CLI path used `admin history host describe`; the correct command is `admin history_host describe`:

- [43525835905](https://github.com/auenkr/cadence-rust/actions/runs/43525835905) MySQL graceful — failed at `grace-both-hosts-own-shards` with `command not found: history`.
- [43525836075](https://github.com/auenkr/cadence-rust/actions/runs/43525836075) PostgreSQL graceful — same CLI path error.
- [43525835240](https://github.com/auenkr/cadence-rust/actions/runs/43525835240) Cassandra graceful — same CLI path error.
- [43525835404](https://github.com/auenkr/cadence-rust/actions/runs/43525835404) plain MySQL regression — success, `GATE SUMMARY failed: []`.

The next dispatches on `3c635cd7eaaf33388e738d33fe649da558bf54ec` failed because the parser read `shardIds`; Go CLI returned `shardIDs`. The uploaded step details confirmed nonempty ownership on each host (9 and 7 shards on MySQL; 6 and 10 on PostgreSQL):

- [43527353995](https://github.com/auenkr/cadence-rust/actions/runs/43527353995) MySQL graceful — parser rejected `shardIDs`.
- [43527354085](https://github.com/auenkr/cadence-rust/actions/runs/43527354085) PostgreSQL graceful — parser rejected `shardIDs`.
- [43527355031](https://github.com/auenkr/cadence-rust/actions/runs/43527355031) Cassandra graceful — parser rejected `shardIDs`.
- [43527354793](https://github.com/auenkr/cadence-rust/actions/runs/43527354793) plain MySQL regression — success, `GATE SUMMARY failed: []`.

Validation was performed on GitHub Actions. No local Cargo, Go, or Docker commands were run. A local Python syntax compilation and `git diff --check` passed for the harness edits.
