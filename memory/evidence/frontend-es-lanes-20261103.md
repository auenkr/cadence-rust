# Frontend ES lanes: Cassandra + OpenSearch + ES restart resilience (2026-11-03)
Branch t3/frontend-es-lanes, gate head b1293dd319a549bbfe4c16d2d468169b19be7ce6, rust_ref=master (387b007b5; no Rust code changed).
Inputs: kafka=true, es=true. Topology: Rust Frontend + Rust Matching, Go History/Worker (+Go Kafka indexer).

| Lane | Run | Result | GATE ok / failed | Rust binary SHA256 |
|---|---|---|---|---|
| Cassandra + Elasticsearch 7.17.10 | https://github.com/auenkr/cadence-rust/actions/runs/41649747172 | success | 191 / 0 | fccfc81434d5bb389c421e586a246a478b2c126ae87c68800ae916ea1a5af588 |
| Postgres + OpenSearch 2.13.0 (Go version "os2", os2 index template) | https://github.com/auenkr/cadence-rust/actions/runs/41649752766 | success | 191 / 0 | 8a7e765ef2569e375edb2bc6ebb272f6f3660eba8f911b4b5131602ac59b9293 |

Changes (gate only): lib-es.sh flavor (CADENCE_GATE_ES_FLAVOR / workflow input es_flavor), ES wiring in postgres + cassandra lane scripts,
removed the "SQL only" guard in run-frontend-gate.py, new es-restart-* steps (list/count/scan over tchannel + list over gRPC:
ok before, `docker stop` ES -> all error, `docker start` -> all recover within retries, data retained, services alive).
Note: master's postgres lane script did not source lib-es.sh before; now it does.

Not done: GCS fake-gcs-server archival gate (optional; archival-providers branch not merged); no Go Frontend in this topology, so
"errors while down, recovery after" was asserted from the Go contract, not diffed against a Go Frontend run; Cassandra+OpenSearch and MySQL+OpenSearch combos not run; rust-ci not run (no Rust changes).
