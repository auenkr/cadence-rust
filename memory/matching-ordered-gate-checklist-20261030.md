# Matching final gate checklist — 2026-10-30

Documentation-only dispatch and review checklist. Workflow source reviewed at
`db06adaa3ce183c59b85fc6b3c6029bc900b10f3`; requirements/evidence map reviewed
at `0c27d5452`. Matching is still incomplete; this checklist grants no lane or
service pass. The supplied workflow revision `exactdb06adaa3` resolves here as
`db06adaa3` (full SHA above).

## Freeze and run order

1. Select and record one full immutable `FINAL_SHA` and a ref resolving to it.
   Do not move the ref or credit evidence from another SHA. Existing Actions run
   `41104458299` is reported ongoing; its outcome is unknown and is not counted.
2. Dispatch `.github/workflows/rust-ci.yml` at that ref with empty `package`
   and `clean_target=true`. This selects `$RUNNER_TEMP/cargo-target-final` and
   skips the Cargo cache. Require the successful fresh-target workspace Clippy
   step followed by workspace tests (`clippy.exit=0`, then `test.exit=0`);
   capture the run URL, `headSha`, conclusion, target path, both logs/exits,
   and reported formatting results. Its format-check step is advisory in this
   workflow; do not treat it as the Clippy/tests gate. Do not overlap this run
   with a mixed gate.
3. Dispatch and fully review the ten mixed lanes serially, in this order:

   | # | Workflow / input | Required lane evidence |
   |---|---|---|
   | 1 | `matching-production-validation.yml`, `lane=postgres` | `matching-production-postgres`; Go-compatible PostgreSQL persistence, SDK and listed PostgreSQL/query/standby/partition/restart/auto-config assertions. |
   | 2 | `matching-production-validation.yml`, `lane=mysql-probes` | `matching-production-mysql-probes`; Go schema, SDK, backlog forwarding, query/standby, partitions, durable restart and auto-config assertions. |
   | 3 | `matching-production-validation.yml`, `lane=thrift` | `matching-production-thrift`; Matching Thrift with TChannel History, SDK, restart, two-node recovery, idle recovery and auto-config assertions. |
   | 4 | `matching-production-validation.yml`, `lane=configstore` | `matching-production-configstore`; selected and seeded configstore mode, SDK, restart, idle and two-node recovery assertions. |
   | 5 | `matching-production-validation.yml`, `lane=shard-distributor` | `matching-production-shard-distributor`; real Go shard-distributor with runner-owned single-node etcd, assignment/routing, heartbeat expiry, cleanup, SDK, restart, idle and two-node recovery. This is not the in-memory handler fixture or multi-node etcd proof. |
   | 6 | `matching-mixed-gate.yml`, defaults `--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3` | `matching-mixed-gate`; two-node MySQL, SDK, observed drain/restart, owner kill/takeover, durable task/ownership assertions. Explicit args replace defaults; preserve these flags. |
   | 7 | `matching-cassandra-mixed-gate.yml`, defaults `--matching-restart --matching-idle-check-seconds 3` | `matching-cassandra-mixed-gate`; shared Go Cassandra schema, SDK, restart/drain, two-node owner kill/takeover and persisted task/semaphore owner/token assertions. Cassandra gate enables failure recovery itself. |
   | 8 | `matching-production-validation.yml`, `lane=adaptive` | `matching-production-adaptive`; full adaptive SDK/load/routing/scaler scenario plus restart, two-node recovery and idle recovery. Do not substitute `adaptive-single-diagnostic`, `adaptive-go-control`, or retry-only. |
   | 9 | `matching-active-offer-gate.yml` | `matching-active-offer-gate`; delivery from Go History, scheduled task/token, zero persistence rows/inserts as asserted, SDK, restart/drain and two-node recovery. |
   | 10 | `matching-production-tls-mixed.yml` | `matching-production-tls-mixed`; production CA-backed hostname-verified TLS on the live Rust gRPC listener, certificate/ALPN evidence, SDK, restart/drain and two-node recovery. |

   Use the exact lane dispatch commands and default/explicit arguments in
   `matching-acceptance-evidence-readiness-20261027.md`. The separate
   dependency-oracles workflow is supplemental, not one of these lanes and
   cannot replace a mixed lane.

## Review each run before the next

- Require the Actions URL, `headSha == FINAL_SHA`, uploaded `out/commit.txt ==
  FINAL_SHA`, conclusion success, and exact artifact/run identity. Retain the
  artifact promptly (three-day retention).
- Require the Rust Matching `out/binary.sha256` and successful build exit.
  Also inspect `out/gate.exit`, nested harness `out/gate/run/run.exit`, and
  `out/gate/run/results.json` result/scenario exits where emitted; inspect
  probe and service logs. All applicable exits and actual scenario outcomes
  must succeed. A green outer workflow or requested flag alone is insufficient.
- Verify topology in run evidence: Go Frontend, History and Worker; Rust
  Matching; no Go Matching. Review the lane's actual backend, transport,
  configuration, discovery/routing and persistence assertions from the table,
  plus SDK results.
- Require observed failure/restart/recovery and restored/durable work as
  applicable, not just enabled flags. Check the lane's cleanup evidence,
  including database/container and helper/distributor teardown. Missing,
  failed or unidentifiable evidence leaves that lane pending/failed.
- For TLS, review the retained sanitized handshake evidence linking PID and
  endpoint/readiness to the Rust listener, verified hostname, ALPN h2, CA and
  peer fingerprints, and peer subject/issuer/SAN. Do not retain private keys
  or passwords.

Record each run URL, immutable SHA, conclusion, artifact path, binary SHA256,
topology/backend/transport proof, SDK/scenario and recovery outcomes, all
relevant outer/nested exits, and cleanup result. Preserve evidence under
`memory/evidence/`. Only after fresh workspace CI and all ten same-SHA lanes
pass and the separate Matching parity audit is closed may Matching be marked
complete. Record reproducible evidence before starting History. This checklist
adds no behavior or parity requirement beyond `AGENTS.md` and the cited map.
