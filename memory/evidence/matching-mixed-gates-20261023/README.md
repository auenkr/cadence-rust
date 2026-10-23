# Mixed gates 2026-10-23 (branch matching-mixed-gates-20261023, from 47774da67)
Binaries: frozen Rust/Go from the PG run (binaries.sha256); this task ran no cargo.

## 1. MySQL gate with cancel scenario: PASS
- mysql-gate.exit = 0 (command-mysql.txt, mysql-run.log, mysql-run/). mysql:8.0.36, Go v8 schema, no Go Matching.
- go-sdk.log: "workflow cancellation ended Canceled through Rust Matching"; echo/activity retry/child/workflow retry/decision retry/activity timeout/signal passed.
- probe.log (asserts in matching-service-compat.cjs): queryDispatch, isolationDispatch (+CancelOutstandingPoll), nativePartitionConfig, activity heartbeat/timeout (RecordActivityTaskHeartbeat), SIGTERM drain+recovery (queued 1 -> 0, pid 546176 -> 546758, 1 completion), SIGKILL restart (1 durable row, pid 545877 -> 546176, 1 completion), task GC + domain metadata: passed. matching-shutdown.json exitCode 0.
- goSchemaOnly: presentTables == Go schema + setup tables, missingGoTables [] (no Rust-private tables).
- Activity queues show durableGcEvidence=false (no activity task persisted before delivery): expected.
- Rust stderr "Matching membership cleanup failed: Not enough hosts" at shutdown (also in PG run): benign log, exit 0.

## acceptanceEligible=false is by design, not a harness gap
The cjs sets acceptanceEligible = (CADENCE_MATCHING_CASSANDRA_GATE==='1'); run-go-service-baseline.py hardcodes `not production_matching`. Acceptance = Cassandra shared-schema gate plus a service parity audit. Left unchanged (changing it would weaken checks). A MySQL/PG acceptance definition is a policy decision.

## 2. PG run2 audit (matching-pg-mixed-20261023/run2)
- Evidenced: cancel scenario, SDK samples + signal, SIGTERM drain+recovery (queued 1 -> 0, pid 497857 -> 498008, 1 completion), SIGKILL restart (1 row, 497832 -> 497857, 1 completion), final SIGTERM exit 0, probe exit 0.
- query / isolation / CancelOutstandingPoll / heartbeat+timeout: the probe prints "passed" only after cjs asserts, so pass is inferred from probe exit 0; no per-assertion log lines exist.
- NOT verified for PG: productionDatabaseEvidence = "not collected" (GC/domain-metadata SQL checks are MySQL-only); goSchemaOnly = null.

## 3. Two-node scenario: harness restored, NOT passing
- tests/run-go-service-baseline.py had lost --matching-failure-recovery / --matching-idle-check-seconds in a merge (cjs support intact). Restored. cjs now prints "PROBE BODY FAILED" so shutdown errors no longer mask the real one.
- run a (mysql-2node-a-failed.log; exit never written, harness hung, killed): probe failed after ring maps matched on both nodes, idle eviction + SDK recovery (1 completion) and typed non-owner ABORTED decoded by Go oracle. Primary Rust exited 1 at SIGTERM: "Matching shutdown failed: stopping Matching maintenance: Task list range ID was 12 when it was should have been 11". REAL RUST BUG candidate: a node that lost task-list ownership to the peer fails shutdown (exit 1); Go does not fail process exit on that. Not fixed (needs Go shutdown-path check).
- run b (mysql-2node-b.exit = 1): ECONNREFUSED to secondary gRPC at ring-convergence loop (cjs:420) though secondary logged listening. Unexplained.
- run c (exit 143, killed by me; probe hung after failing): NOT_FOUND "Decision task not found" at cjs:515 (RespondDecisionTaskCompleted for owner probe). Possibly 10s decision timeout under host load 9-13; unconfirmed.
- The kill-one-node recovery step was never reached: NOT verified.
