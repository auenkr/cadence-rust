# Frontend final audit (branch frontend-final-audit)

## Baseline gate (MySQL, kafka + two_history)
- Run https://github.com/auenkr/cadence-rust/actions/runs/41583260241 — success, head 074a8db184d4c64fc21ffca2d61ece226ffa0971
- binary sha256 818485373ada62e0cf34782cecbf2d5b0e42016a8090ed0c1f4e099fb349401b
- steps: 122/122 true (memory/evidence/frontend-final-audit-41583260241-steps.json)

## Cross-cluster admin proxy (partial)
- rust-ci cadence-service-core run 41583408598 success. Payload types not yet ported.

## Metrics parity (item 4) — fixed and proven
- Bugs fixed: Prometheus series were labelled `cadence_service="cadence_matching"` on the Frontend (now the owning service, `cadence_frontend`); requests counter and latency histogram now carry `domain`/`wf-domain-name` tags like Go's metered scope.
- Go parity facts: `cadence_latency_ns` is only emitted when histogram migration allows it (`histograms.default: timer` by default), same as Go; the gate sets `histograms.default: both`. Prometheus label values are sanitised (`fe-gate-t-x` → `fe_gate_t_x`).
- Gate run https://github.com/auenkr/cadence-rust/actions/runs/41588409434 — success, head 0e16557726c77eb0a9c5affaf19f4ac517a17c84 (mysql, kafka, two_history), 131/131 steps, binary sha256 959a17b3c5fbc50e48cdf0a2fff11c43fb2db0c279e4eebc4418d910a9fa9634.
- Steps: `metrics-requests-start`, `metrics-requests-poll-decision`, `metrics-latency-start`, `metrics-latency-ns-start`; evidence: memory/evidence/frontend-final-audit-41588409434-steps.json and -metrics.prom (bucket lines trimmed).
- Async restart-during-traffic: `sdk-async-frontend-restart-no-lost-messages` passes in the same run (40 RejectDuplicate async starts, Rust Frontend killed/restarted mid-traffic).

## Item 3 — replication/cross-cluster
- ResendReplicationTasks implemented (`admin_replication.rs::resend_replication_tasks`): pages remote admin `GetWorkflowExecutionRawHistoryV2` (page 100, 30s timeout) and sends each batch with version-history items to History `ReplicateEventsV2`; NotFound maps to EntityNotExists. Unit tests: `resend_tests::*`; rust-ci cadence-service-core run 41587752247 success. Limits: remote must be a gRPC cluster (TChannel remotes return a clear error), no remote TLS, no real two-cluster gate (single-cluster gate only), Go `fixCurrentExecution` path is nil in the admin handler so not needed.
- GetDomainReplicationMessages re-checked against Go handler.go:945-1001: default -1 cursor, ack-level lookup, batch 100, UpdateAckLevel best-effort — matches (`admin.rs::domain_replication_messages_response`).
- GetCrossClusterTasks / RespondCrossClusterTasksCompleted: validated + proxied to History; non-empty task payloads (`CrossClusterTaskRequests`, start-child/cancel/signal/record-child/apply-parent-close attributes) are NOT ported and return an Internal error. Stays `partial`; Go cross-cluster queue is experimental and off by default so History returns empty maps in the gate cluster.

## Item 2 — archived-only GetWorkflowExecutionHistory: documented infeasible in a gate
Go archives history only when the retention timer fires (retention is whole days, minimum 1 day, plus deletion jitter), so a mixed-gate run cannot produce an archived-only history through Go History. Not attempted: pre-populating a filestore archive by hand would test only our own file layout. The archive fallback remains covered by Handler tests against the real filestore reader; row stays `partial`.
