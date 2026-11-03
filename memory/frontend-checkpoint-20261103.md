# Frontend checkpoint 2026-11-03 (master b92a0bc2b)

Master full rust-ci: https://github.com/auenkr/cadence-rust/actions/runs/41557574536 (success).

Merged this session: async publisher + Kafka (MySQL lane), Cassandra lane + visibility store, Admin dynamic config,
multi-History churn gate, archival readers, archival gate (ListArchivedWorkflowExecutions, filestore only, MySQL lane),
Frontend metrics sink. Evidence: see the per-topic notes in memory/ and memory/evidence/.

NOT complete (Frontend must not be declared done):
- Cluster redirection (policy exists, not installed; needs resolver, active-cluster manager, remote Frontend client)
- Config-driven access control / authorizer (NoOp default)
- Metrics name/tag comparison against Go (no mixed run)
- AddSearchAttribute ES mapping; replication/DLQ and cross-cluster ops
- Async: restart-during-traffic, TLS/SASL, duplicate semantics, Kafka on Postgres/Cassandra lanes
- Archival: GetWorkflowExecutionHistory archived-only gate, s3/gcloud, Postgres/Cassandra lanes
- Cassandra closed-visibility ordering config, row-decode unit tests; retry-loop unit test; Ringpop churn beyond History-2 kill
