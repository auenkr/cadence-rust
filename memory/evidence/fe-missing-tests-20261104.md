# fe-missing-tests (2026-11-04)
- O30: cadence-persistence/src/domains.rs `failover_notification_version_tests` (UpdateDomain + FailoverDomain on sqlite-backed PersistedFrontendDomainManager, asserts failover_notification_version == metadata version).
- C10: cadence-matching/src/grpc_admin.rs `resend_replication_input` extracted + test asserting start/end event id and version.
- DLQ content assertion (item 3): skipped, not cheap.
- CI: https://github.com/auenkr/cadence-rust/actions/runs/41762775070 head 19019edaa939957be8fab5114ce4758ebe94021b conclusion success (clippy + workspace tests). Earlier runs failed on fixture issues (replication queue stub, destination cluster) fixed in tests only; no production change.
