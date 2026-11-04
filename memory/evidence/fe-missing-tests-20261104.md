# fe-missing-tests (2026-11-04)
- O30: cadence-persistence/src/domains.rs `failover_notification_version_tests` (UpdateDomain + FailoverDomain on sqlite-backed PersistedFrontendDomainManager, asserts failover_notification_version == metadata version).
- C10: cadence-matching/src/grpc_admin.rs `resend_replication_input` extracted + test asserting start/end event id and version.
- DLQ content assertion (item 3): skipped, not cheap.
