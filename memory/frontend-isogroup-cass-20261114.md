# frontend-isogroup-cass (2026-11-14)

Failure: run 43537738073 (cassandra, two_cluster, rust_frontend_active, master 938b77005):
`admin isolation-groups update-domain` on Rust Frontend B returned
`InternalServiceError not declared for AdminService::UpdateDomainIsolationGroups`; replication to A then saw "No groups found".
The preceding domain update and the following async-config update (same manager path) passed in the same run.

Analysis: the Internal branch is Persistence error from PersistedFrontendDomainManager::update_isolation_groups
(most likely Cassandra LWT batch "domain metadata notification version changed during update", or a transient Cassandra error).
The error text was not logged, so the exact cause could not be proven. mysql/postgres passed.
Rerun on the branch: not reproduced (3 cassandra attempts incl. cancelled concurrency dupes; 2 completed passes) => treated as flaky/transient.
No Go-parity logic difference found; async-config path is identical and passes.

Change: admin.rs now logs the error (tracing::warn) for UpdateDomainIsolationGroups internal errors so a recurrence is diagnosable from rust-frontend-b.log.

Runs (branch frontend-isogroup-cass):
- cassandra 43541560165 success, GATE SUMMARY failed: []
- mysql 43541610553 success, GATE SUMMARY failed: []
- cassandra 43542577906 success, GATE SUMMARY failed: []
Open: if it recurs, check the new warn log line for the persistence error text.
