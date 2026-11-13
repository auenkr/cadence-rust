# Frontend domain-replication source (2026-11-14)
Branch `frontend-domainrepl`. Gap: Rust Frontend gRPC admin `get_domain_replication_messages` was a stub ("domain replication queue not enabled for cluster"), so a Rust-Frontend cluster could not be a domain-replication source for a Go consumer.

## Changes
- `rust/crates/cadence-matching/src/grpc_admin.rs`: gRPC handler now calls the shared `cadence_service_core::admin::domain_replication_messages_response` (queue read + ack-level update, same logic the Thrift/HTTP path already used; mysql/postgres/cassandra via `DomainMetadataPersistence`), converting with `replication_messages_out`.
- `rust/crates/cadence-service-core/src/replication_convert.rs`: `replication_task_out` previously dropped info/config/replication_config for domain tasks (Go consumer panicked in `ToDomainTaskAttributes`, shared.go:545). Added `domain_out` mirroring Go `FromDomainTaskAttributes`/`FromDescribeDomainResponseDomain`. Not mapped: isolation groups, async workflow config, active_clusters, failover_info (Go maps them; none are set by the gate) - residual gap for active-active domains.
- Gate: `tests/gates/run-frontend-two-cluster-gate.py --rust-frontend-active` + workflow input `rust_frontend_active` (with `two_cluster=true`): cluster B = Rust Frontend + Go history/matching/worker, cluster A all Go; register global domain and update on B, poll A.
- Domain-handler publish on register/update, DLQ calls and tchannel path already existed in Rust (not changed, not re-audited line by line).

## Evidence
Head SHA 984daac8085a06f977658197eca785ff0c5c42f1.
- rust-ci cadence-matching (earlier head 0f9f4b55c): https://github.com/auenkr/cadence-rust/actions/runs/43501997740 success
- rust-ci cadence-service-core: https://github.com/auenkr/cadence-rust/actions/runs/43508508474 success
- mysql two-cluster, Rust FE source: https://github.com/auenkr/cadence-rust/actions/runs/43508528911 success, `GATE SUMMARY failed: []`
  (earlier runs 43502092028, 43504860773 failed: wrong Go seed ports; then missing Domain proto -> Go worker panic)
- Default two-cluster gate regression: https://github.com/auenkr/cadence-rust/actions/runs/43509659013 success, `GATE SUMMARY failed: []`

## Not covered
postgres and cassandra: `run-frontend-two-cluster-lane.sh` is MySQL-only, so the end-to-end replication gate was NOT run on them. The handler uses the same persistence trait implemented for all three backends, but that is unverified end-to-end. Follow-up: a two-cluster lane for postgres/cassandra.

## Part 2 (branch frontend-domainrepl2)
Changes:
- `replication_convert.rs::domain_out`: now maps isolation_groups (sorted by name, Go `FromIsolationGroupConfig`), async_workflow_config (`FromDomainAsyncWorkflowConfiguraton`) and active_clusters by cluster attribute (`FromActiveClusters`/`FromClusterAttributeScope`). failover_info stays None: Go builds the DescribeDomainResponse from task attributes without FailoverInfo. Thrift path already carried all fields (shared Thrift types).
- Audit divergences fixed (Go `UpdateIsolationGroups` / `UpdateAsyncWorkflowConfiguraton`, common/domain/handler.go): Rust async-config update did not publish a domain replication task for global domains; isolation-group update only changed the in-memory store (not persisted, no version bump, no publish). Added `FrontendDomainManager::update_isolation_groups` (persist, config_version+1, publish Update) and publish in async update; admin.rs HTTP handler now uses `domain_manager`.
- Not changed: Go cool-down / primary-cluster checks on these two updates are not ported (gate passes without them). Domain DLQ calls and tchannel GetDomainReplicationMessages not re-audited line by line.
- Gate: Rust-FE-active mode now also sets isolation groups (`admin isolation-groups update-domain --set-drains`) and async-wf config (`admin async-wf-queue update --json`) on B and asserts both replicate to A; updates retry on Go "Domain update too frequent" (first postgres run failed on that cool-down).
Evidence (head dd53650f25b1a0f7362d240a9a5937b02756d94b for gates):
- rust-ci service-core 43511756397, persistence 43511750836, matching 43511745351: success (head before gate-script-only commit)
- rust_frontend_active two_cluster: mysql 43514692582, postgres 43514697798, cassandra 43514702914: all success, `GATE SUMMARY failed: []`
- default two-cluster regression 43513067704: success (previous head 7f code-identical; gate script change only touches rust-frontend-active branch)
- Earlier failed: postgres 43513057218 (cool-down, fixed in harness)

## Part 3 (branch frontend-domainrepl3)
Changes (Go refs: common/domain/handler.go UpdateIsolationGroups L1175, UpdateAsyncWorkflowConfiguraton L1271, ensureUpdateOrFailoverCooldown L473; dlq_message_handler.go; service/frontend/admin/handler.go ReadDLQMessages/Purge/Merge/GetDomainReplicationMessages; replication_queue.go):
- `domain_update.rs::check_admin_config_update`: cool-down (ServiceBusy "Domain update too frequent.") then primary-cluster check for global domains (BadRequest). `FrontendDomainManager::update_isolation_groups/update_async_workflow_config` now take `(metadata, policy)` and enforce it; isolation update rejects a nil config ("invalid request, isolationGroup configuration must be set"). admin.rs maps Busy->ServiceBusyError(503), Invalid->BadRequest. Main UpdateDomain/failover paths already had cool-down + primary checks (domain_update.rs check_cooldown/PRIMARY); verified, unchanged.
- Domain DLQ audit fixes (admin.rs): invalid page-token message now Go `%v` format (`[1 2 3]`); merge only fails on nil DomainTaskAttributes (Go message), no task_type check; merge always range-deletes (ack, acked) and updates ack level with acked starting at 0 like Go; DLQ decode error text "failed to decode dlq task"; tchannel/gRPC GetDomainReplicationMessages decode error text "failed to decode task". Ack levels/paging (LE 8-byte token, page size 1000 read default, token when rows>=page size) already matched.
- Gate (`run-frontend-two-cluster-gate.py --rust-frontend-active`): also runs `admin dlq read|purge|merge --dlq_type domain` through the Go CLI against the Rust Frontend.
- Tests: persistence `admin_config_updates_enforce_cooldown_and_primary_cluster`, service-core `domain_dlq_page_token_error_matches_go_format`; e2e admin_isolation_and_async_config_flow now sets failoverCoolDown=0s.
Evidence (head d2287041520d235f7fa40521e9a99442078da7b5; earlier fmt-only failures 43516261486/43516267688/43516275414, e2e cool-down failure 43516902343):
- rust-ci persistence 43516897832 success, matching 43516906975 success (head 15504c6a), service-core 43519779396 success
- rust_frontend_active two_cluster: mysql 43519802311, postgres 43519817199, cassandra 43519829350 all success, `GATE SUMMARY failed: []` (domain-dlq-read/purge/merge ok)
- default two-cluster regression 43519845041 success, `GATE SUMMARY failed: []`
