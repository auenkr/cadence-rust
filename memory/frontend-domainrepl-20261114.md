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
