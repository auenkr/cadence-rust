# Frontend admin gaps (branch fe-admin-gaps, 2026-11-01)

Items (one commit each, all validated only on GitHub Actions):
1. DescribeCluster: live membership resolver (`ClusterMembership`, `MembershipSlot`, Ringpop adapter in matching) + Go persistence keys (`visibilityStore`, `historyStore`, `advancedVisibilityEnabled`).
2. AddSearchAttribute/GetSearchAttributes: Go does NOT persist to the config store (UpdateValue rejects the map); faithful behavior is the `frontend.validSearchAttributes` overlay update. `cadence-frontend` Handler now shares `add_search_attribute_core` / `valid_search_attributes` with service-core.
3. Replication proxy: `replication_convert.rs` (proto<->API, int16 task type narrowing, oneof order) and `admin_replication.rs` (GetReplicationMessages, GetDLQReplicationMessages, CountDLQMessages, replication-type Read/Purge/Merge DLQ via History; Go defaults: read page size 1000, end id MaxInt64-1, merge page size passed through). Wired into axum, YARPC/Thrift (CountDLQMessages is "Feature not supported on TChannel" in Go, so not in Thrift) and gRPC admin. Domain-task body (Domain info/config) is not mapped: History never emits domain tasks. API `ReadDLQMessagesResponse` has no `ReplicationTasksInfo` field (pre-existing).
4. Audit identity: Go `updateDomainAuditLog` never sets Identity/IdentityType (go/common/domain/handler.go:685-720), so empty values in Rust are already faithful. No change; domain persistence is owned by fe-domain-visible.

CI evidence:
- run 41334800943 (cadence-matching, commit 1): FAILED at rustfmt check (formatting only).
- run 41335898015 (cadence-matching, format_all=true, head 3cc0fda54): success (clippy + tests); rustfmt patch applied in follow-up commit.
- run 41336600237 (cadence-frontend, head 8ea349c40): success (fmt check, clippy, tests).
