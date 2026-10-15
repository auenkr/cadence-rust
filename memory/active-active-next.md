# Active-active domain continuation (2026-10-15)

Go reference rules (registration/failover helpers now ported; UpdateDomain remains incomplete):

- Go common/domain/handler.go activeClustersFromRegisterRequest (1829): local
  domains and absent/empty scopes yield nil. Attribute registration versions
  come from each cluster's ORIGINAL InitialFailoverVersion, not the requested
  version or dynamic migration override. Unknown cluster errors include scope
  and attribute. It then validates global cluster membership/enabled status.
- validateActiveActiveDomainReplicationConfig in attrValidator.go rejects
  unknown/disabled active clusters and negative incoming versions before the
  handler computes versions. Nonnegative caller versions are then overwritten.
- buildActiveActiveClusterScopesFromUpdateRequest (1964): absent request/scopes
  preserves existing state; each supplied attribute uses its existing version
  (or UndefinedFailoverVersion=-1), GetNextFailoverVersion(target, current),
  then mergeActiveActiveScopes. If nothing changes, returns existing state.
- clusterattributes.go mergeActiveActiveScopes/mergeScope: preserve missing
  scopes/attributes; add new ones; replace an existing attribute only when the
  incoming version is STRICTLY greater. Equal/stale versions lose even if
  the incoming cluster name differs. Both-present merge creates nonnil scope
  maps; nil vs empty and Go pointer behavior need explicit fixture handling.
- updateReplicationConfig marks activeClusterChanged=true whenever a domain
  ActiveClusterName pointer is supplied, even if equal to existing. Therefore
  Rust now accepts repeated explicit domain targets; actual helper fixtures
  verify the presence flag and network checks verify AP unchanged versions and
  AA domain-level fencing increments.
- FailoverDomain destination validation sorts scopes then attribute names;
  Rust now uses that ordering and ignores legacy region fields. Original Go
  proto/Thrift mappers ignore deprecated region maps; Rust adapters now match.

Rust integration status:

- cadence-persistence::domain_replication ports registration_attributes,
  validate_incoming, validate_global, merge_scopes and build_update.
- handlers::domain_replication calls registration and global membership helpers;
  both HTTP/gRPC and Thrift registration use that shared path.
- DomainStore::failover validates/merges attributes, computes versions from
  stored state, preserves other attributes, and uses scoped destination ordering.
- DomainStore::update now uses optional patch DTOs and the cloned candidate
  controller in domain_update.rs. 34 actual-Go controller cases plus transport
  checks cover ordinary config/failover behavior; see domain-update-next.md for
  runtime policy, archival and distributed lifecycle limits.
- grpc_convert and Thrift mapping ignore deprecated region maps; protobuf empty
  maps normalize like Go's mapper. Thrift ActiveClusterInfo now emits version 0
  explicitly: Go's mapper dereferences that field rather than tolerating absence.
- Audit comparison for existing attributes now follows version changes rather
  than cluster-name changes, matching Go ToFailoverEvents. Full audit removal
  paths and persistence backend semantics still need review.

Evidence: 62 shared cases call actual unexported Go helpers via a test overlay,
plus a binary fixture from the real Go mapper/codec. Original-IDL gRPC checks
cover initialization, partial merges, caller-version replacement, audit versions,
no-change attributes, negative updates, repeated explicit targets, and sorted
error selection. Latest full run PASSED all 14 stages: /tmp/cadence-active-active-compat-v2/
results.json and outer exit file 0. It included 290 workspace tests, strict
clippy, differential suites and all network/SDK drivers. User development
server was not restarted; isolated test servers were cleaned up.

Next: port runtime UpdateDomain cooldown/dynamic policy, registration primary
restrictions and full graceful state transitions (metadata/AA rejection now exist), per-domain migration override lookup, workflow
attribute routing and passive ownership/replication. The generic Thrift Fields
builder still omits empty maps; exact empty-container presence needs broader wire
codec work, especially before relying on raw domain replication. No passive
replication or complete active-active domain service claim is justified yet.
