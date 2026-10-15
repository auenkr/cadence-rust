# Active-active domain continuation (2026-10-15)

Current source findings, not yet implemented:

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
  current Rust's blanket intended==record rejection appears wrong for repeated
  explicit domain-target requests. Verify with a differential test against the
  actual helper/handler before changing the existing Rust no-change expectation.
- FailoverDomain destination validation sorts scopes then attribute names;
  current Rust sorts target cluster names and consults legacy region fields
  that Go's current internal types no longer have. Original Go proto mapper
  ignores deprecated region_to_cluster; Rust grpc_convert still preserves it.

Rust integration points:

- handlers::domain_replication currently copies request ActiveClusters directly.
- InMemoryDomainStore::failover replaces active_clusters wholesale. It already
  computes domain-level AA fencing from current+1 using cluster metadata.
- DomainStore::update also replaces scopes wholesale and currently bypasses
  the Go domain handler's distinction between configuration and failover updates.
- grpc_convert::active_clusters_in/out currently emits deprecated region maps.
- Thrift converters/types still expose the old region map; preserve public wire
  compatibility, but follow current Go internal semantics for engine behavior.

Suggested next implementation: a persistence domain-replication helper module
for Go registration, validation, versioned merge and update; Go overlay test in
package domain calling actual unexported functions (as scheduler fixtures do),
with shared JSON cases and normalized Rust/Go scope names. Construct handlerImpl
with clusterMetadata and domainAttrValidator only for helper-level tests; whole
handler tests need mocked domain manager, clock, config and replication producer.
Then connect registration/failover/update and verify original-IDL network reads
and ListFailoverHistory attributes/versions. Do not claim passive ownership or
replication merely because domain metadata matches.
