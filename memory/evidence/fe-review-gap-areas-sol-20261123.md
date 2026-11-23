# Frontend parity review of redirection, pagination, domains, and admin APIs

Source comparison at `d592e6dc103d6baa811bc09330da64e635e7f97e`. Findings refer to the production paths. Validation was source-only. The outbound gRPC transport and Pinot visibility were outside this review.

## Cluster redirection

### 1. High — History routing drops the workflow execution

- Go `go/service/frontend/wrappers/clusterredirection/api_generated.go:441`
- Rust `rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs:211` and `rust/crates/cadence-api/src/service_types.rs:1456`
- Go passes the workflow and run IDs into the policy. Rust looks for `workflowExecution` in its normalized request while the history request uses `execution`. An active-active history request that requires workflow-based routing can therefore be handled on the local cluster without resolving the workflow’s active cluster.
- Suggested fix — Extract the typed history request’s execution and test routing with a run whose active cluster differs from the local cluster.

### 2. High — Forwarding loses the caller’s resolved isolation context

- Go `go/common/rpc/middleware.go:214` and `go/common/rpc/middleware.go:229`
- Rust `rust/crates/cadence-service-core/src/frontend_remote.rs:72`, `rust/crates/cadence-service-core/src/frontend_remote.rs:178`, and `rust/crates/cadence-frontend/src/production_hooks.rs:41`
- Go writes or clears the two forwarded isolation headers from the resolved request context. Rust forwards inbound headers by rule. The receiving Rust Frontend then replaces the context derived from `cadence-client-isolation-group` as soon as it sees the forwarding marker. A request arriving at the source with just the client isolation header can lose both its group and its partition config after forwarding.
- Suggested fix — Carry the resolved group and config into the remote call and use them to set or clear the two forwarded headers after applying generic forwarding rules.

### 3. Medium — Local schedule errors bypass the policy’s cross-cluster retry

- Go `go/service/frontend/wrappers/clusterredirection/api_generated.go:86` and `go/service/frontend/wrappers/clusterredirection/policy.go:356`
- Rust `rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs:704` and `rust/crates/cadence-service-core/src/workflow_api.rs:707`
- Go invokes the local schedule handler inside the policy callback. The policy can retry a `DomainNotActive` that identifies a different active cluster. Rust’s callback returns `Ok(None)` for a local target and the dispatch runs the local schedule handler afterward, outside the retry path.
- Suggested fix — Invoke the local schedule handler inside the policy callback and preserve the typed error until the policy finishes.

### 4. Medium — Active-active detection accepts empty scopes and legacy regions

- Go `go/common/persistence/data_manager_interfaces.go:3309`
- Rust `rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs:286`
- Go requires a nonempty attribute map in at least one scope. Rust treats a scope with no attributes, or a legacy region map alone, as active-active. Either persisted configuration can change the routing path, including bypassing normal active-passive forwarding.
- Suggested fix — Use Go’s nonempty nested attribute-map predicate.

### 5. Low — Explicit noop policy skips wrapper validation

- Go `go/service/frontend/service.go:158`, `go/service/frontend/wrappers/clusterredirection/api_generated.go:883`, and `go/service/frontend/wrappers/clusterredirection/policy.go:263`
- Rust `rust/crates/cadence-frontend/src/workflow_backend.rs:1758`
- A configured Go noop policy still runs the wrapper’s task-token decoding or domain lookup before dispatch. Rust removes the wrapper for noop. A request that fails both wrapper validation and underlying API validation can return a different first error.
- Suggested fix — Preserve the distinction between absent config and explicit noop config, and run the wrapper with the noop callback in the latter case.

### 6. Low — Unknown forwarding target returns different error text

- Go `go/client/clientBean.go:157`
- Rust `rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs:665`
- Go reports `unknown cluster name: <cluster>` when it cannot find a remote Frontend client. Rust reports `Cannot find remote Frontend client for cluster <cluster>`.
- Suggested fix — Preserve the Go error message in the lookup failure path.

## Pagination

### 7. High — Archived history continuation is validated as live history

- Go `go/service/frontend/api/handler.go:1878` and `go/service/frontend/api/handler.go:1954`
- Rust `rust/crates/cadence-frontend/src/handler_workflow.rs:2078`, `rust/crates/cadence-frontend/src/handler_workflow.rs:1164`, and `rust/crates/cadence-frontend/src/archive_readers.rs:325`
- Go chooses archived history before decoding the live-history continuation. Rust decodes and validates the live token before invoking the archive reader. The archiver returns its own provider token, so a valid archived continuation can fail with `Invalid NextPageToken.` or a run mismatch.
- Suggested fix — Select the history source before token decoding and give the original bytes to the selected archiver.

### 8. High — Cassandra domain DLQ continuation is incompatible

- Go `go/common/persistence/nosql/nosql_queue_store.go:197` and `go/common/persistence/nosql/nosqlplugin/cassandra/queue.go:102`
- Rust `rust/crates/cadence-service-core/src/admin.rs:1787`, `rust/crates/cadence-service-core/src/admin.rs:3874`, and `rust/crates/cadence-persistence/src/matching_domain_cassandra.rs:205`
- Go passes Cassandra’s opaque page state through Read and Merge for the domain DLQ. Rust uses an unpaged query bounded by a message ID and expects a fixed eight-byte little-endian cursor from every backend. A client resuming a Cassandra page across a Frontend replacement or rollback cannot carry its existing token across.
- Suggested fix — Pass tokens through the persistence interface and use native Cassandra page state with Go’s query bounds and page size. Keep the SQL message-ID cursor for SQL.

### 9. Medium — Elasticsearch scan skips stale-node scroll recovery

- Go `go/common/elasticsearch/client.go:145`
- Rust `rust/crates/cadence-frontend/src/es_visibility.rs:225`
- If the scroll ID points to an unavailable node, Go starts one fresh scroll and returns that result. Rust fails the request immediately on the first scroll error.
- Suggested fix — Recognize the same node-unavailable error, retry once with a fresh scroll, and preserve the Go terminal error if that attempt fails.

### 10. Low — Elasticsearch token JSON validation differs

- Go `go/common/elasticsearch/page_token.go:45`
- Rust `rust/crates/cadence-frontend/src/es_query.rs:891` and `rust/crates/cadence-frontend/src/es_query.rs:903`
- Go accepts JSON `null` as a zero-value cursor, matches field names case-insensitively, and reads the first JSON value. Rust rejects `null` and a second JSON value, and a field such as `scrollid` is ignored, which can restart a scan.
- Suggested fix — Match Go’s field lookup and single-value decoding rules while retaining the generated token shape.

### 11. Low — ListDomains adds a negative page-size check

- Go `go/common/domain/handler.go:371` and `go/common/persistence/sql/sql_domain_store.go:442`
- Rust `rust/crates/cadence-persistence/src/domains.rs:2434`
- Go delegates any nonzero signed page size to persistence. Rust returns `BadRequest: Page size must be positive.` before reading the token or calling the store. Zero defaults to 100 in both implementations.
- Suggested fix — Preserve the signed size through validation and reproduce the selected backend’s handling.

### 12. Low — History pagination conflates absent and empty Thrift tokens

- Go `go/service/frontend/api/handler.go:1954`, `go/service/frontend/api/handler.go:3094`, `go/service/frontend/admin/handler.go:737`, `go/service/frontend/admin/handler.go:759`, and `go/service/frontend/admin/handler.go:1574`
- Rust `rust/crates/cadence-frontend/src/handler_workflow.rs:2078`, `rust/crates/cadence-service-core/src/yarpc.rs:2947`, `rust/crates/cadence-service-core/src/admin.rs:2438`, and `rust/crates/cadence-persistence/src/raw_history.rs:154`
- Go uses token presence to select the continued-page path for live workflow history and admin raw history. A present empty Thrift token fails JSON decoding. Rust uses length and treats the same request as a first page. In admin raw history, a malformed nonempty token gets the Rust typed BadRequest `Invalid pagination token.`, while Go propagates the JSON decode error.
- Suggested fix — Preserve token presence in the Thrift bridge and match Go’s malformed-token error mapping at the admin boundary.

## Domain handler

### 13. Medium — UpdateDomain writes on no-op and can publish the old candidate

- Go `go/common/domain/handler.go:823` and `go/common/domain/handler.go:848`
- Rust `rust/crates/cadence-persistence/src/domains.rs:960` and `rust/crates/cadence-persistence/src/domains.rs:2218`
- Go writes the row only for a configuration or active-cluster change, then passes the validated candidate to domain replication. Rust always writes the row and publishes the value left in the local record. An update whose only change is a BadBinaries merge can return the merged candidate while publishing the prior config. A no-op can also incur a persistence failure Go avoids.
- Suggested fix — Return the candidate and row-write decision separately, persist only when required, and publish from the candidate.

### 14. Medium — Domain audit is written before the replication task

- Go `go/common/domain/handler.go:324`, `go/common/domain/handler.go:652`, `go/common/domain/handler.go:848`, and `go/common/domain/handler.go:1135`
- Rust `rust/crates/cadence-persistence/src/domains.rs:2127`, `rust/crates/cadence-persistence/src/domains.rs:2225`, `rust/crates/cadence-persistence/src/domains.rs:2268`, and `rust/crates/cadence-persistence/src/domains.rs:2321`
- With domain audit enabled, Go publishes the global-domain task before creating the audit entry for Register, Update, Failover, and Deprecate. Rust audits first. A queue failure after the row commit can leave a Rust audit entry and affect failover history even though Go returns before auditing.
- Suggested fix — Publish after the domain row commit and before attempting the audit. Keep Go’s best-effort audit persistence semantics.

### 15. Low — RegisterDomain reports invalid retention before invalid archival configuration

- Go `go/common/domain/handler.go:212`, `go/common/domain/handler.go:275`, and `go/common/domain/handler.go:287`
- Rust `rust/crates/cadence-service-core/src/handlers.rs:688` and `rust/crates/cadence-service-core/src/yarpc.rs:378`
- Go validates archival state and builds the active-active attributes before validating retention. A registration combining invalid retention and an invalid archival URI in an archive-enabled deployment returns the archival error in Go. Rust returns `A valid retention period is not set on request.` first.
- Suggested fix — Perform retention validation at the same point as Go after archival and attribute preparation.

## Admin APIs

### 16. Medium — ResetQueue and DescribeQueue validate shard bounds before required fields

- Go `go/service/frontend/admin/handler.go:629` and `go/service/frontend/admin/handler.go:652`
- Rust `rust/crates/cadence-service-core/src/admin.rs:2276`, `rust/crates/cadence-service-core/src/admin.rs:2337`, `rust/crates/cadence-service-core/src/yarpc.rs:915`, and `rust/crates/cadence-service-core/src/yarpc.rs:980`
- Go returns `Request is nil.` for an omitted task type and checks the cluster field before delegating. Rust checks the shard first and returns `TaskType is not set on request.` if the task type is missing and the shard passes. Invalid shard IDs also mask the missing cluster error.
- Suggested fix — Match the required-field ordering and error text and delegate shard handling as Go does.

### 17. Medium — RefreshWorkflowTasks resolves the domain before validating execution

- Go `go/service/frontend/admin/handler.go:1268`
- Rust `rust/crates/cadence-service-core/src/admin.rs:1057` and `rust/crates/cadence-service-core/src/admin.rs:1077`
- Go checks optional execution presence and validates its IDs before looking up the domain. Rust looks up the domain first and replaces an absent execution with an empty execution. Depending on the domain, a missing execution therefore yields either a domain error or a workflow-ID error.
- Suggested fix — Validate the optional execution at the start of the shared admin handler.

### 18. Medium — Domain CountDLQMessages bypasses the cached count

- Go `go/service/frontend/admin/handler.go:1185` and `go/common/domain/dlq_message_handler.go:103`
- Rust `rust/crates/cadence-service-core/src/admin_replication.rs:311` and `rust/crates/cadence-matching/src/grpc_admin.rs:981`
- Go returns the last known domain DLQ count when initialized and `forceFetch` is false. Rust queries persistence for every domain count and applies the flag only on the History path. A non-forced count after a successful count can fail in Rust during a transient persistence outage while Go still returns the cached value.
- Suggested fix — Implement the domain DLQ count cache and refresh lifecycle, including forced refresh and initial load.

### 19. Medium — ResendReplicationTasks validates fields before domain resolution

- Go `go/service/frontend/admin/handler.go:1298` and `go/common/ndc/history_resender.go:121`
- Rust `rust/crates/cadence-service-core/src/admin.rs:1923` and `rust/crates/cadence-service-core/src/yarpc.rs:952`
- Go resolves the domain UUID before requesting history. Rust rejects missing execution IDs or remote cluster before domain resolution. A missing run ID combined with an unknown domain yields the domain lookup error in Go and `Workflow execution is not set on request.` in Rust.
- Suggested fix — Match Go’s domain-resolution precedence and leave downstream execution validation to the corresponding history request path.

### 20. Low — Admin DescribeWorkflowExecution checks the domain before the shard owner

- Go `go/service/frontend/admin/handler.go:269` and `go/service/frontend/admin/handler.go:277`
- Rust `rust/crates/cadence-service-core/src/admin.rs:2673`
- Go resolves the History host first. Rust looks up the domain first. If shard-owner resolution fails and the supplied domain is unknown, the two implementations return different errors. DeleteWorkflow shares this ordering difference through its describe step.
- Suggested fix — Resolve the shard owner before calling the domain manager.
