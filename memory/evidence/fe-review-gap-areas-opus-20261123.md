# Cross-verification of fe-review-gap-areas-sol-20261123 (Opus)

Base: origin/fe-review-gap-areas (code at d592e6dc1). This was a source-only review; no cargo, go or docker was run.
Result: 16 CONFIRMED, 0 REJECTED, 4 ADJUSTED (#6, #8, #11, #12).

| # | Verdict | Evidence / note |
|---|---|---|
| 1 | CONFIRMED | Rust redirection_runtime.rs:211-216 reads `workflowExecution` for every op in EXECUTION_OPERATIONS (:96-103), but GetWorkflowExecutionHistoryRequest serializes the field as `execution` (cadence-api service_types.rs:1456). Go passes `gp1.GetWorkflowExecution()` (api_generated.go:441-444). |
| 2 | CONFIRMED | Go middleware.go:229-240 rewrites or deletes the partition-config and isolation headers from ctx. Rust frontend_remote.rs:178-186 only appends rule-forwarded inbound headers. production_hooks.rs:46-57 overrides the client group when the forwarding header is present. |
| 3 | CONFIRMED | Go invokes the local BackfillSchedule handler inside the Redirect callback (api_generated.go:86-95), and policy.go:356+ retries DomainNotActive. Rust ScheduleRedirect::route (redirection_runtime.rs:692-715) returns Ok(None), and workflow_api.rs:707-709 serves the request locally outside the policy. |
| 4 | CONFIRMED | Go data_manager_interfaces.go:3309-3324 checks only AttributeScopes with nonempty ClusterAttributes and has no region fallback. Rust redirection_runtime.rs:286-297 accepts empty scopes and also the region map. |
| 5 | CONFIRMED | Go service.go:158 wraps whenever the policy pointer is non-nil (noop included). Rust workflow_backend.rs:1758-1761 removes the wrapper for Noop. |
| 6 | ADJUSTED | Real, but Go's text is `unknown cluster name: %v with given cluster client map: %v` (clientBean.go:160), not just the prefix. Rust redirection_runtime.rs:665-669 also wraps the error as InternalServiceError, while Go returns a plain error. Match the full Go text and error type. |
| 7 | CONFIRMED | Go handler.go:1878-1882 selects archived history before decoding the token (:1954). Rust handler_workflow.rs:2078-2090 decodes and validates the live token first. |
| 8 | ADJUSTED | Real but Cassandra-only. It breaks only when an existing token crosses a Go/Rust boundary or when Rust paging does not match Go's. Fix it, but treat it as medium priority for clusters that run only Rust. |
| 9 | CONFIRMED | Go es client.go:145 retries with a fresh scroll on a stale node. Rust es_visibility.rs:225 has no retry. |
| 10 | CONFIRMED | Go uses encoding/json semantics (null, case-insensitive keys, first value only). Rust uses strict serde. |
| 11 | ADJUSTED | Rust domains.rs:2434-2440 adds the error. Go passes the size to the store, where a negative LIMIT fails with a store-specific error. Fix by removing the Rust check and passing the value through. Priority is low. |
| 12 | ADJUSTED | Go checks `NextPageToken != nil` (handler.go:1954). Matching that requires the Thrift bridge to keep Option<Vec<u8>>. That change touches the codec, so limit it to the live-history and admin raw-history paths. |
| 13 | CONFIRMED | Per the cited code, Go handler.go:823/848 writes only on a config or active-cluster change. Rust domains.rs:960/2218 always writes and publishes the stored record. |
| 14 | CONFIRMED | Go publishes the replication task before the audit entry (handler.go:324/652/848/1135). Rust domains.rs:2127-2321 audits first. |
| 15 | CONFIRMED | Rust handlers.rs:688 validates retention first. Go validates archival and active-active first (handler.go:212-287). |
| 16 | CONFIRMED | Go admin/handler.go:629-636 and :652-657: `request.Type == nil` returns ErrRequestNotSet, then ClusterName, and the shard is left to History. Rust admin.rs:2276/2337 and yarpc.rs:915/980 check the shard first. |
| 17 | CONFIRMED | Go handler.go:1268 validates the execution before the domain. Rust admin.rs:1057-1077 looks up the domain first and defaults the execution. |
| 18 | CONFIRMED | Go handler.go:1185 calls domainDLQHandler.Count(forceFetch), which is cached. Rust admin_replication.rs:311+ always queries persistence. |
| 19 | CONFIRMED | Go handler.go:1298 resolves the domain first. Rust admin.rs:1923 and yarpc.rs:952 validate the fields first. |
| 20 | CONFIRMED | Go handler.go:271-279 resolves the shard owner before the domain. Rust admin.rs:2673-2680 resolves the domain first. |

## Agreed fix packages (disjoint files)

**WP-A: redirection and forwarding (#1-#6)**
Files: rust/crates/cadence-frontend/src/wrappers/redirection_runtime.rs, wrappers/clusterredirection*.rs, workflow_backend.rs, production_hooks.rs; rust/crates/cadence-service-core/src/frontend_remote.rs, workflow_api.rs (schedule dispatch only).

**WP-B: frontend pagination and visibility (#7, #9, #10, #12 live-history part)**
Files: rust/crates/cadence-frontend/src/handler_workflow.rs, handler_workflow_history.rs, archive_readers.rs, es_visibility.rs, es_query.rs; the Thrift codec in cadence-service-core/src/yarpc.rs for the GetWorkflowExecutionHistory token presence only. Coordinate on yarpc.rs with WP-C: B edits only the history codec, and C edits only the admin handlers.

**WP-C: domain handler and admin (#8, #11, #12 admin raw-history part, #13-#20)**
Files: rust/crates/cadence-persistence/src/domains.rs, raw_history.rs, matching_domain_cassandra.rs; rust/crates/cadence-service-core/src/admin.rs, admin_replication.rs, handlers.rs, yarpc.rs (admin and RegisterDomain entries only). This package is large and can be split into C1 (domains.rs + handlers.rs: #11, #13-#15) and C2 (admin*.rs + persistence DLQ: #8, #12-admin, #16-#20).
