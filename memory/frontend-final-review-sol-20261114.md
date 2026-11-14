# Rust Frontend independent parity review — 2026-11-14

Reviewed commit: `924bcf463ca1d7b5bfdaf0c3655e783af845a867` (origin/master when this review branch was created).

Result: **18 findings: 5 P1, 12 P2, 1 P3**. Sixteen are CONFIRMED by static source/call-chain inspection; two are PLAUSIBLE and need targeted confirmation. P1 denotes broken supported operations or materially corrupt wire data; P2 denotes observable compatibility, operational or resource-control differences; P3 denotes a narrower compatibility edge case. No runtime reproduction is claimed.

Read AGENTS.md. No Cargo, Go, Docker, services, or local validation workloads were run. This report is the only changed file. Domain-RPC validation, graceful-failover concurrency, missing UpdateTaskListPartitionConfig/CountDLQMessages handlers, and typed error passthrough are excluded as requested.

## P1

### 1. CONFIRMED — gRPC asynchronous starts bypass the production publisher

- Rust: `rust/crates/cadence-matching/src/grpc.rs:1195`, `:1211`, `:1214`, `:1261`.
- Go: `go/service/frontend/wrappers/grpc/api_generated.go:279`, `:264`; `go/service/frontend/api/handler.go:1601`, `:2216`.
- Difference: both async-start adapters call `self.domain(...)` and enqueue on the local execution engine rather than calling the installed production Frontend hook. Go validates through its handler, obtains the configured producer, encodes the request, propagates inbound headers into the message, and publishes before acknowledging. A successful Rust gRPC response therefore does not establish publication to the configured durable async queue; the request can be stranded in local state and lose the production wrapper behavior. This affects StartWorkflowExecutionAsync and SignalWithStartWorkflowExecutionAsync.
- Fix: route both original async operations, with their complete wrapper requests and caller context, through the same production handler/publisher used by the Thrift path. Verify publication failure and restart recovery as well as successful submission.

### 2. CONFIRMED — gRPC activity-by-ID operations require a local activity token

- Rust: `rust/crates/cadence-matching/src/grpc.rs:423`, `:440`, `:2248`, `:2267`, `:2285`, `:2302`.
- Go: `go/service/frontend/wrappers/grpc/api_generated.go:219`, `:174`; `go/service/frontend/api/handler.go:885`, `:699` (and the corresponding failed/canceled ByID handlers).
- Difference: completed, failed, canceled and heartbeat ByID adapters first call `activity_token`, which searches the local execution engine, then invoke the token-based RPC. Go sends the domain/execution/activity ID to History through the original ByID handler. Activities owned by Go History need not exist in the Rust Frontend's local engine, so these supported SDK calls fail before reaching History. Even a successful local lookup selects the token operation's wrappers and metrics instead of the ByID operation's.
- Fix: map each request directly to its production ByID hook, preserving the original operation and caller context; let History resolve the activity. Test SDK asynchronous completion and heartbeat against Go History.

### 3. CONFIRMED — gRPC domain DLQ read/purge/merge acknowledge without performing the operation

- Rust: `rust/crates/cadence-matching/src/grpc_admin.rs:998`, `:1040`, `:1067` (the `type == 2` branches); working core counterparts are `rust/crates/cadence-service-core/src/yarpc.rs:771`, `:827`, `:1006`.
- Go: `go/service/frontend/admin/handler.go:1068`, `:1129`, `:1203`.
- Difference: ReadDLQMessages returns empty tasks and token for the domain DLQ; PurgeDLQMessages immediately succeeds; MergeDLQMessages immediately returns an empty token. No domain DLQ persistence/merge operation runs. Go calls domainDLQHandler Read/Purge/Merge and returns their data/errors. Operators can falsely conclude the queue is empty, purged, or replayed.
- Fix: share the core domain-DLQ implementations with the gRPC adapter, mapping bounds, page size, page token, tasks and errors. Verify nonempty queues and multi-page merges through both transports.

### 4. CONFIRMED — ReleaseSemaphore decisions are lost on Thrift and rejected on gRPC

- Rust: `rust/crates/cadence-api/src/tasks.rs:164`, `:263`; `rust/crates/cadence-thrift/src/convert.rs:5166`, `:5188`; `rust/crates/cadence-proto/src/convert/decisions.rs:276`.
- Go: `go/idls/thrift/shared.thrift:588` (Decision field 140); `go/common/types/mapper/thrift/shared.go:1119`, `:1144`; `go/common/types/mapper/proto/api.go:6808`.
- Difference: the API Decision and Thrift conversions end with AcquireSemaphore attributes (field 130), omitting ReleaseSemaphore attributes (field 140). The protobuf converter handles AcquireSemaphore, then its wildcard returns Unimplemented, "This decision type has not been ported." for ReleaseSemaphore. Go maps and forwards this supported decision. Thrift can forward a release decision type stripped of its attributes; gRPC rejects it outright.
- Fix: add the release attributes to the shared model and both directions of both codecs, preserving field 140 and the protobuf oneof. Exercise RespondDecisionTaskCompleted against Go History with acquire/release decisions.

### 5. CONFIRMED — Thrift visibility response timestamps multiply nanoseconds a second time

- Rust: `rust/crates/cadence-frontend/src/workflow_visibility.rs:33`, `:49`, `:59`; `rust/crates/cadence-thrift/src/convert.rs:2326`, `:2331`, `:2342` (helper at `:41`).
- Go: `go/common/types/mapper/thrift/shared.go:7202`, `:7213`.
- Difference: production visibility rows are converted from microseconds to nanoseconds in row_to_info. WorkflowExecutionInfo::to_thrift then treats these numbers as milliseconds and multiplies by 1,000,000 again. Modern timestamps saturate to i64::MAX: e.g. 1,700,000,000,123,456,000 ns becomes 9,223,372,036,854,775,807. Go copies its internal nanoseconds directly. This corrupts start/execution/close times returned by the DB visibility list paths over Thrift.
- Fix: establish a single explicit timestamp unit across the production model and its codecs; convert legacy local-engine millisecond values at that engine's boundary. Check exact timestamp values through actual list RPC responses, including nonzero fractions and absent close time.

## P2

### 6. CONFIRMED — gRPC activity polls interpret decoded milliseconds as nanoseconds

- Rust: `rust/crates/cadence-frontend/src/workflow_matching.rs:609`, `:623`; `rust/crates/cadence-thrift/src/convert.rs:4557`, `:4565`, `:4567`, `:4571`; `rust/crates/cadence-proto/src/convert/mod.rs:320`, `:329`, `:337`; adapter `rust/crates/cadence-matching/src/grpc.rs:2472`.
- Go: `go/common/types/mapper/proto/api.go:2215` (FromPollForActivityTaskResponse).
- Difference: production Matching timestamps arrive in nanoseconds, are divided by 1,000,000 by FromThrift, then passed to timestamp_nanos by the gRPC response mapper. Scheduled, started and current-attempt scheduled timestamps are therefore a million times too small, appearing near the epoch. A Thrift decode/re-encode also discards sub-millisecond precision. Go maps nanoseconds directly to protobuf timestamps.
- Fix: keep these timestamps in nanoseconds through the shared model, or consistently convert milliseconds at the gRPC boundary while separately preserving wire precision. Verify all three timestamps from a real Go Matching response.

### 7. CONFIRMED — gRPC DescribeWorkflowExecution discards query_consistency_level

- Rust: `rust/crates/cadence-matching/src/grpc.rs:1520` (describe_workflow_execution); GET-hook reconstruction at `:372`.
- Go: `go/common/types/mapper/proto/api.go:1281`, `:1288`; `go/idls/proto/uber/cadence/api/v1/service_workflow.proto:231`.
- Difference: the adapter reconstructs a GET request containing only domain, workflowId and runId, dropping query_consistency_level. The shared Rust request already supports the field, and Go maps it into the handler/History request. A requested strong-consistency describe becomes the default consistency request.
- Fix: use a typed DescribeWorkflowExecution hook request containing the field, or extend the reconstruction without collapsing unspecified/default/explicit values. Verify the outgoing History request.

### 8. CONFIRMED — gRPC task-list descriptions lose partition configuration and returned task-list fields

- Rust: `rust/crates/cadence-proto/src/convert/mod.rs:658`, `:695`; `rust/crates/cadence-matching/src/grpc.rs:1655`, `:1670`, `:1691`.
- Go: `go/common/types/mapper/proto/api.go:1246`, `:1253`, `:1874`.
- Difference: describe_task_list_out always returns partition_config=None and task_list=None. DescribeTaskList substitutes the request task list instead of the returned one. GetTaskListsByDomain rebuilds the returned task list with its name only, defaulting kind/base_name, and still drops partition_config. Go maps the complete returned TaskList and PartitionConfig, including map entries in GetTaskListsByDomain. CLI/SDK inspection cannot see the actual partition configuration or canonical task-list metadata.
- Fix: implement complete response conversion once and use it unchanged for both RPCs, including optional presence and task-list kind/base_name.

### 9. CONFIRMED — gRPC GetClusterInfo returns empty supported-client versions

- Rust: `rust/crates/cadence-matching/src/grpc.rs:1540` (get_cluster_info).
- Go: `go/service/frontend/api/handler.go:3245`; `go/service/frontend/wrappers/grpc/api_generated.go:89`.
- Difference: the adapter constructs SupportedClientVersions::default(), returning empty version strings. Go reports Go SDK 1.7.0 and Java SDK 1.5.0; the Rust production handler also has a separate implementation of this operation. The gRPC adapter bypasses that handler and its wrappers.
- Fix: route through the production GetClusterInfo hook and map its response rather than synthesizing an empty object. Compare SDK/CLI output on both transports.

### 10. CONFIRMED — outbound long-poll TTLs do not preserve the caller's remaining deadline

- Rust: `rust/crates/cadence-frontend/src/workflow_backend.rs:353`, `:380`; `rust/crates/cadence-frontend/src/workflow_matching.rs:28`, `:572`, `:620`, `:792`; `rust/crates/cadence-service-core/src/tchannel_json_client.rs:243`; `rust/crates/cadence-matching/src/tchannel.rs:606`.
- Go: `go/client/history/client.go:125`, `:137`; `go/service/frontend/api/handler.go:1907` (queryHistory passes ctx to PollMutableState).
- Difference: backend calls receive a fresh fixed duration (75 seconds for Matching polls and History PollMutableState long polls, normally 30 seconds elsewhere), rather than min(configured RPC budget, caller time remaining). The TChannel client derives its wire TTL from that supplied duration. The inbound TChannel dispatcher additionally passes a context deadline capped at 60 seconds even when the inbound TTL is longer. Go preserves the parent context through downstream calls. A short client budget therefore becomes a longer downstream budget, while the context visible to the handler can disagree with the transport budget.
- Fix: thread the inbound deadline/cancellation through backend dispatch, use the remaining budget for wire TTL, and avoid an unrelated 60-second context cap. Validate decision/activity polls and waitForNewEvent history with short, normal and extended client deadlines. Tonic and the outer TChannel timeout do enforce local transport timeouts; this finding concerns downstream budget propagation, not an assertion that gRPC timeouts are absent. Whether a remote request continues after local cancellation needs a mixed-cluster trace.

### 11. CONFIRMED — quota waits block the executor and cannot observe caller cancellation

- Rust: `rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:210`, `:220`; `rust/crates/cadence-frontend/src/wrappers/ratelimited.rs:202`, `:229`.
- Go: `go/service/frontend/wrappers/ratelimited/ratelimit.go:79`, `:80`, `:84`.
- Difference: the production worker-poll policy waits synchronously with std::thread::sleep and accepts only a configured duration, with no caller context/deadline. Its CallerCancelled result variant cannot detect cancellation in this implementation. Go derives the wait context from the parent and explicitly returns the parent's error when it expires. Rust can occupy an async worker for the entire configured delay and delay timeout/cancellation observation or return throttling instead of the caller cancellation result.
- Fix: make the wait asynchronous and cancellation-aware, bounded by the parent deadline and the configured poll delay; preserve Go's cancellation-versus-throttling precedence.

### 12. CONFIRMED — live client-version/poll-delay settings become startup snapshots

- Rust: `rust/crates/cadence-frontend/src/workflow_backend.rs:1145`, `:1303`; `rust/crates/cadence-frontend/src/wrappers/ratelimited.rs:307`.
- Go: `go/service/frontend/config/config.go:177`, `:203`; `go/service/frontend/wrappers/versioncheck/api_generated.go:53`; `go/service/frontend/wrappers/ratelimited/ratelimit.go:68`.
- Difference: frontend.enableClientVersionCheck is read once while installing the backend and stored as a bool. frontend.maxWorkerPollDelay is likewise read once with no domain filter and stored as a single Duration. Go evaluates the first dynamically on calls and the second dynamically with info.Domain. Live updates are ignored, and per-domain poll-delay overrides never select their intended domain value.
- Fix: retain dynamic property readers in the wrappers and evaluate with the appropriate domain on each call, following Go's property refresh semantics. Cover runtime toggles and distinct domain filters.

### 13. CONFIRMED — a zero quota retains a one-request burst

- Rust: `rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:42`, `:49`, `:57`.
- Go: `go/common/quotas/dynamicratelimiter.go:117`, `:120`.
- Difference: burst(rps) returns max(rps,1), and new/refilled buckets use that capacity even at rps=0. Go explicitly sets burst=0 for zero RPS to cut off permits. Rust can admit an initial request through a zero-rate bucket, including a freshly created per-domain bucket, and retain a permit on a transition to zero.
- Fix: zero the capacity and available tokens when the effective limit is zero; match Go's rounding/minimum-burst behavior for nonzero fractional limits. Verify startup-zero and live nonzero-to-zero transitions.

### 14. CONFIRMED — Frontend metrics omit Go's caller and transport tags

- Rust: `rust/crates/cadence-frontend/src/wrappers/metrics_sink.rs:59`, `:76`; `rust/crates/cadence-frontend/src/handler_workflow.rs:928`.
- Go: `go/common/rpc/middleware.go:111`; `go/service/frontend/api/handler.go:293`, `:427`, `:1877`.
- Difference: the metrics sink builds operation/domain/signalName tags (plus task-list tags for applicable secondary scopes), discarding other tag keys. The handler does not add transport caller tags to that scope. Go's inbound middleware adds CallerTag(req.Caller) and TransportTag(req.Transport), and its Frontend request scopes consume those context tags. Existing metric names therefore produce different series and cannot preserve dashboards/quotas analysis split by transport and caller. This caller is the RPC transport caller, not the cadence-caller-type header.
- Fix: carry transport caller and transport identity into the metered context and preserve Go's tag keys/values at every emission, including errors and histograms.

### 15. PLAUSIBLE — production AdminService paths lack Go's request/error metric scopes

- Rust: `rust/crates/cadence-service-core/src/yarpc.rs:2596` (dispatch_with_headers Admin dispatch); `rust/crates/cadence-matching/src/grpc_admin.rs:160` and its direct handler paths.
- Go: `go/service/frontend/admin/handler.go:1517`, `:1525`; example DescribeCluster scope at `:854`.
- Difference: inspected production Admin adapters authorize and invoke handlers without the Workflow handler's metered wrapper or an equivalent Admin startRequestProfile/error wrapper. Go emits CadenceRequests, CadenceLatency, CadenceLatencyHistogram and typed error counters under each Admin scope with unknown-domain/context tags. No equivalent Admin observation was found in the core admin handlers or shared dispatch. Generic transport metrics, if emitted elsewhere, do not by themselves provide these operation scopes.
- Fix: confirm the deployed metric sink output, then install one common Admin observer on both transports with Go's scope names, timer/histogram and error classification. Marked PLAUSIBLE because the missing-emission conclusion is a negative static call-chain finding, not a captured metric scrape.

### 16. CONFIRMED — DB visibility sampling/list quota settings are registered but not applied

- Rust: `rust/crates/cadence-frontend/src/config.rs:87`, `:119`; `rust/crates/cadence-frontend/src/workflow_visibility.rs:277`, `:316`.
- Go: `go/service/frontend/service.go:94`; `go/common/persistence/client/factory.go:574`; `go/common/persistence/wrappers/sampled/visibility_manager.go:158`, `:300`.
- Difference: Rust registers system.enableVisibilitySampling and frontend.visibilityListMaxQPS but the production DB list path directly invokes list_page, without the corresponding sampled visibility wrapper. Go conditionally installs that wrapper when DB visibility sampling is enabled; list calls consume a per-domain list token and return its persistence-limit error when exhausted. The general Frontend visibility RPC quota is a separate control and does not implement this configured persistence sampling limit. This finding is conditional on enabling the Go sampling setting; it does not claim that every visibility limit is absent or that the similarly named ES property is enforced by Go.
- Fix: install equivalent per-domain DB visibility list limiting under the same enable/config conditions, preserving the persistence error and metrics. Verify a low list quota with a higher Frontend visibility RPC quota.

### 17. CONFIRMED — gRPC reset/restart reject a valid omitted run ID

- Rust: `rust/crates/cadence-matching/src/grpc.rs:1392`, `:1409`, `:1420`, `:1441`.
- Go: `go/service/frontend/api/handler.go:2494`, `:2570`; `go/service/frontend/validate/errors.go:91`.
- Difference: ResetWorkflowExecution and RestartWorkflowExecution reject an execution when run_id is empty and return InvalidArgument, "Execution is not set on request." Go uses CheckExecution, which requires workflow ID but permits an empty run ID (and checks UUID syntax only when a run ID is supplied). A request selecting the current run is rejected by Rust before the production handler; an empty workflow ID also receives the wrong validation message at this adapter layer.
- Fix: remove the divergent adapter check and use the shared Go-equivalent operation validation, preserving omitted-run semantics and distinct missing-execution/missing-workflow-ID/invalid-run-ID errors.

## P3

### 18. PLAUSIBLE — client-version parsing collapses prereleases into releases

- Rust: `rust/crates/cadence-frontend/src/wrappers/versioncheck.rs:31`, `:33`, `:74`.
- Go: `go/common/client/versionChecker.go:187`, `:208`, `:257`.
- Difference: Rust discards everything after '-' or '+' and compares a numeric triple. Go parses with hashicorp/go-version and evaluates version constraints. In particular, Rust treats uber-go 1.6.0-rc.1 exactly as 1.6.0 and enables raw-history-query support; prerelease constraint behavior can differ from the Go implementation. Invalid prerelease suffixes also become a valid numeric triple in Rust. The loss of information is confirmed; the complete Go library constraint acceptance matrix was not executed, hence PLAUSIBLE for the compatibility impact.
- Fix: use equivalent version parsing/constraint semantics for both supported-client checks and feature gates. Differentially test prereleases, malformed suffixes, shortened versions, and the exact supported upper/lower boundaries.

## Coverage and limits

Static review followed the installed Frontend hook (`rust/crates/cadence-frontend/src/workflow_backend.rs:777`) and the shared server registration (`rust/crates/cadence-matching/src/grpc.rs:2648`). The cadence-matching directory contains production shared gRPC/TChannel adapters; it is not safe to exclude these files as Matching-only test code. Workflow requests otherwise pass through `rust/crates/cadence-service-core/src/workflow_api.rs`; Admin Thrift requests continue through yarpc.rs dispatch.

Inspected WorkflowService groups: domain field conversions (excluding the already-reported validation work), workflow start/signal/cancel/terminate/reset/restart/refresh/diagnose/query/describe, asynchronous starts, decision/activity poll/respond/heartbeat including ByID, history and raw-history handling, visibility listing/count/scan/archive, task-list/semaphore calls and cluster/search-attribute responses. Inspected AdminService groups: domain-cache/cluster/shard/workflow description, dynamic configuration/isolation, replication/cross-cluster, DLQ, raw-history and task operations. Request/response mappings were compared with Go mapper/IDL definitions, including a top-level Thrift converter field-ID scan against shared.thrift/admin.thrift and checks against generated wire shapes in go/.gen. The scan is not proof of nested-field or default-value parity.

Long-poll review traced Matching poll calls, History PollMutableState for waitForNewEvent, inbound timeout handling and outbound TChannel TTL generation. Header/version review included caller metadata propagation, feature-version/raw-history checks and the production wrapper installation. Quota review distinguished global, per-domain user/worker/worker-poll/visibility/async controls from the persistence visibility sampling control. Metrics review compared scope/tag construction and dynamic-property use with the Go wrappers/configuration. Remaining adapter validation/error branches were compared where encountered; the specific reset/restart regression above is independent of the excluded typed-error-passthrough finding.

This is a static defect report, not a certification that every unlisted RPC/field/error is equivalent. No exhaustive wire differential suite, SDK/CLI run, cancellation trace, metric scrape, or mixed Go/Rust failure/restart gate was performed. In particular, generated schema coverage alone does not validate optional-versus-zero behavior, timestamp units, or whether a handler reaches the production backend. Each fix should be validated against its original Go path in CI, with exact wire fields and error messages, before claiming drop-in parity.
