# Independent Frontend compatibility review — 2026-11-04

Reviewed baseline: `master` at `3ace896eb`. Static inspection only, using low-priority file reads/searches. No Cargo, Go, Docker, builds, tests, or cluster runs were executed. Only this report was written. The gRPC Frontend implementation lives in `cadence-matching/src/grpc.rs`; findings there concern its public Frontend server, not migration of Matching.

## Findings

### C1 — high — gRPC FirstRunAt is converted to milliseconds instead of nanoseconds

- **Rust:** `rust/crates/cadence-proto/src/convert/mod.rs:382` (conversion); `rust/crates/cadence-frontend/src/workflow_backend.rs:611` (consumer).
- **Go:** `go/common/types/mapper/proto/api.go:3907`; `go/common/types/mapper/proto/helpers.go:71`; `go/common/util.go:539`.
- **Difference:** `start()` computes `seconds * 1000 + nanos / 1_000_000` for `first_run_at_timestamp`. Go uses `timeToUnixNano`. The production Rust backend compares this value with Unix nanoseconds and forwards the same incorrectly scaled public request. The public gRPC Start and SignalWithStart adapters both use this converter (`cadence-matching/src/grpc.rs:1070` and `:1202`).
- **Failure scenario:** An SDK requests a first run tomorrow, without a cron schedule or delay. A present-day timestamp around `1.8e18` ns becomes `1.8e12`; Rust considers it in the past and starts immediately. Go waits until the requested time. SignalWithStart and async start paths using this converter inherit the error.
- **Suggested fix:** Convert protobuf timestamps to nanoseconds with the shared checked nanosecond conversion; preserve that unit through the API model and History envelope. Cover a future timestamp with nonzero nanoseconds through the production gRPC-to-History path.

### C2 — high — decision-task scheduled/started timestamps lose a factor of one million

- **Rust:** `rust/crates/cadence-frontend/src/workflow_matching.rs:313` and `:341`; downstream `rust/crates/cadence-proto/src/convert/mod.rs:293`.
- **Go:** `go/service/frontend/api/handler.go:3019`; `go/idls/thrift/shared.thrift:1409`.
- **Difference:** Both `DecisionTask::from_matching` and `DecisionTask::from_started` divide scheduled/started timestamps by `1_000_000`. `decision_response` copies those values into the public response unchanged. Thrift expects nanoseconds, and the protobuf response explicitly uses `timestamp_nanos`; Go copies the original nanosecond values without division.
- **Failure scenario:** Every normal decision poll, and an inline decision returned by RespondDecisionTaskCompleted, reports a modern scheduled/started time near January 1970. SDK scheduling and execution latency measurements become enormous or otherwise incorrect, and timestamp precision is lost on both transports.
- **Suggested fix:** Retain nanoseconds when decoding both internal responses. Verify public Thrift values and protobuf seconds/nanos against the same internal timestamp.

### C3 — high — production gRPC errors discard typed details and payload fields

- **Rust:** `rust/crates/cadence-matching/src/grpc.rs:499` and `:93` (also forwarded schedule errors at `:472`).
- **Go:** `go/common/types/mapper/proto/errors.go:362`, `:370`, `:386`, and `:350`.
- **Difference:** The production hook converts `CadenceError` to `(name, thrift_value, message)`, discards `thrift_value`, then calls a name/message-only mapper. WorkflowExecutionAlreadyStarted, DomainNotActive, ClientVersionNotSupported and CancellationAlreadyRequested return bare statuses without Go's protobuf error details. ServiceBusy retains an empty detail but loses its reason; EntityNotExists loses its cluster fields. The richer AlreadyStarted conversion later in `start_workflow_execution` is only used by the fallback local engine, so it does not protect the deployed Frontend hook.
- **Failure scenario:** Starting an already-running workflow returns ALREADY_EXISTS without the typed AlreadyStarted detail, run ID or start request ID that existing SDK error mappers consume. A passive-cluster request similarly loses the typed DomainNotActive detail and active-cluster information. Callers receive different error objects and cannot recover using the Go payload.
- **Suggested fix:** Map the original structured `CadenceError` directly to Go-compatible `google.rpc.Status` details, preserving every field and code; share this conversion across local, production and redirected calls.

### C4 — high — bad deployment checks are never installed in the production Frontend

- **Rust:** `rust/crates/cadence-frontend/src/workflow_backend.rs:1397` (factory); `rust/crates/cadence-frontend/src/handler_workflow.rs:1285` (optional check).
- **Go:** `go/service/frontend/api/handler.go:466`, `:473`, and `:582`.
- **Difference:** `PollForDecisionTask` checks bad binaries only through optional `poll_admission`. Constructors set it to `None`; `build_workflow_api` never calls `with_poll_admission`. The only call that installs an admission implementation is in a test. Go always loads the domain and rejects a checksum present in its BadBinaries map before polling Matching.
- **Failure scenario:** An operator marks a worker checksum bad using UpdateDomain to stop a faulty deployment. After replacing Go Frontend with Rust, that worker can continue polling and receiving decisions. Go returns `BadRequestError("binary <checksum> already marked as bad deployment")`.
- **Suggested fix:** Install a production admission implementation backed by the domain manager/cache, with Go's error and bad-binary metric. Exercise UpdateDomain followed by a real worker poll, rather than only a handler supplied with a test policy.

### C5 — high — production isolation metadata, drain checks and workflow partition configuration are missing

- **Rust:** `rust/crates/cadence-frontend/src/workflow_backend.rs:776`, `:1397`, and `:461`; `rust/crates/cadence-frontend/src/handler_workflow.rs:1781`.
- **Go:** `go/common/rpc/middleware.go:212`, `:255`; `go/service/frontend/api/handler.go:537`, `:541`, `:545`, `:477`, `:1669`, and `:2289`.
- **Difference:** `HandlerApi::call` builds CallerMetadata with version/deadline/original headers but leaves `isolation_group` at its empty default. The factory installs neither `with_isolation_group_state` nor `with_poll_isolation_waiter`. Poll envelopes therefore omit the group, drained groups are not checked or waited on, and Start/SignalWithStart History envelopes never encode their IDL `partitionConfig` fields. Forwarding raw headers does not fill these context-derived request fields. Go derives context from `cadence-client-isolation-group`, or forwarded partition/group headers, and passes its partition configuration to History.
- **Failure scenario:** A worker in isolation group `blue` polls with the supported client isolation header while that group is drained. Go waits and returns an empty result if its budget expires; Rust polls Matching without the group and can receive work. A workflow started from that client also loses its intended isolation partition in Go History, allowing tasks to run in the wrong worker group.
- **Suggested fix:** Port the client and forwarded partition middleware semantics into CallerMetadata/context, encode partition maps in both History start envelopes, and install the production isolation state and poll waiter with the domain-filtered enable flag and Go's fail-open rules.

### C6 — medium — zombie-poller cancellation exists only as an uninstalled hook

- **Rust:** `rust/crates/cadence-frontend/src/workflow_backend.rs:1397`; `rust/crates/cadence-frontend/src/handler_workflow.rs:1344`.
- **Go:** `go/service/frontend/api/handler.go:364`, `:505`, and `:596`.
- **Difference:** `OutstandingPollGuard` is created only when `outstanding_poll_canceller` is present. All constructors default it to `None`, and the production factory never calls `with_outstanding_poll_canceller`; only a test installs it. Thus the separate MatchingPollCanceller implementation is unreachable from deployed worker polls. Go explicitly calls CancelOutstandingPoll when the client connection closes because its RPC stack does not propagate that cancellation to Matching.
- **Failure scenario:** A worker disconnects or restarts while a long poll is pending. Its Matching poll can remain eligible for dispatch until timeout, consuming a decision/activity task for a client that is gone and delaying another worker until task recovery. Repeated disconnects accumulate stale pollers.
- **Suggested fix:** Wire a production canceller that resolves the same Matching owner and transport as the original poll, and ensure inbound cancellation drops the guarded dispatch and sends the exact poller ID/task-list/type request.

### C7 — high — supported active-cluster selection requests return UNIMPLEMENTED on gRPC

- **Rust:** `rust/crates/cadence-proto/src/convert/mod.rs:345`; callers `rust/crates/cadence-matching/src/grpc.rs:1070` and `:1202`.
- **Go:** `go/common/types/mapper/proto/api.go:3909`, `:3543`, and `:7067`; `go/service/frontend/api/handler.go:1669`.
- **Difference:** The inbound start converter rejects any non-nil `active_cluster_selection_policy` with `UNIMPLEMENTED: Active cluster selection policy has not been ported.` Go maps and preserves this field, allowing the configured selection/redirection policy to handle it. Rejection occurs before the production Frontend hook, including when Rust's domain-filtered active-cluster-selection configuration is enabled.
- **Failure scenario:** An existing multi-cluster SDK client supplies an active-cluster selection policy to Start or SignalWithStart. The identical request works against Go but is rejected immediately after swapping the Frontend image. Async requests that reuse this start conversion are also affected.
- **Suggested fix:** Carry the selection policy in the public API models and all inbound/outbound conversions, then apply Go's configuration and redirection semantics instead of rejecting a supported wire field.

### C8 — medium — worker poll responses drop AutoConfigHint and TotalHistoryBytes

- **Rust:** `rust/crates/cadence-frontend/src/workflow_matching.rs:297`, `:516`, and `:551`; public models `rust/crates/cadence-api/src/tasks.rs:279` and `:479`; protobuf responses `rust/crates/cadence-proto/src/convert/mod.rs:284` and `:315`.
- **Go:** `go/service/frontend/api/handler.go:401`, `:2941`, `:3023`; `go/idls/thrift/shared.thrift:1413`; `go/idls/thrift/matching.thrift:57`.
- **Difference:** The Rust decision decoder/model omits Matching's TotalHistoryBytes and AutoConfigHint, an empty decision poll returns `{}`, and the activity decoder's public model also has no AutoConfigHint. Protobuf conversion fills omitted fields with defaults. Although the final JSON shaping functions allow these keys, the production typed decoders have already removed them. Go returns the hints even for an empty decision poll and forwards the byte count for real decision tasks.
- **Failure scenario:** Matching returns poller autoscaling hints and a nonzero history byte count to an existing SDK. Rust Frontend strips them, so SDK poller tuning loses its server guidance and history-size-based worker behavior observes zero instead of the real count. Both Thrift and gRPC responses differ.
- **Suggested fix:** Add the missing fields to the public models, Thrift codecs and protobuf converters; decode and forward them from Matching and preserve hints in empty polls and inline decision responses where supplied.

### C9 — medium — decision polling ignores frontend.historyMaxPageSize

- **Rust:** `rust/crates/cadence-frontend/src/workflow_matching.rs:427`; `rust/crates/cadence-frontend/src/workflow_backend.rs:1418`.
- **Go:** `go/service/frontend/api/handler.go:2983`; `go/service/frontend/config/config.go:158`.
- **Difference:** `decision_response` always calls the history reader with `HISTORY_MAX_PAGE_SIZE` (1000), ignoring the domain. The dynamic page-size policy installed by the factory applies to GetWorkflowExecutionHistory but is not consulted when assembling decision polls. Go resolves `HistoryMaxPageSize(domainName)` for these responses too.
- **Failure scenario:** A deployment sets a domain's history page size to 100 to bound poll-response size or persistence read cost. Rust still reads and returns up to 1000 events on its first decision page, potentially crossing existing transport/message limits for large events, while Go paginates at the configured limit. RespondDecisionTaskCompleted's inline decision takes the same path.
- **Suggested fix:** Resolve the domain-filtered dynamic page size when building decision responses, including inline tasks, and pass it to the history reader with Go's corresponding semantics.

## Areas checked with no defect found in the inspected behavior

- History continuation-token field names, base64 byte fields, null pointer serialization and explicit run-ID mismatch message match Go (`handler_workflow_history.rs:10`, `:57`; Go `api/handler.go:90`, `:1959`). This does not assert all history or failover paths are complete.
- History page-completeness validation preserves Go's empty-last-page behavior and the lower-bound rule for intermediate pages (`handler_workflow_history.rs:76`; Go `api/handler.go:3030`).
- Start-workflow first-decision backoff and retry-expiration construction have the corresponding delay/cron/jitter branches and millisecond rounding (`workflow_backend.rs:602`; Go `common/util.go:515`), apart from the inbound timestamp-unit defect C1.
- SQL visibility response timestamps convert stored microseconds to API nanoseconds, execution time falls back to start time, and an undecodable memo is ignored as in Go (`workflow_visibility.rs:32`; Go `common/persistence/visibility_single_manager.go:384`).
- Thrift task completion and heartbeat dispatch use the expected domain/public-request envelope fields (`workflow_backend.rs:548`; Go History IDL). The review followed these through task-token conversion rather than treating public JSON adapters as the production path.
- Production domain registration applies cluster archival policy before persistence (`cadence-service-core/src/yarpc.rs:339`); the separate `handler_domain` helper's apparent omission is not reported as a deployed registration defect.
- The apparent missing VersionHistoryItem on the TChannel PollMutableState request is also absent from the Go Thrift IDL (`go/idls/thrift/history.thrift:100`), so it is not counted as a Rust-only incompatibility.

These are source-backed findings, not runtime reproductions or a complete parity certification. Build and mixed-cluster validation remain necessary after fixes.
