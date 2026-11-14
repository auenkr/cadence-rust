# Frontend AdminService metrics (Sol review #15)

Branch `frontend-admin-metrics`, code head f7a35e302.

Implementation:
- `workflow_api::AdminMetrics` seam + `AdminOutcome` (core); `WorkflowApi::admin_metrics()`.
- Thrift admin dispatch (`yarpc.rs dispatch_with_transport`): after authz, `admin_request`, then `admin_finish` with the exception class.
- gRPC admin (`cadence-matching grpc.rs AdminAuth`): same, status from `grpc-status` header.
- Frontend `SinkAdminMetrics` (`wrappers/metrics_sink.rs`): `cadence_requests`, `cadence_latency`, `cadence_latency_ns`, `cadence_errors` / `_bad_request` / `_service_busy` (EntityNotExists: none), tags `operation` (Go Admin*Scope name via `admin_scope_operation`) + `domain=_unknown_`, as Go `startRequestProfile`/`error`.
- Unit tests with capturing sink (3).

Limitations: Go's `caller`/`transport` context tags are not added on admin scopes; scope names for the Admin methods not in Go's `defs.go` fall back to the method name; Go's uncategorized-error conversion to InternalServiceError is not changed here.

CI (head f7a35e302), all success:
- cadence-frontend https://github.com/auenkr/cadence-rust/actions/runs/43598473923
- cadence-service-core https://github.com/auenkr/cadence-rust/actions/runs/43598477178
- cadence-matching https://github.com/auenkr/cadence-rust/actions/runs/43598480120
Frontend mixed gate mysql:
- https://github.com/auenkr/cadence-rust/actions/runs/43599084228 FAILED (`sdk-async-frontend-restart-no-lost-messages`, 1 message lost; async Kafka restart, unrelated to admin path, flaky)
- https://github.com/auenkr/cadence-rust/actions/runs/43600542329 success, `GATE SUMMARY failed: []`
