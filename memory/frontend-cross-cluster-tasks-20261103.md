# Frontend cross-cluster task types (2026-11-03)

Branch `frontend-redirection-xc`, head 38ddf4ce69affaef2b10f38e838a4abbf1c7c554.

## Done
- cadence-api admin.rs: CrossClusterTaskRequest/Response, TaskInfo, 5 request + 5 response attribute types, ApplyParentClosePolicy*; Respond request now carries taskResponses/fetchNewTasks (JSON "shardID", alias "shardId").
- cadence-thrift cross_cluster.rs: ToThrift/FromThrift with shared.thrift field IDs; value fields always written (Go `&t.Field`).
- cadence-proto convert/cross_cluster.rs: admin.v1 <-> API (enum +1 offset, INVALID failed causes dropped like Go ToGetTaskFailedCauseMap). completion_event only supports workflow close events (Completed/Failed/Canceled/TimedOut/Terminated); other types return Unimplemented.
- admin_replication: Get/Respond forward real payloads to History (the old Internal error on non-empty tasks is gone); yarpc thrift dispatch arms added; grpc_admin (frontend admin gRPC) uses the HTTP layer instead of empty stubs.

## Evidence
- rust-ci full workspace (clippy + tests): https://github.com/auenkr/cadence-rust/actions/runs/41622883106 success @38ddf4ce6
- Earlier failures fixed: 41620302792 (test expected string enum; API enums serialize as ints), 41621479172 (HistoryEvent thrift timestamp asymmetry).

## Note / possible pre-existing bug
HistoryEvent thrift ToThrift writes ms*1e6 but FromThrift keeps the raw ns value (convert.rs ~6136). Not changed here; worth an audit.
