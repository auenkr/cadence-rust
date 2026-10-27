# Matching native Thrift extra request adapters — 2026-10-28

Parent review update: the module is registered in lib.rs. Wrong-typed scalar
wire fields skip without erasing an earlier typed struct/map. A map with bad
element types assigns nil. Metadata preserves DoubleValue presence, empty
payloads map to None, and heartbeat details use Payload rather than Vec.
Dispatch timestamps and raw WorkerVersionInfo are now implemented and tested;
unknown query completion types map to INVALID like Go. The remaining explicit
unsupported graph is AddActivityTask's scheduled HistoryEvent. These new source
corrections still need CI; no production handler uses the adapters yet.

Added isolated `thrift_matching_native_extra.rs` with native byte-valued request adapters for PollForActivityTask, AddActivityTask, QueryWorkflow, RespondQueryTaskCompleted, CancelOutstandingPoll, DescribeTaskList, ListTaskListPartitions, and GetTaskListsByDomain. It uses `GoString` directly through `FromValue`, constructs `cadence_proto::native` request models, and preserves nested optional request objects and payload bytes. The methods are not registered; parent owns registration and integration.

Field IDs were mapped from `thrift_matching.rs::dispatch_inner` and compared with the matching protobuf API/native schemas and the existing `grpc_convert.rs` conversions. Enum values use the API offset convention (wire value plus one, absent remains zero). Raw scalar strings scan backward while skipping wrong types. Struct selection uses the last occurrence, so a wrong-typed duplicate replaces an earlier valid container with absence. Partition string maps accept raw bytes and use last-wins insertion.

Tests cover request shape/byte preservation for all eight methods, including `ff` and `fe`, and correctly typed duplicate scalar replacement after a wrong-typed duplicate.

Explicit remaining graph gap: AddActivityTask dispatch info currently returns an error when a scheduled event or either timestamp field is present, because native history-event and timestamp nested conversion is not implemented in this adapter. RespondQueryTaskCompleted returns an error when worker_version_info is present. This avoids silently dropping those fields. No native response encoders are included here.

Validation is unverified: no Cargo, Go, or rustfmt commands were run locally, per task constraints. Parent CI must confirm compilation and test results before integration.
