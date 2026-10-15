# Service definitions and implementation boundaries

Upstream protobuf service inventory: **13 services, 164 methods**. Exact methods
and adapter counts are generated in `migration-inventory.md`.

| Go API | Definition source | Rust listener code | Remaining boundary |
| --- | --- | --- | --- |
| Domain/Meta/Workflow/Visibility/Worker APIs | `go/idls/proto/uber/cadence/api/v1/service_*.proto` | frontend gRPC 7833 | Adapters and engine behavior remain partial |
| Schedule/Semaphore APIs | same public IDLs | not registered | No concrete adapters yet |
| AdminAPI | `go/idls/proto/uber/cadence/admin/v1/service.proto` | not registered | 39 methods unported |
| HistoryAPI | `go/proto/internal/uber/cadence/history/v1/service.proto` | internal history gRPC 7834 | 14/44 adapters; shard, branch, replication and advanced metadata incomplete |
| MatchingAPI | `go/proto/internal/uber/cadence/matching/v1/service.proto` | internal matching gRPC 7835 | 4/13 adapters; durable queues, task transfer, partitions and query dispatch incomplete |
| ShardDistributorAPI | `rust/proto/shard-manager/.../service.proto` | not registered | 9 methods and ownership state unported |
| ShardDistributorExecutorAPI | `rust/proto/shard-manager/.../executor.proto` | not registered | Heartbeat/executor ownership unported |
| ShardDistributorExecutorCanaryAPI | `rust/proto/shard-manager/.../canary.proto` | not registered | Ping/canary processing unported |

All definitions compile to Rust protobuf/tonic bindings and descriptors. A
registered service can still contain generated UNIMPLEMENTED methods. Reflection
advertises only services actually registered on each listener.

The Go worker service is a background processing role. Public `WorkerAPI` is
hosted by frontend; its name does not prove the worker role's scanners, archival,
replication, batch workflows and other background jobs are ported. Follow the
actual Go role and IDL definitions rather than inventing a new RPC service.

Current listeners share one in-memory execution engine in one process. Separate
ports do not establish distributed service independence. Shared production
persistence, internal task transfer, ownership/failover, discovery and lifecycle
are required before independent service processes can be claimed compatible.
