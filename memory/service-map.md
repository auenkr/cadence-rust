# Service definitions and implementation boundaries

Upstream protobuf service inventory: **13 services, 164 methods**. Exact methods
and adapter counts are generated in `migration-inventory.md`.

| Go API | Definition source | Rust listener code | Remaining boundary |
| --- | --- | --- | --- |
| Domain/Meta/Workflow/Visibility/Worker APIs | `go/idls/proto/uber/cadence/api/v1/service_*.proto` | frontend gRPC 7833 | Adapters and engine behavior remain partial |
| Schedule/Semaphore APIs | same public IDLs | frontend gRPC 7833 | 8/8 + 1/1 adapters; cron evaluation and semaphore waiter queues remain partial |
| AdminAPI | `go/idls/proto/uber/cadence/admin/v1/service.proto` | frontend gRPC 7833 | 39/39 adapters; archival backends and distributed queue/shard ops are single-host |
| HistoryAPI | `go/proto/internal/uber/cadence/history/v1/service.proto` | internal history gRPC 7834 | 44/44 adapters; branches/versions, replication apply and distributed shards incomplete |
| MatchingAPI | `go/proto/internal/uber/cadence/matching/v1/service.proto` | internal matching gRPC 7835 | 13/13 adapters; durable queues and distributed matching incomplete |
| ShardDistributorAPI | `rust/proto/shard-manager/.../service.proto` | frontend gRPC 7833 | 9/9 adapters; etcd/leader fleet coordination is single-host memory state |
| ShardDistributorExecutorAPI | `rust/proto/shard-manager/.../executor.proto` | frontend gRPC 7833 | 1/1 adapter; assignments echo the reporter's shards, no leader rebalancer |
| ShardDistributorExecutorCanaryAPI | `rust/proto/shard-manager/.../canary.proto` | frontend gRPC 7833 | 1/1 adapter; ownership reads come from single-host state |

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
