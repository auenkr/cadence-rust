# Migration inventory

Public gRPC adapters: 57 / 57.

Internal/administrative gRPC adapters: 107 / 107.

This audit counts concrete public gRPC adapters, not complete Go behavior.
An adapter can still depend on incomplete engine behavior. Generated default
UNIMPLEMENTED stubs do not count as implemented.

## DomainAPI

Adapters present: 8 / 8.

Present: RegisterDomain, DescribeDomain, ListDomains, UpdateDomain, FailoverDomain, DeprecateDomain, DeleteDomain, ListFailoverHistory.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## MetaAPI

Adapters present: 1 / 1.

Present: Health.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## ScheduleAPI

Adapters present: 8 / 8.

Present: CreateSchedule, DescribeSchedule, ListSchedules, DeleteSchedule, PauseSchedule, UnpauseSchedule, BackfillSchedule, UpdateSchedule.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## SemaphoreAPI

Adapters present: 1 / 1.

Present: CreateSemaphore.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## VisibilityAPI

Adapters present: 7 / 7.

Present: ListWorkflowExecutions, ListOpenWorkflowExecutions, ListClosedWorkflowExecutions, ListArchivedWorkflowExecutions, ScanWorkflowExecutions, CountWorkflowExecutions, GetSearchAttributes.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## WorkerAPI

Adapters present: 14 / 14.

Present: PollForDecisionTask, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, PollForActivityTask, RespondActivityTaskCompleted, RespondActivityTaskCompletedByID, RespondActivityTaskFailed, RespondActivityTaskFailedByID, RespondActivityTaskCanceled, RespondActivityTaskCanceledByID, RecordActivityTaskHeartbeat, RecordActivityTaskHeartbeatByID, RespondQueryTaskCompleted, ResetStickyTaskList.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## WorkflowAPI

Adapters present: 18 / 18.

Present: RestartWorkflowExecution, StartWorkflowExecution, StartWorkflowExecutionAsync, SignalWorkflowExecution, SignalWithStartWorkflowExecution, SignalWithStartWorkflowExecutionAsync, ResetWorkflowExecution, RequestCancelWorkflowExecution, TerminateWorkflowExecution, DescribeWorkflowExecution, QueryWorkflow, DescribeTaskList, GetTaskListsByDomain, ListTaskListPartitions, GetClusterInfo, GetWorkflowExecutionHistory, RefreshWorkflowTasks, DiagnoseWorkflowExecution.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## AdminAPI (internal/administrative)

Adapters present: 39 / 39.

Definitions: go/idls/proto/uber/cadence/admin/v1/service.proto.

Present: DescribeWorkflowExecution, DescribeHistoryHost, DescribeShardDistribution, CloseShard, RemoveTask, ResetQueue, DescribeQueue, GetWorkflowExecutionRawHistoryV2, GetReplicationMessages, GetDLQReplicationMessages, GetDomainReplicationMessages, ReapplyEvents, AddSearchAttribute, DescribeCluster, CountDLQMessages, ReadDLQMessages, PurgeDLQMessages, MergeDLQMessages, RefreshWorkflowTasks, ResendReplicationTasks, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, GetDynamicConfig, UpdateDynamicConfig, RestoreDynamicConfig, ListDynamicConfig, GetOperationalDynamicConfig, UpdateOperationalDynamicConfig, RestoreOperationalDynamicConfig, ListOperationalDynamicConfig, DeleteWorkflow, MaintainCorruptWorkflow, GetGlobalIsolationGroups, UpdateGlobalIsolationGroups, GetDomainIsolationGroups, UpdateDomainIsolationGroups, GetDomainAsyncWorkflowConfiguraton, UpdateDomainAsyncWorkflowConfiguraton, UpdateTaskListPartitionConfig.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## HistoryAPI (internal/administrative)

Adapters present: 44 / 44.

Definitions: go/proto/internal/uber/cadence/history/v1/service.proto.

Present: StartWorkflowExecution, SignalWorkflowExecution, SignalWithStartWorkflowExecution, ResetWorkflowExecution, TerminateWorkflowExecution, DescribeWorkflowExecution, QueryWorkflow, ResetStickyTaskList, GetMutableState, PollMutableState, RecordDecisionTaskStarted, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, RecordActivityTaskStarted, RespondActivityTaskCompleted, RespondActivityTaskFailed, RespondActivityTaskCanceled, RecordActivityTaskHeartbeat, RequestCancelWorkflowExecution, RemoveSignalMutableState, ScheduleDecisionTask, RecordChildExecutionCompleted, ReplicateEventsV2, SyncShardStatus, SyncActivity, DescribeMutableState, DescribeHistoryHost, CloseShard, RemoveTask, ResetQueue, DescribeQueue, GetReplicationMessages, GetDLQReplicationMessages, ReapplyEvents, RefreshWorkflowTasks, CountDLQMessages, ReadDLQMessages, PurgeDLQMessages, MergeDLQMessages, NotifyFailoverMarkers, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, GetFailoverInfo, RatelimitUpdate.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## MatchingAPI (internal/administrative)

Adapters present: 13 / 13.

Definitions: go/proto/internal/uber/cadence/matching/v1/service.proto.

Present: PollForDecisionTask, PollForActivityTask, AddDecisionTask, AddActivityTask, QueryWorkflow, RespondQueryTaskCompleted, CancelOutstandingPoll, DescribeTaskList, ListTaskListPartitions, GetTaskListsByDomain, UpdateTaskListPartitionConfig, RefreshTaskListPartitionConfig, AcquireSemaphore.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## ShardDistributorExecutorCanaryAPI (internal/administrative)

Adapters present: 1 / 1.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/canary.proto.

Present: Ping.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## ShardDistributorExecutorAPI (internal/administrative)

Adapters present: 1 / 1.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/executor.proto.

Present: Heartbeat.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## ShardDistributorAPI (internal/administrative)

Adapters present: 9 / 9.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/service.proto.

Present: GetShardOwner, InspectShard, GetNamespaceState, WatchNamespaceState, ListNamespaces, DrainShards, UndrainShards, GetDrainedShards, ForceResetNamespace.

RPC adapter gaps: none. Behavioral parity is incomplete; see the required work below.

## Other required work

- Service discovery and membership, distributed ownership/failover, worker
  service background roles (scanners, archival, replication, batch), and
  service lifecycle/independence. All 164 RPC adapters exist, but many encode
  honest single-host semantics; distributed behavior remains unported.
- Production SQL and NoSQL persistence backends, Go schema compatibility, transactions, visibility backends, indexing queues, and crash durability. Current JSON snapshots are development persistence.
- Buffered history events and their event-ID relationships, closing decisions with unhandled events, task and workflow timeout engines, complete workflow retry/cron behavior, reset/restart, sticky task execution, local activity dispatch, and closed-workflow queries.
- Domain UUID isolation after deletion/recreation, complete global/active-active domain validation and failover, XDC and replication, archival, batch workflows, schedules, semaphores, scanners and repair/diagnostics.
- Full dynamic configuration, authentication/authorization, rate limiting, metrics and tracing, operational tooling, lifecycle and health semantics. Current service registry labels do not establish separate history/matching/worker service implementations.
- Complete unknown-field/enum compatibility and comprehensive Go/Java SDK coverage, including caching, replay and recovery. Core SDK workflows passing do not establish complete Cadence compatibility.

## Completion criteria

Every Go capability must have a Rust implementation and evidence of equivalent
behavior. All public and internal RPC methods and transports, persistence schemas
and backends, workflow histories, failure/recovery paths, SDK replay and lifecycle
behavior must be covered. The running server must require no Go Cadence process.
Until those checks pass, this rewrite remains incomplete.
