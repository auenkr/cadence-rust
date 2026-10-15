# Migration inventory

Public gRPC adapters: 57 / 57.

Internal/administrative gRPC adapters: 68 / 107.

This audit counts concrete public gRPC adapters, not complete Go behavior.
An adapter can still depend on incomplete engine behavior. Generated default
UNIMPLEMENTED stubs do not count as implemented.

## DomainAPI

Adapters present: 8 / 8.

Present: RegisterDomain, DescribeDomain, ListDomains, UpdateDomain, FailoverDomain, DeprecateDomain, DeleteDomain, ListFailoverHistory.

Unported: none.

## MetaAPI

Adapters present: 1 / 1.

Present: Health.

Unported: none.

## ScheduleAPI

Adapters present: 8 / 8.

Present: CreateSchedule, DescribeSchedule, ListSchedules, DeleteSchedule, PauseSchedule, UnpauseSchedule, BackfillSchedule, UpdateSchedule.

Unported: none.

## SemaphoreAPI

Adapters present: 1 / 1.

Present: CreateSemaphore.

Unported: none.

## VisibilityAPI

Adapters present: 7 / 7.

Present: ListWorkflowExecutions, ListOpenWorkflowExecutions, ListClosedWorkflowExecutions, ListArchivedWorkflowExecutions, ScanWorkflowExecutions, CountWorkflowExecutions, GetSearchAttributes.

Unported: none.

## WorkerAPI

Adapters present: 14 / 14.

Present: PollForDecisionTask, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, PollForActivityTask, RespondActivityTaskCompleted, RespondActivityTaskCompletedByID, RespondActivityTaskFailed, RespondActivityTaskFailedByID, RespondActivityTaskCanceled, RespondActivityTaskCanceledByID, RecordActivityTaskHeartbeat, RecordActivityTaskHeartbeatByID, RespondQueryTaskCompleted, ResetStickyTaskList.

Unported: none.

## WorkflowAPI

Adapters present: 18 / 18.

Present: RestartWorkflowExecution, StartWorkflowExecution, StartWorkflowExecutionAsync, SignalWorkflowExecution, SignalWithStartWorkflowExecution, SignalWithStartWorkflowExecutionAsync, ResetWorkflowExecution, RequestCancelWorkflowExecution, TerminateWorkflowExecution, DescribeWorkflowExecution, QueryWorkflow, DescribeTaskList, GetTaskListsByDomain, ListTaskListPartitions, GetClusterInfo, GetWorkflowExecutionHistory, RefreshWorkflowTasks, DiagnoseWorkflowExecution.

Unported: none.

## AdminAPI (internal/administrative)

Adapters present: 39 / 39.

Definitions: go/idls/proto/uber/cadence/admin/v1/service.proto.

Present: DescribeWorkflowExecution, DescribeHistoryHost, DescribeShardDistribution, CloseShard, RemoveTask, ResetQueue, DescribeQueue, GetWorkflowExecutionRawHistoryV2, GetReplicationMessages, GetDLQReplicationMessages, GetDomainReplicationMessages, ReapplyEvents, AddSearchAttribute, DescribeCluster, CountDLQMessages, ReadDLQMessages, PurgeDLQMessages, MergeDLQMessages, RefreshWorkflowTasks, ResendReplicationTasks, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, GetDynamicConfig, UpdateDynamicConfig, RestoreDynamicConfig, ListDynamicConfig, GetOperationalDynamicConfig, UpdateOperationalDynamicConfig, RestoreOperationalDynamicConfig, ListOperationalDynamicConfig, DeleteWorkflow, MaintainCorruptWorkflow, GetGlobalIsolationGroups, UpdateGlobalIsolationGroups, GetDomainIsolationGroups, UpdateDomainIsolationGroups, GetDomainAsyncWorkflowConfiguraton, UpdateDomainAsyncWorkflowConfiguraton, UpdateTaskListPartitionConfig.

Unported: none.

## HistoryAPI (internal/administrative)

Adapters present: 16 / 44.

Definitions: go/proto/internal/uber/cadence/history/v1/service.proto.

Present: StartWorkflowExecution, SignalWorkflowExecution, SignalWithStartWorkflowExecution, ResetWorkflowExecution, TerminateWorkflowExecution, DescribeWorkflowExecution, QueryWorkflow, ResetStickyTaskList, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, RespondActivityTaskCompleted, RespondActivityTaskFailed, RespondActivityTaskCanceled, RecordActivityTaskHeartbeat, RequestCancelWorkflowExecution, RefreshWorkflowTasks.

Unported: GetMutableState, PollMutableState, RecordDecisionTaskStarted, RecordActivityTaskStarted, RemoveSignalMutableState, ScheduleDecisionTask, RecordChildExecutionCompleted, ReplicateEventsV2, SyncShardStatus, SyncActivity, DescribeMutableState, DescribeHistoryHost, CloseShard, RemoveTask, ResetQueue, DescribeQueue, GetReplicationMessages, GetDLQReplicationMessages, ReapplyEvents, CountDLQMessages, ReadDLQMessages, PurgeDLQMessages, MergeDLQMessages, NotifyFailoverMarkers, GetCrossClusterTasks, RespondCrossClusterTasksCompleted, GetFailoverInfo, RatelimitUpdate.

## MatchingAPI (internal/administrative)

Adapters present: 13 / 13.

Definitions: go/proto/internal/uber/cadence/matching/v1/service.proto.

Present: PollForDecisionTask, PollForActivityTask, AddDecisionTask, AddActivityTask, QueryWorkflow, RespondQueryTaskCompleted, CancelOutstandingPoll, DescribeTaskList, ListTaskListPartitions, GetTaskListsByDomain, UpdateTaskListPartitionConfig, RefreshTaskListPartitionConfig, AcquireSemaphore.

Unported: none.

## ShardDistributorExecutorCanaryAPI (internal/administrative)

Adapters present: 0 / 1.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/canary.proto.

Present: none.

Unported: Ping.

## ShardDistributorExecutorAPI (internal/administrative)

Adapters present: 0 / 1.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/executor.proto.

Present: none.

Unported: Heartbeat.

## ShardDistributorAPI (internal/administrative)

Adapters present: 0 / 9.

Definitions: rust/proto/shard-manager/uber/cadence/sharddistributor/v1/service.proto.

Present: none.

Unported: GetShardOwner, InspectShard, GetNamespaceState, WatchNamespaceState, ListNamespaces, DrainShards, UndrainShards, GetDrainedShards, ForceResetNamespace.

## Other required work

- Internal history and matching gRPC APIs, admin APIs, shard distributor, service discovery and membership, ownership/failover, worker service internals.
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
