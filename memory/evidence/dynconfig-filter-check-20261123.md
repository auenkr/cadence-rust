# Dynamic config filter parity: History and Frontend

Static comparison of every field initialized through a `dynamicproperties` getter in the Go History and Frontend config constructors. Go filter comes from the getter type. Rust filter comes from the actual typed History config accessor or dynamic-config helper call sites in `cadence-frontend`/`cadence-persistence`; the admin key registry is ignored.

Go fields examined: 330 (History 250, Frontend 80).

| Service | Go config field | Key | Go getter / filter | Rust helper / filter | Match? |
|---|---|---|---|---|---|
| History | `RPS` | `history.rps` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `MaxIDLengthWarnLimit` | `limit.maxIDWarnLength` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `DomainNameMaxLength` | `limit.domainNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `IdentityMaxLength` | `limit.identityLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `WorkflowIDMaxLength` | `limit.workflowIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SignalNameMaxLength` | `limit.signalNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `WorkflowTypeMaxLength` | `limit.workflowTypeLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `RequestIDMaxLength` | `limit.requestIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskListNameMaxLength` | `limit.taskListNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ActivityIDMaxLength` | `limit.activityIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ActivityTypeMaxLength` | `limit.activityTypeLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MarkerNameMaxLength` | `limit.markerNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TimerIDMaxLength` | `limit.timerIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SemaphoreNameMaxLength` | `limit.semaphoreNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `PersistenceMaxQPS` | `history.persistenceMaxQPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `PersistenceGlobalMaxQPS` | `history.persistenceGlobalMaxQPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ShutdownDrainDuration` | `history.shutdownDrainDuration` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `EnableVisibilitySampling` | `system.enableVisibilitySampling` | `GetBoolProperty` (none) | `prop=none; boolean=none` | yes |
| History | `EnableReadFromClosedExecutionV2` | `system.enableReadFromClosedExecutionV2` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `VisibilityOpenMaxQPS` | `history.historyVisibilityOpenMaxQPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `VisibilityClosedMaxQPS` | `history.historyVisibilityClosedMaxQPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MaxAutoResetPoints` | `history.historyMaxAutoResetPoints` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MaxDecisionStartToCloseSeconds` | `system.maxDecisionStartToCloseSeconds` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `WriteVisibilityStoreName` | `system.writeVisibilityStoreName` | `GetStringProperty` (none) | `prop=none` | yes |
| History | `EmitShardDiffLog` | `history.emitShardDiffLog` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `HistoryCacheInitialSize` | `history.cacheInitialSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `HistoryCacheMaxSize` | `history.cacheMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ExecutionCacheMaxByteSize` | `history.executionCacheMaxByteSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `HistoryCacheTTL` | `history.cacheTTL` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `EnableSizeBasedHistoryExecutionCache` | `history.enableSizeBasedHistoryExecutionCache` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EventsCacheInitialCount` | `history.eventsCacheInitialSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EventsCacheMaxCount` | `history.eventsCacheMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EventsCacheMaxSize` | `history.eventsCacheMaxSizeInBytes` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EventsCacheTTL` | `history.eventsCacheTTL` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `EventsCacheGlobalEnable` | `history.eventsCacheGlobalEnable` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EventsCacheGlobalInitialCount` | `history.eventsCacheGlobalInitialSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EventsCacheGlobalMaxCount` | `history.eventsCacheGlobalMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableSizeBasedHistoryEventCache` | `history.enableSizeBasedHistoryEventCache` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `AcquireShardInterval` | `history.acquireShardInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `AcquireShardConcurrency` | `history.acquireShardConcurrency` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `StandbyClusterDelay` | `history.standbyClusterDelay` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `StandbyTaskMissingEventsResendDelay` | `history.standbyTaskMissingEventsResendDelay` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `StandbyTaskMissingEventsDiscardDelay` | `history.standbyTaskMissingEventsDiscardDelay` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `WorkflowDeletionJitterRange` | `system.workflowDeletionJitterRange` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `DeleteHistoryEventContextTimeout` | `system.deleteHistoryEventContextTimeout` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TaskProcessRPS` | `history.taskProcessRPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskSchedulerType` | `history.taskSchedulerType` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerWorkerCount` | `history.taskSchedulerWorkerCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerQueueSize` | `history.taskSchedulerQueueSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerDispatcherCount` | `history.taskSchedulerDispatcherCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerRoundRobinWeights` | `history.taskSchedulerRoundRobinWeight` | `GetMapProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerDomainRoundRobinWeights` | `history.taskSchedulerDomainRoundRobinWeight` | `GetMapPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskSchedulerGlobalDomainRPS` | `history.taskSchedulerGlobalDomainRPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskSchedulerEnableRateLimiter` | `history.taskSchedulerEnableRateLimiter` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `TaskSchedulerEnableRateLimiterShadowMode` | `history.taskSchedulerEnableRateLimiterShadowMode` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskCriticalRetryCount` | `history.taskCriticalRetryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ActiveTaskRedispatchInterval` | `history.activeTaskRedispatchInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `StandbyTaskRedispatchInterval` | `history.standbyTaskRedispatchInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `StandbyTaskReReplicationContextTimeout` | `history.standbyTaskReReplicationContextTimeout` | `GetDurationPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `EnableDropStuckTaskByDomainID` | `history.DropStuckTaskByDomain` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `ResurrectionCheckMinDelay` | `history.resurrectionCheckMinDelay` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableHierarchicalWeightedRoundRobinTaskScheduler` | `history.enableHierarchicalWeightedRoundRobinTaskScheduler` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EnableTaskListAwareTaskSchedulerByDomain` | `history.enableTaskListAwareTaskSchedulerByDomain` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `TaskListNiceValue` | `history.taskListNiceValue` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `prop_domain_task_list=domainName+taskListName` | yes |
| History | `EnableTimerQueueV2` | `history.enableTimerQueueV2` | `GetBoolPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `EnableTransferQueueV2` | `history.enableTransferQueueV2` | `GetBoolPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `QueueMaxPendingTaskCount` | `history.queueMaxPendingTaskCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableTimerQueueV2PendingTaskCountAlert` | `history.enableTimerQueueV2PendingTaskCountAlert` | `GetBoolPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `EnableTransferQueueV2PendingTaskCountAlert` | `history.enableTransferQueueV2PendingTaskCountAlert` | `GetBoolPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `QueueCriticalPendingTaskCount` | `history.queueCriticalPendingTaskCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `QueueMaxVirtualQueueCount` | `history.queueMaxVirtualQueueCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `VirtualSliceForceAppendInterval` | `history.virtualSliceForceAppendInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorEnableSplit` | `history.queueProcessorEnableSplit` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorSplitMaxLevel` | `history.queueProcessorSplitMaxLevel` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorEnableRandomSplitByDomainID` | `history.queueProcessorEnableRandomSplitByDomainID` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `QueueProcessorRandomSplitProbability` | `history.queueProcessorRandomSplitProbability` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `QueueProcessorEnablePendingTaskSplitByDomainID` | `history.queueProcessorEnablePendingTaskSplitByDomainID` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `QueueProcessorPendingTaskSplitThreshold` | `history.queueProcessorPendingTaskSplitThreshold` | `GetMapProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorEnableStuckTaskSplitByDomainID` | `history.queueProcessorEnableStuckTaskSplitByDomainID` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `QueueProcessorStuckTaskSplitThreshold` | `history.queueProcessorStuckTaskSplitThreshold` | `GetMapProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorSplitLookAheadDurationByDomainID` | `history.queueProcessorSplitLookAheadDurationByDomainID` | `GetDurationPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `QueueProcessorPollBackoffInterval` | `history.queueProcessorPollBackoffInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorPollBackoffIntervalJitterCoefficient` | `history.queueProcessorPollBackoffIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `QueueProcessorEnablePersistQueueStates` | `history.queueProcessorEnablePersistQueueStates` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorEnableLoadQueueStates` | `history.queueProcessorEnableLoadQueueStates` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `QueueProcessorEnableGracefulSyncShutdown` | `history.queueProcessorEnableGracefulSyncShutdown` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `TimerTaskBatchSize` | `history.timerTaskBatchSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerTaskDeleteBatchSize` | `history.timerTaskDeleteBatchSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorGetFailureRetryCount` | `history.timerProcessorGetFailureRetryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCompleteTimerFailureRetryCount` | `history.timerProcessorCompleteTimerFailureRetryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorUpdateAckInterval` | `history.timerProcessorUpdateAckInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorUpdateAckIntervalJitterCoefficient` | `history.timerProcessorUpdateAckIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TimerProcessorCompleteTimerInterval` | `history.timerProcessorCompleteTimerInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorFailoverMaxStartJitterInterval` | `history.timerProcessorFailoverMaxStartJitterInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorFailoverMaxPollRPS` | `history.timerProcessorFailoverMaxPollRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorMaxPollRPS` | `history.timerProcessorMaxPollRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorMaxPollInterval` | `history.timerProcessorMaxPollInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorMaxPollIntervalJitterCoefficient` | `history.timerProcessorMaxPollIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TimerProcessorSplitQueueInterval` | `history.timerProcessorSplitQueueInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorSplitQueueIntervalJitterCoefficient` | `history.timerProcessorSplitQueueIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TimerProcessorMaxRedispatchQueueSize` | `history.timerProcessorMaxRedispatchQueueSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorMaxTimeShift` | `history.timerProcessorMaxTimeShift` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorHistoryArchivalSizeLimit` | `history.timerProcessorHistoryArchivalSizeLimit` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorArchivalTimeLimit` | `history.timerProcessorArchivalTimeLimit` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `DisableTimerFailoverQueue` | `history.disableTimerFailoverQueue` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCachedQueueReaderMode` | `history.timerProcessorCachedQueueReaderMode` | `GetStringPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `TimerProcessorCacheMaxSize` | `history.timerProcessorCacheMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCachePrefetchTriggerWindow` | `history.timerProcessorCachePrefetchTriggerWindow` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCacheTimeEvictionWindow` | `history.timerProcessorCacheTimeEvictionWindow` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCacheMinPrefetchInterval` | `history.timerProcessorCacheMinPrefetchInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TimerProcessorCachedQueueReaderShadowSampleInterval` | `history.timerProcessorCachedQueueReaderShadowSampleInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferTaskBatchSize` | `history.transferTaskBatchSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferTaskDeleteBatchSize` | `history.transferTaskDeleteBatchSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorFailoverMaxStartJitterInterval` | `history.transferProcessorFailoverMaxStartJitterInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorFailoverMaxPollRPS` | `history.transferProcessorFailoverMaxPollRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorMaxPollRPS` | `history.transferProcessorMaxPollRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorCompleteTransferFailureRetryCount` | `history.transferProcessorCompleteTransferFailureRetryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorMaxPollInterval` | `history.transferProcessorMaxPollInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorMaxPollIntervalJitterCoefficient` | `history.transferProcessorMaxPollIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TransferProcessorSplitQueueInterval` | `history.transferProcessorSplitQueueInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorSplitQueueIntervalJitterCoefficient` | `history.transferProcessorSplitQueueIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TransferProcessorUpdateAckInterval` | `history.transferProcessorUpdateAckInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorUpdateAckIntervalJitterCoefficient` | `history.transferProcessorUpdateAckIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `TransferProcessorCompleteTransferInterval` | `history.transferProcessorCompleteTransferInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorMaxRedispatchQueueSize` | `history.transferProcessorMaxRedispatchQueueSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorEnableValidator` | `history.transferProcessorEnableValidator` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorValidationInterval` | `history.transferProcessorValidationInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorVisibilityArchivalTimeLimit` | `history.transferProcessorVisibilityArchivalTimeLimit` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `DisableTransferFailoverQueue` | `history.disableTransferFailoverQueue` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorCachedQueueReaderMode` | `history.transferProcessorCachedQueueReaderMode` | `GetStringPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `TransferProcessorCacheMaxSize` | `history.transferProcessorCacheMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `TransferProcessorCachedQueueReaderShadowSampleInterval` | `history.transferProcessorCachedQueueReaderShadowSampleInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ReplicatorTaskDeleteBatchSize` | `history.replicatorTaskDeleteBatchSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicatorReadTaskMaxRetryCount` | `history.replicatorReadTaskMaxRetryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicatorProcessorFetchTasksBatchSize` | `history.replicatorTaskBatchSize` | `GetIntPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicatorProcessorMaxTaskBatchSize` | `history.replicatorMaxTaskBatchSize` | `GetIntPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicatorProcessorMinTaskBatchSize` | `history.replicatorMinTaskBatchSize` | `GetIntPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicatorProcessorBatchSizeStepCount` | `history.replicatorTaskBatchStepCount` | `GetIntPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicatorUpperLatency` | `history.replicatorUpperLatency` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ReplicatorCacheCapacity` | `history.replicatorCacheCapacity` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicatorCacheMaxSize` | `history.replicatorCacheMaxSize` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicationBudgetManagerMaxSizeBytes` | `history.replicationBudgetManagerMaxSizeBytes` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicationBudgetManagerMaxSizeCount` | `history.replicationBudgetManagerMaxSizeCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicationBudgetManagerSoftCapThreshold` | `history.replicationBudgetManagerSoftCapThreshold` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `MaximumBufferedEventsBatch` | `history.maximumBufferedEventsBatch` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `MaximumSignalsPerExecution` | `history.maximumSignalsPerExecution` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ShardUpdateMinInterval` | `history.shardUpdateMinInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ShardSyncMinInterval` | `history.shardSyncMinInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ShardSyncTimerJitterCoefficient` | `history.transferProcessorMaxPollIntervalJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `LongPollExpirationInterval` | `history.longPollExpirationInterval` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EventEncodingType` | `history.defaultEventEncoding` | `GetStringPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableParentClosePolicy` | `history.enableParentClosePolicy` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `NumParentClosePolicySystemWorkflows` | `history.numParentClosePolicySystemWorkflows` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableParentClosePolicyWorker` | `system.enableParentClosePolicyWorker` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `ParentClosePolicyThreshold` | `history.parentClosePolicyThreshold` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ParentClosePolicyBatchSize` | `history.parentClosePolicyBatchSize` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `NumArchiveSystemWorkflows` | `history.numArchiveSystemWorkflows` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ArchiveRequestRPS` | `history.archiveRequestRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ArchiveInlineHistoryRPS` | `history.archiveInlineHistoryRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ArchiveInlineHistoryGlobalRPS` | `history.archiveInlineHistoryGlobalRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ArchiveInlineVisibilityRPS` | `history.archiveInlineVisibilityRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ArchiveInlineVisibilityGlobalRPS` | `history.archiveInlineVisibilityGlobalRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `AllowArchivingIncompleteHistory` | `worker.AllowArchivingIncompleteHistory` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `BlobSizeLimitError` | `limit.blobSize.error` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `BlobSizeLimitWarn` | `limit.blobSize.warn` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `HistorySizeLimitError` | `limit.historySize.error` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `HistorySizeLimitWarn` | `limit.historySize.warn` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `HistoryCountLimitError` | `limit.historyCount.error` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `HistoryCountLimitWarn` | `limit.historyCount.warn` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `PendingActivitiesCountLimitError` | `limit.pendingActivityCount.error` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `PendingActivitiesCountLimitWarn` | `limit.pendingActivityCount.warn` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `PendingActivityValidationEnabled` | `limit.pendingActivityCount.enabled` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `ThrottledLogRPS` | `history.throttledLogRPS` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableStickyQuery` | `system.enableStickyQuery` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableQueryAttributeValidation` | `frontend.enableQueryAttributeValidation` | `GetBoolProperty` (none) | `prop=none; boolean=none` | yes |
| History | `ValidSearchAttributes` | `frontend.validSearchAttributes` | `GetMapProperty` (none) | `prop=none` | yes |
| History | `SearchAttributesNumberOfKeysLimit` | `frontend.searchAttributesNumberOfKeysLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SearchAttributesSizeOfValueLimit` | `frontend.searchAttributesSizeOfValueLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SearchAttributesTotalSizeLimit` | `frontend.searchAttributesTotalSizeLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SearchAttributesHiddenValueKeys` | `frontend.searchAttributesHiddenValueKeys` | `GetMapProperty` (none) | `prop=none` | yes |
| History | `StickyTTL` | `history.stickyTTL` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `DecisionHeartbeatTimeout` | `history.decisionHeartbeatTimeout` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableDistributedSemaphore` | `system.enableDistributedSemaphore` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `SemaphoreAcquireDefaultWaitTimeout` | `history.semaphoreAcquireDefaultWaitTimeout` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `DecisionRetryCriticalAttempts` | `history.decisionRetryCriticalAttempts` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `DecisionRetryMaxAttempts` | `history.decisionRetryMaxAttempts` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnforceDecisionTaskAttempts` | `history.enforceDecisionTaskAttempts` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `NormalDecisionScheduleToStartMaxAttempts` | `history.normalDecisionScheduleToStartMaxAttempts` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `NormalDecisionScheduleToStartTimeout` | `history.normalDecisionScheduleToStartTimeout` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ReplicationTaskFetcherParallelism` | `history.ReplicationTaskFetcherParallelism` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `ReplicationTaskFetcherAggregationInterval` | `history.ReplicationTaskFetcherAggregationInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ReplicationTaskFetcherTimerJitterCoefficient` | `history.ReplicationTaskFetcherTimerJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `ReplicationTaskFetcherErrorRetryWait` | `history.ReplicationTaskFetcherErrorRetryWait` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ReplicationTaskFetcherServiceBusyWait` | `history.ReplicationTaskFetcherServiceBusyWait` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `ReplicationTaskProcessorErrorRetryWait` | `history.ReplicationTaskProcessorErrorRetryWait` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorErrorRetryMaxAttempts` | `history.ReplicationTaskProcessorErrorRetryMaxAttempts` | `GetIntPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorErrorSecondRetryWait` | `history.ReplicationTaskProcessorErrorSecondRetryWait` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorErrorSecondRetryMaxWait` | `history.ReplicationTaskProcessorErrorSecondRetryMaxWait` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorErrorSecondRetryExpiration` | `history.ReplicationTaskProcessorErrorSecondRetryExpiration` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorNoTaskRetryWait` | `history.ReplicationTaskProcessorNoTaskInitialWait` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorCleanupInterval` | `history.ReplicationTaskProcessorCleanupInterval` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorCleanupJitterCoefficient` | `history.ReplicationTaskProcessorCleanupJitterCoefficient` | `GetFloat64PropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorStartWait` | `history.ReplicationTaskProcessorStartWait` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorStartWaitJitterCoefficient` | `history.ReplicationTaskProcessorStartWaitJitterCoefficient` | `GetFloat64PropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `ReplicationTaskProcessorHostQPS` | `history.ReplicationTaskProcessorHostQPS` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `ReplicationTaskProcessorShardQPS` | `history.ReplicationTaskProcessorShardQPS` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `ReplicationTaskGenerationQPS` | `history.ReplicationTaskGenerationQPS` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `EnableReplicationTaskGeneration` | `history.enableReplicationTaskGeneration` | `GetBoolPropertyFilteredByDomainIDAndWorkflowID` (domainID+workflowID) | `prop_domain_id_workflow_id=domainID+workflowID` | yes |
| History | `EnableRecordWorkflowExecutionUninitialized` | `history.enableRecordWorkflowExecutionUninitialized` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableCleanupOrphanedHistoryBranchOnWorkflowCreation` | `history.enableCleanupOrphanedHistoryBranchOnWorkflowCreation` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ReplicationTaskProcessorLatencyLogThreshold` | `history.ReplicationTaskProcessorLatencyLogThreshold` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `WorkflowIDExternalRPS` | `history.workflowIDExternalRPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `WorkflowIDInternalRPS` | `history.workflowIDInternalRPS` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableConsistentQuery` | `history.EnableConsistentQuery` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EnableConsistentQueryByDomain` | `history.EnableConsistentQueryByDomain` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableContextHeaderInVisibility` | `history.enableContextHeaderInVisibility` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableCrossClusterOperationsForDomain` | `history.enableCrossClusterOperations` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MaxBufferedQueryCount` | `history.MaxBufferedQueryCount` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `MutableStateChecksumGenProbability` | `history.mutableStateChecksumGenProbability` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MutableStateChecksumVerifyProbability` | `history.mutableStateChecksumVerifyProbability` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `MutableStateChecksumInvalidateBefore` | `history.mutableStateChecksumInvalidateBefore` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `EnableWorkflowTimerTaskCleanup` | `system.enableWorkflowTimerTaskCleanup` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EnableCorruptionAutoRepair` | `history.enableCorruptionAutoRepair` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `CorruptionRepairTimeout` | `history.corruptionRepairTimeout` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `RequireChecksumMatchAfterRebuildRepair` | `history.requireChecksumMatchAfterRebuildRepair` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableHistoryCorruptionCheck` | `history.enableHistoryCorruptionCheck` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `NotifyFailoverMarkerInterval` | `history.NotifyFailoverMarkerInterval` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `NotifyFailoverMarkerTimerJitterCoefficient` | `history.NotifyFailoverMarkerTimerJitterCoefficient` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `EnableGracefulFailover` | `system.enableGracefulFailover` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EnableActivityLocalDispatchByDomain` | `history.enableActivityLocalDispatchByDomain` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `ActivityMaxScheduleToStartTimeoutForRetry` | `history.activityMaxScheduleToStartTimeoutForRetry` | `GetDurationPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableDebugMode` | `system.enableDebugMode` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `EnableTaskInfoLogByDomainID` | `history.enableTaskInfoLogByDomainID` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `EnableTimerDebugLogByDomainID` | `history.enableTimerDebugLogByDomainID` | `GetBoolPropertyFilteredByDomainID` (domainID) | `prop_domain_id=domainID` | yes |
| History | `SampleLoggingRate` | `system.sampleLoggingRate` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableShardIDMetrics` | `system.enableShardIDMetrics` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `LargeShardHistorySizeMetricThreshold` | `system.largeShardHistorySizeMetricThreshold` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `LargeShardHistoryEventMetricThreshold` | `system.largeShardHistoryEventMetricThreshold` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `LargeShardHistoryBlobMetricThreshold` | `system.largeShardHistoryBlobMetricThreshold` | `GetIntProperty` (none) | `prop=none` | yes |
| History | `EnableStrongIdempotency` | `history.enableStrongIdempotency` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `EnableStrongIdempotencySanityCheck` | `history.enableStrongIdempotencySanityCheck` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `GlobalRatelimiterNewDataWeight` | `history.globalRatelimiterNewDataWeight` | `GetFloat64Property` (none) | `prop=none` | yes |
| History | `GlobalRatelimiterUpdateInterval` | `frontend.globalRatelimiterUpdateInterval` | `GetDurationProperty` (none) | `prop=none; duration=none` | yes |
| History | `GlobalRatelimiterDecayAfter` | `history.globalRatelimiterDecayAfter` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `GlobalRatelimiterGCAfter` | `history.globalRatelimiterGCAfter` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `HistoryTaskDLQMode` | `history.historyTaskDLQMode` | `GetStringPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| History | `HistoryTaskDLQProcessorInterval` | `history.historyTaskDLQProcessorInterval` | `GetDurationPropertyFilteredByShardID` (shardID) | `prop_shard=shardID` | yes |
| History | `HistoryTaskDLQProcessorFailoverJitterMaxDelay` | `history.historyTaskDLQProcessorFailoverJitterMaxDelay` | `GetDurationProperty` (none) | `prop=none` | yes |
| History | `HistoryTaskDLQProcessorEnabled` | `history.historyTaskDLQProcessorEnabled` | `GetBoolProperty` (none) | `prop=none` | yes |
| History | `HistoryTaskDLQProcessorHostConcurrency` | `history.historyTaskDLQProcessorHostConcurrency` | `GetIntProperty` (none) | `prop=none` | yes |
| Frontend | `PersistenceMaxQPS` | `frontend.persistenceMaxQPS` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `PersistenceGlobalMaxQPS` | `frontend.persistenceGlobalMaxQPS` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `VisibilityMaxPageSize` | `frontend.visibilityMaxPageSize` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableVisibilitySampling` | `system.enableVisibilitySampling` | `GetBoolProperty` (none) | `prop=none; boolean=none` | yes |
| Frontend | `EnableReadFromClosedExecutionV2` | `system.enableReadFromClosedExecutionV2` | `GetBoolProperty` (none) | `prop=none` | yes |
| Frontend | `VisibilityListMaxQPS` | `frontend.visibilityListMaxQPS` | `GetIntPropertyFilteredByDomain` (domainName) | `integer=domainName` | yes |
| Frontend | `ESVisibilityListMaxQPS` | `frontend.esVisibilityListMaxQPS` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `ReadVisibilityStoreName` | `system.readVisibilityStoreName` | `GetStringPropertyFilteredByDomain` (domainName) | `string_domain=domainName` | yes |
| Frontend | `EnableLogCustomerQueryParameter` | `system.enableLogCustomerQueryParameter` | `GetBoolPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `ESIndexMaxResultWindow` | `frontend.esIndexMaxResultWindow` | `GetIntProperty` (none) | `integer=none` | yes |
| Frontend | `HistoryMaxPageSize` | `frontend.historyMaxPageSize` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `UserRPS` | `frontend.rps` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `WorkerRPS` | `frontend.workerrps` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `VisibilityRPS` | `frontend.visibilityrps` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `AsyncRPS` | `frontend.asyncrps` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxDomainUserRPSPerInstance` | `frontend.domainrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxTaskListUserRPSPerInstance` | `frontend.tasklistrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxDomainWorkerRPSPerInstance` | `frontend.domainworkerrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxTaskListWorkerRPSPerInstance` | `frontend.tasklistworkerrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxDomainVisibilityRPSPerInstance` | `frontend.domainvisibilityrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxDomainAsyncRPSPerInstance` | `frontend.domainasyncrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxTaskListAsyncRPSPerInstance` | `frontend.tasklistasyncrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalDomainUserRPS` | `frontend.globalDomainrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalTaskListUserRPS` | `frontend.globalTaskListrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalDomainWorkerRPS` | `frontend.globalDomainWorkerrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalTaskListWorkerRPS` | `frontend.globalTaskListWorkerrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalDomainVisibilityRPS` | `frontend.globalDomainVisibilityrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalDomainAsyncRPS` | `frontend.globalDomainAsyncrps` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `GlobalTaskListAsyncRPS` | `frontend.globalTaskListAsyncrps` | `GetIntPropertyFilteredByDomainAndTaskList` (domainName+taskListName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxWorkerPollDelay` | `frontend.maxWorkerPollDelay` | `GetDurationPropertyFilteredByDomain` (domainName) | `duration=domainName` | yes |
| Frontend | `RateLimiterBypassCallerTypes` | `system.rateLimiterBypassCallerTypes` | `GetListProperty` (none) | `string_list_lenient=none` | yes |
| Frontend | `GlobalRatelimiterKeyMode` | `frontend.globalRatelimiterMode` | `GetStringPropertyFilteredByRatelimitKey` (ratelimitKey) | `string_ratelimit_key=ratelimitKey` | yes |
| Frontend | `GlobalRatelimiterUpdateInterval` | `frontend.globalRatelimiterUpdateInterval` | `GetDurationProperty` (none) | `prop=none; duration=none` | yes |
| Frontend | `MaxIDLengthWarnLimit` | `limit.maxIDWarnLength` | `GetIntProperty` (none) | `prop=none` | yes |
| Frontend | `DomainNameMaxLength` | `limit.domainNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `IdentityMaxLength` | `limit.identityLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `WorkflowIDMaxLength` | `limit.workflowIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `SignalNameMaxLength` | `limit.signalNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `WorkflowTypeMaxLength` | `limit.workflowTypeLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `RequestIDMaxLength` | `limit.requestIDLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `TaskListNameMaxLength` | `limit.taskListNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `SemaphoreNameMaxLength` | `limit.semaphoreNameLength` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `EnableAdminProtection` | `history.enableAdminProtection` | `GetBoolProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `AdminOperationToken` | `history.adminOperationToken` | `GetStringProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `DisableListVisibilityByFilter` | `frontend.disableListVisibilityByFilter` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `BlobSizeLimitError` | `limit.blobSize.error` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `BlobSizeLimitWarn` | `limit.blobSize.warn` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `ThrottledLogRPS` | `frontend.throttledLogRPS` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `ShutdownDrainDuration` | `frontend.shutdownDrainDuration` | `GetDurationProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `WarmupDuration` | `frontend.warmupDuration` | `GetDurationProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableDomainNotActiveAutoForwarding` | `system.enableDomainNotActiveAutoForwarding` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `EnableGracefulFailover` | `system.enableGracefulFailover` | `GetBoolProperty` (none) | `prop=none` | yes |
| Frontend | `DomainFailoverRefreshInterval` | `frontend.domainFailoverRefreshInterval` | `GetDurationProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `DomainFailoverRefreshTimerJitterCoefficient` | `frontend.domainFailoverRefreshTimerJitterCoefficient` | `GetFloat64Property` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableActiveClusterSelectionPolicyInStartWorkflow` | `frontend.enableActiveClusterSelectionPolicyInStartWorkflow` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `EnableClientVersionCheck` | `frontend.enableClientVersionCheck` | `GetBoolProperty` (none) | `boolean=none` | yes |
| Frontend | `EnableQueryAttributeValidation` | `frontend.enableQueryAttributeValidation` | `GetBoolProperty` (none) | `prop=none; boolean=none` | yes |
| Frontend | `ValidSearchAttributes` | `frontend.validSearchAttributes` | `GetMapProperty` (none) | `prop=none` | yes |
| Frontend | `SearchAttributesNumberOfKeysLimit` | `frontend.searchAttributesNumberOfKeysLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `SearchAttributesSizeOfValueLimit` | `frontend.searchAttributesSizeOfValueLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `SearchAttributesTotalSizeLimit` | `frontend.searchAttributesTotalSizeLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `PinotOptimizedQueryColumns` | `frontend.pinotOptimizedQueryColumns` | `GetMapProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `VisibilityArchivalQueryMaxPageSize` | `frontend.visibilityArchivalQueryMaxPageSize` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `DisallowQuery` | `system.disallowQuery` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `SendRawWorkflowHistory` | `frontend.sendRawWorkflowHistory` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `DecisionResultCountLimit` | `frontend.decisionResultCountLimit` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EmitSignalNameMetricsTag` | `frontend.emitSignalNameMetricsTag` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `Lockdown` | `system.Lockdown` | `GetBoolPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableTasklistIsolation` | `system.enableTasklistIsolation` | `GetBoolPropertyFilteredByDomain` (domainName) | `boolean=domainName` | yes |
| Frontend | `EnableDomainAuditLogging` | `system.enableDomainAuditLogging` | `GetBoolProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableScheduler` | `worker.enableScheduler` | `GetBoolPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableDistributedSemaphore` | `system.enableDistributedSemaphore` | `GetBoolPropertyFilteredByDomain` (domainName) | `prop_domain=domainName` | yes |
| Frontend | `MaxBadBinaryCount` | `frontend.maxBadBinaries` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MinRetentionDays` | `system.minRetentionDays` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxRetentionDays` | `system.maxRetentionDays` | `GetIntProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `FailoverCoolDown` | `frontend.failoverCoolDown` | `GetDurationPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `RequiredDomainDataKeys` | `system.requiredDomainDataKeys` | `GetMapProperty` (none) | `map_property=none` | yes |
| Frontend | `FailoverHistoryMaxSize` | `frontend.failoverHistoryMaxSize` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `MaxFailoverTimeoutInSeconds` | `frontend.maxFailoverTimeoutInSeconds` | `GetIntPropertyFilteredByDomain` (domainName) | `no direct read located` | n/a (no direct Rust read located) |
| Frontend | `EnableDomainAuditLogging` | `system.enableDomainAuditLogging` | `GetBoolProperty` (none) | `no direct read located` | n/a (no direct Rust read located) |

Filter mismatches among fields with a Rust read site: **0**. Fields with no direct Rust read site: **44** (listed as n/a; this audit does not count key-presence gaps as filter mismatches).
