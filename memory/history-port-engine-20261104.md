# history-port-engine checklist (2026-11-05)

Branch: `history-port-engine`; leave unmerged and undeleted.

Current fresh validation: https://github.com/auenkr/cadence-rust/actions/runs/41924902412
Head: `e6353b96f` (full SHA below); conclusion **success**.
Inputs: `clean_target=true`, `format_all=true`, no package selector.
Fresh uncached dedicated target; workspace Clippy (all targets, warnings denied)
followed by workspace tests passed. Runner-generated formatting patch applied
only to owned engine/handler/wrapper files. Final formatting/documentation commit
is also validated with the cadence-history package gate (see branch CI checks).

Source entry points for every requested non-test/non-mock engine, handler and
wrapper file are represented below. The implementation uses the requested
TODO(link) stand-ins for dependencies owned by concurrent package ports.
This is **not service completion or SDK parity evidence**: concrete dependency
adapters, transport conversion adapters and live server registration remain.
No integration tests or mixed-cluster gates were run for this task.

The function inventory below records named Rust methods and explicitly inlined
helpers. Historical continuation notes follow; the inventory supersedes their
old missing-file lists.

## Continuation checkpoint (2026-11-05)

Fetched origin and inspected existing worktree `/mnt/storage/service/wt-port-engine`;
branch already contains newer integration commits than the previous handoff summary.
Baseline CI run 41897923483 succeeded. No Cargo or Go validation ran locally.

Added `engine/notify_tasks.rs`: NotifyNewHistoryEvent, NotifyNewTransferTasks,
NotifyNewTimerTasks, including empty batches, first-task failover version resolution,
lookup failure logging and missing processor behavior. NotifyNewReplicationTasks and
hydrateReplicationTask remain unported. This is a checkpoint, not a parity claim.

Continuation commits: a391d9999 adds start validation, timeout override, reuse policy,
shouldTerminateAndStart and termination details; start orchestration remains missing.
9f8fa838e adds ReapplyEvents including filtering, reset, cron and failure mapping;
new stand-in dependency methods need adapters. DescribeWorkflowExecution and its
response mapping helpers added next; CI pending. Do not infer service parity.

## Compaction continuation checkpoint (head 62b210a92)

CI https://github.com/auenkr/cadence-rust/actions/runs/41915179732 is in progress;
no success claimed for this head. No local Cargo or Go commands ran.
Since the older checklist, source modules now cover start/signal-with-start orchestration,
describe workflow, reapply, failover callbacks, lifecycle, hydration notifications,
engine interface/factory ordering, handler workflow/admin/shard routes, replication
fanout/budget, and generic gRPC/Thrift wrappers.

Still incomplete: concrete wire conversion/dispatch adapters, runtime dependency
adapters, and exhaustive line-by-line behavior audit. TODO(link) declarations are
not evidence of implemented behavior in the dependent package. Previous table is
a historical checkpoint and must be replaced with the final audited mapping.

## Function coverage inventory (2026-11-05; audit in progress)

This inventory locates ported entry points; it is not a completed parity audit.
Dependency contracts and inline mappings require final review.

| Go file/function | Rust location |
|---|---|
| engine/describe_mutable_state.go::DescribeMutableState | engine/mutable_state_admin.rs |
| engine/describe_mutable_state.go::toMutableStateJSON | engine/mutable_state_admin.rs |
| engine/describe_queues.go::DescribeTransferQueue | engine/queue_operations.rs |
| engine/describe_queues.go::DescribeTimerQueue | engine/queue_operations.rs |
| engine/describe_queues.go::describeQueue | engine/queue_operations.rs |
| engine/describe_queues.go::serializeQueueState | engine/queue_operations.rs (QueueProcessor adapter contract) |
| engine/describe_workflow_execution.go::DescribeWorkflowExecution | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::validateDescribeWorkflowExecutionRequest | engine/describe_workflow_execution.rs (inlined domain validation) |
| engine/describe_workflow_execution.go::createDescribeWorkflowExecutionResponse | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::mapWorkflowExecutionConfiguration | engine/describe_workflow_execution.rs (inlined response construction) |
| engine/describe_workflow_execution.go::mapWorkflowExecutionInfo | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::mapPendingActivityInfo | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::mapPendingChildExecutionInfo | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::mapPendingDecisionInfo | engine/describe_workflow_execution.rs |
| engine/describe_workflow_execution.go::mapDecisionInfoToTaskList | engine/describe_workflow_execution.rs |
| engine/dlq_operations.go::CountDLQMessages | engine/dlq_operations.rs |
| engine/dlq_operations.go::ReadDLQMessages | engine/dlq_operations.rs |
| engine/dlq_operations.go::PurgeDLQMessages | engine/dlq_operations.rs |
| engine/dlq_operations.go::MergeDLQMessages | engine/dlq_operations.rs |
| engine/get_replication_messages.go::GetReplicationMessages | engine/dlq_operations.rs |
| engine/get_replication_messages.go::GetDLQReplicationMessages | engine/dlq_operations.rs |
| engine/get_replication_messages.go::convertToReplicationTask | engine/dlq_operations.rs |
| engine/history_engine.go::NewEngineWithShardContext | engine/factory.rs |
| engine/history_engine.go::Start | engine/history_engine.rs, history_engine.rs, history_engine.rs, runtime.rs |
| engine/history_engine.go::Stop | engine/history_engine.rs, history_engine.rs, history_engine.rs, runtime.rs |
| engine/history_engine.go::ScheduleDecisionTask | engine/decision_replication.rs |
| engine/history_engine.go::ReplicateEventsV2 | engine/decision_replication.rs |
| engine/history_engine.go::SyncShardStatus | engine/decision_replication.rs |
| engine/history_engine.go::SyncActivity | engine/decision_replication.rs, runtime.rs |
| engine/history_engine.go::newDomainNotActiveError | engine/deps.rs, workflow_mutations.rs |
| engine/history_engine.go::checkForHistoryCorruptions | engine/decision_replication.rs |
| engine/history_engine.go::getScheduleID | engine/workflow_mutations.rs |
| engine/history_engine.go::getActiveDomainByWorkflow | engine/workflow_mutations.rs |
| engine/history_engine.go::createShardNameFromShardID | engine/register_domain_failover_callback.rs, workflow_mutations.rs |
| engine/notify_tasks.go::NotifyNewHistoryEvent | engine/runtime.rs, notify_tasks.rs |
| engine/notify_tasks.go::NotifyNewTransferTasks | engine/notify_tasks.rs |
| engine/notify_tasks.go::NotifyNewTimerTasks | engine/notify_tasks.rs |
| engine/notify_tasks.go::NotifyNewReplicationTasks | engine/notify_tasks.rs |
| engine/notify_tasks.go::hydrateReplicationTask | engine/notify_tasks.rs |
| engine/poll_mutable_state.go::GetMutableState | engine/poll_mutable_state.rs |
| engine/poll_mutable_state.go::PollMutableState | engine/poll_mutable_state.rs |
| engine/poll_mutable_state.go::getMutableState | engine/poll_mutable_state.rs::load_mutable_state |
| engine/poll_mutable_state.go::updateEntityNotExistsErrorOnPassiveCluster | engine/poll_mutable_state.rs |
| engine/poll_mutable_state.go::getMutableStateWithCurrentVersionHistory | engine/poll_mutable_state.rs |
| engine/poll_mutable_state.go::getMutableStateOrLongPoll | engine/poll_mutable_state.rs |
| engine/poll_mutable_state.go::longPollForEventID | engine/poll_mutable_state.rs |
| engine/query_workflow.go::QueryWorkflow | engine/runtime.rs, query_workflow.rs |
| engine/query_workflow.go::queryDirectlyThroughMatching | engine/query_workflow.rs |
| engine/reapply_events.go::ReapplyEvents | engine/runtime.rs, reapply_events.rs |
| engine/record_activity_task_started.go::RecordActivityTaskStarted | engine/record_activity_task_started.rs |
| engine/record_child_execution_completed.go::RecordChildExecutionCompleted | engine/record_child_execution_completed.rs |
| engine/record_decision_task_started.go::RecordDecisionTaskStarted | engine/decision_replication.rs |
| engine/refresh_workflow_tasks.go::RefreshWorkflowTasks | engine/mutable_state_admin.rs |
| engine/register_domain_failover_callback.go::registerDomainFailoverCallback | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::domainChangeCB | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::activeClusterAttributesIn | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::notifyQueues | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::generateGracefulFailoverTasksForDomainUpdateCallback | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::lockTaskProcessingForDomainUpdate | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::unlockProcessingForDomainUpdate | engine/register_domain_failover_callback.rs |
| engine/register_domain_failover_callback.go::failoverPredicate | engine/register_domain_failover_callback.rs |
| engine/remove_signal_mutable_state.go::RemoveSignalMutableState | engine/workflow_mutations.rs |
| engine/request_cancel_workflow_execution.go::RequestCancelWorkflowExecution | engine/workflow_mutations.rs |
| engine/reset_queues.go::ResetTransferQueue | engine/queue_operations.rs |
| engine/reset_queues.go::ResetTimerQueue | engine/queue_operations.rs |
| engine/reset_sticky_tasklist.go::ResetStickyTaskList | engine/workflow_mutations.rs |
| engine/reset_workflow_execution.go::ResetWorkflowExecution | engine/reset_workflow_execution.rs |
| engine/reset_workflow_execution.go::validateResetPointForResetWorkflowExecutionRequest | engine/reset_workflow_execution.rs::validate_reset_point |
| engine/reset_workflow_execution.go::getPaginationFn | engine/reset_workflow_execution.rs::validate_reset_point (loop; HistoryReader contract) |
| engine/reset_workflow_execution.go::checkResetEventType | engine/reset_workflow_execution.rs |
| engine/respond_activity_task_canceled.go::RespondActivityTaskCanceled | engine/activity_responses.rs |
| engine/respond_activity_task_completed.go::RespondActivityTaskCompleted | engine/activity_responses.rs |
| engine/respond_activity_task_failed.go::RespondActivityTaskFailed | engine/activity_responses.rs |
| engine/respond_activity_task_heartbeat.go::RecordActivityTaskHeartbeat | engine/activity_responses.rs |
| engine/respond_decision_task_completed.go::RespondDecisionTaskCompleted | engine/decision_replication.rs |
| engine/respond_decision_task_failed.go::RespondDecisionTaskFailed | engine/decision_replication.rs |
| engine/signal_workflow_execution.go::SignalWorkflowExecution | engine/signal_workflow_execution.rs |
| engine/start_workflow_execution.go::StartWorkflowExecution | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::startWorkflowHelper | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::handleCreateWorkflowExecutionFailureCleanup | engine/start_workflow_execution.rs::cleanup_start_failure |
| engine/start_workflow_execution.go::SignalWithStartWorkflowExecution | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::getStartRequest | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::shouldTerminateAndStart | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::validateStartWorkflowExecutionRequest | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::overrideTaskStartToCloseTimeoutSeconds | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::terminateAndStartWorkflow | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::addStartEventsAndTasks | engine/start_dependencies.rs |
| engine/start_workflow_execution.go::getTerminateIfRunningDetails | engine/start_workflow_execution.rs |
| engine/start_workflow_execution.go::applyWorkflowIDReusePolicyForSigWithStart | engine/start_workflow_execution.rs (reuse helper call) |
| engine/start_workflow_execution.go::applyWorkflowIDReusePolicyHelper | engine/start_workflow_execution.rs::apply_workflow_id_reuse_policy |
| engine/start_workflow_execution.go::getWorkflowAlreadyStartedError | engine/start_workflow_execution.rs::apply_workflow_id_reuse_policy (already_started closure) |
| engine/start_workflow_execution.go::newChildContext | engine/start_dependencies.rs::child_lock_timeout + start helper timeout |
| engine/start_workflow_execution.go::createMutableState | engine/start_dependencies.rs::create_start_mutable_state |
| engine/start_workflow_execution.go::generateFirstDecisionTask | engine/start_dependencies.rs::add_start_events_and_tasks (parent guard) |
| engine/terminate_workflow_execution.go::TerminateWorkflowExecution | engine/workflow_mutations.rs |
| handler/handler.go::NewHandler | handler/runtime.rs::HistoryHandler::new (dependency construction supplied by resource adapters) |
| handler/handler.go::Start | handler/runtime.rs |
| handler/handler.go::Stop | handler/runtime.rs |
| handler/handler.go::PrepareToStop | handler/runtime.rs |
| handler/handler.go::prepareToShutDown | handler/runtime.rs::prepare_to_shut_down |
| handler/handler.go::isShuttingDown | handler/runtime.rs::is_shutting_down |
| handler/handler.go::CreateEngine | handler/runtime.rs::create_engine + engine/factory.rs |
| handler/handler.go::Health | handler/admin.rs |
| handler/handler.go::RecordActivityTaskHeartbeat | handler/workflow_routing.rs |
| handler/handler.go::RecordActivityTaskStarted | handler/workflow_routing.rs |
| handler/handler.go::RecordDecisionTaskStarted | handler/workflow_routing.rs |
| handler/handler.go::RespondActivityTaskCompleted | handler/workflow_routing.rs |
| handler/handler.go::RespondActivityTaskFailed | handler/workflow_routing.rs |
| handler/handler.go::RespondActivityTaskCanceled | handler/workflow_routing.rs |
| handler/handler.go::RespondDecisionTaskCompleted | handler/workflow_routing.rs |
| handler/handler.go::RespondDecisionTaskFailed | handler/workflow_routing.rs |
| handler/handler.go::StartWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::DescribeHistoryHost | handler/admin.rs |
| handler/handler.go::RemoveTask | handler/admin.rs |
| handler/handler.go::CloseShard | handler/admin.rs |
| handler/handler.go::ResetQueue | handler/shard_routing.rs |
| handler/handler.go::DescribeQueue | handler/shard_routing.rs |
| handler/handler.go::DescribeMutableState | handler/workflow_routing.rs |
| handler/handler.go::GetMutableState | handler/workflow_routing.rs |
| handler/handler.go::PollMutableState | handler/workflow_routing.rs |
| handler/handler.go::DescribeWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::RequestCancelWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::SignalWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::SignalWithStartWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::RemoveSignalMutableState | handler/workflow_routing.rs |
| handler/handler.go::TerminateWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::ResetWorkflowExecution | handler/workflow_routing.rs |
| handler/handler.go::QueryWorkflow | handler/workflow_routing.rs |
| handler/handler.go::ScheduleDecisionTask | handler/workflow_routing.rs |
| handler/handler.go::RecordChildExecutionCompleted | handler/workflow_routing.rs |
| handler/handler.go::ResetStickyTaskList | handler/workflow_routing.rs |
| handler/handler.go::ReplicateEventsV2 | handler/workflow_routing.rs |
| handler/handler.go::SyncShardStatus | handler/shard_routing.rs |
| handler/handler.go::SyncActivity | handler/workflow_routing.rs |
| handler/handler.go::GetReplicationMessages | handler/replication.rs, admin.rs |
| handler/handler.go::getReplicationShardMessages | handler/replication.rs |
| handler/handler.go::buildGetReplicationMessagesResponse | handler/replication.rs |
| handler/handler.go::sortReplicationShardMessages | handler/replication.rs |
| handler/handler.go::cmpReplicationShardMessages | handler/replication.rs |
| handler/handler.go::GetDLQReplicationMessages | handler/replication.rs, replication.rs, admin.rs |
| handler/handler.go::ReapplyEvents | handler/workflow_routing.rs |
| handler/handler.go::CountDLQMessages | handler/admin.rs |
| handler/handler.go::ReadDLQMessages | handler/shard_routing.rs |
| handler/handler.go::PurgeDLQMessages | handler/shard_routing.rs |
| handler/handler.go::MergeDLQMessages | handler/shard_routing.rs |
| handler/handler.go::RefreshWorkflowTasks | handler/workflow_routing.rs |
| handler/handler.go::NotifyFailoverMarkers | handler/admin.rs |
| handler/handler.go::GetCrossClusterTasks | handler/admin.rs |
| handler/handler.go::RespondCrossClusterTasksCompleted | handler/admin.rs |
| handler/handler.go::GetFailoverInfo | handler/admin.rs, admin.rs |
| handler/handler.go::RatelimitUpdate | handler/admin.rs |
| handler/handler.go::convertError | handler/runtime.rs |
| handler/handler.go::updateErrorMetric | handler/runtime.rs::error (inlined match) |
| handler/handler.go::error | handler/runtime.rs |
| handler/handler.go::emitInfoOrDebugLog | handler/runtime.rs::emit_info_or_debug_log |
| handler/handler.go::startRequestProfile | handler/runtime.rs |
| handler/handler.go::validateTaskToken | handler/runtime.rs |
| wrappers/grpc/grpc_handler.go: Register, Health | wrappers/grpc.rs; concrete mapper/dispatcher TODO(link) |
| wrappers/grpc/grpc_handler_generated.go: NewGRPCHandler, CloseShard, CountDLQMessages, DescribeHistoryHost, DescribeMutableState, DescribeQueue, DescribeWorkflowExecution, GetCrossClusterTasks, GetDLQReplicationMessages, GetFailoverInfo, GetMutableState, GetReplicationMessages, MergeDLQMessages, NotifyFailoverMarkers, PollMutableState, PurgeDLQMessages, QueryWorkflow, RatelimitUpdate, ReadDLQMessages, ReapplyEvents, RecordActivityTaskHeartbeat, RecordActivityTaskStarted, RecordChildExecutionCompleted, RecordDecisionTaskStarted, RefreshWorkflowTasks, RemoveSignalMutableState, RemoveTask, ReplicateEventsV2, RequestCancelWorkflowExecution, ResetQueue, ResetStickyTaskList, ResetWorkflowExecution, RespondActivityTaskCanceled, RespondActivityTaskCompleted, RespondActivityTaskFailed, RespondCrossClusterTasksCompleted, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, ScheduleDecisionTask, SignalWithStartWorkflowExecution, SignalWorkflowExecution, StartWorkflowExecution, SyncActivity, SyncShardStatus, TerminateWorkflowExecution | wrappers/grpc.rs; concrete mapper/dispatcher TODO(link) |
| wrappers/thrift/thrift_handler.go: NewThriftHandler, Register, Health | wrappers/thrift.rs; concrete mapper/dispatcher TODO(link) |
| wrappers/thrift/thrift_handler_generated.go: CloseShard, DescribeHistoryHost, DescribeMutableState, DescribeQueue, DescribeWorkflowExecution, GetCrossClusterTasks, GetDLQReplicationMessages, GetFailoverInfo, GetMutableState, GetReplicationMessages, MergeDLQMessages, NotifyFailoverMarkers, PollMutableState, PurgeDLQMessages, QueryWorkflow, RatelimitUpdate, ReadDLQMessages, ReapplyEvents, RecordActivityTaskHeartbeat, RecordActivityTaskStarted, RecordChildExecutionCompleted, RecordDecisionTaskStarted, RefreshWorkflowTasks, RemoveSignalMutableState, RemoveTask, ReplicateEventsV2, RequestCancelWorkflowExecution, ResetQueue, ResetStickyTaskList, ResetWorkflowExecution, RespondActivityTaskCanceled, RespondActivityTaskCompleted, RespondActivityTaskFailed, RespondCrossClusterTasksCompleted, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, ScheduleDecisionTask, SignalWithStartWorkflowExecution, SignalWorkflowExecution, StartWorkflowExecution, SyncActivity, SyncShardStatus, TerminateWorkflowExecution | wrappers/thrift.rs; concrete mapper/dispatcher TODO(link) |
| wrappers/ratelimited/handler_generated.go: NewHistoryHandler, CloseShard, CountDLQMessages, DescribeHistoryHost, DescribeMutableState, DescribeQueue, DescribeWorkflowExecution, GetCrossClusterTasks, GetDLQReplicationMessages, GetFailoverInfo, GetMutableState, GetReplicationMessages, Health, MergeDLQMessages, NotifyFailoverMarkers, PollMutableState, PrepareToStop, PurgeDLQMessages, QueryWorkflow, RatelimitUpdate, ReadDLQMessages, ReapplyEvents, RecordActivityTaskHeartbeat, RecordActivityTaskStarted, RecordChildExecutionCompleted, RecordDecisionTaskStarted, RefreshWorkflowTasks, RemoveSignalMutableState, RemoveTask, ReplicateEventsV2, RequestCancelWorkflowExecution, ResetQueue, ResetStickyTaskList, ResetWorkflowExecution, RespondActivityTaskCanceled, RespondActivityTaskCompleted, RespondActivityTaskFailed, RespondCrossClusterTasksCompleted, RespondDecisionTaskCompleted, RespondDecisionTaskFailed, ScheduleDecisionTask, SignalWithStartWorkflowExecution, SignalWorkflowExecution, Start, StartWorkflowExecution, Stop, SyncActivity, SyncShardStatus, TerminateWorkflowExecution | wrappers/ratelimited.rs; rate policy wrapper |
| wrappers/ratelimited/ratelimit.go: allowWfID | wrappers/ratelimited.rs; rate policy wrapper |

## Interface and dependency boundaries

- engine/interface.go: engine/interface.rs (Engine trait and HistoryEngine forwarding).
- handler/interface.go: handler/interface.rs and handler/implementation.rs.
- Engine dependency implementations remain in deps.rs, deps_ms.rs, runtime.rs and
  start_dependencies.rs as the requested TODO(link) stand-ins for concurrent ports.
- Wrappers expose all upstream RPC conversions and registration through mapper/dispatcher
  contracts. Concrete upstream proto/Thrift conversion adapters remain TODO(link);
  these wrappers are not registered in the running server yet.
- No mixed-cluster or integration validation was performed, as requested.

CI diagnostics: run 41916262079, head a002b7075, failed compilation (missing response
import, RefreshWorkflowTasks field, misplaced decision metric block, shard width).
Fixed in 05394d39f; validation https://github.com/auenkr/cadence-rust/actions/runs/41917112333
is pending. Applied formatting patch from that runner only to engine/handler/wrappers.
No local Cargo, rustfmt, or Go validation commands were used.

## Validation checkpoint 2026-11-05

- Failed run 41917112333 reached Clippy after successful type checking; all 153
  diagnostics were repeated needless closure borrows or mapper naming conventions.
- Fixed closure borrows and renamed mapper `from_*` methods to `encode_*`.
- Successful package run: https://github.com/auenkr/cadence-rust/actions/runs/41917909636
  at dabe8eab4; conclusion success.
- All formatting was performed on GitHub Actions; downloaded formatting patch was
  applied only to this task's engine/handler/wrappers files.
- Final validation will use `clean_target=true` with no package selector so Clippy
  precedes workspace tests in a fresh uncached CI target.

## Resumed audit checkpoint (2026-11-05)

Fetched origin and verified history-port-engine worktree. Fresh workspace run
41919069574 at a721d991a passed Clippy; tests were still running at checkpoint.
Applied its formatting patch only within owned directories. Query audit adds
Go counter and timer/histogram dependency hooks, including buffered-query
invalid states, timeouts and sticky/non-sticky dispatch. Validation pending.
No local Cargo/Go validation performed; no integration or runtime parity claimed.

YARPC status identity is now retained by HistoryError::TransportStatus; the
handler records deadline-exceeded timeout plus failure counters, and sticky
query dispatch recognizes status code 4 as a fallback condition. Confirmed
code 4 against cached upstream yarpcerrors/codes.go. Concrete adapters must
preserve this status rather than flattening it into Other.

## Fresh validation evidence (2026-11-05)

- Successful fresh workspace run: https://github.com/auenkr/cadence-rust/actions/runs/41921033898
- Full source head: `a5aa15449e2d429515b9ec125c15832721034976`.
- Command: `gh workflow run rust-ci.yml --ref history-port-engine -f clean_target=true -f format_all=true`.
- Conclusion: success; fresh dedicated target, workspace Clippy then workspace tests.
- Earlier runs 41919069574 and 41920693471 were superseded/cancelled; they are not successful evidence.
- Source checklist cross-check: all 38 requested non-test/non-mock Go files are named; 300 Go functions across these files.
- Local `git diff --check` passed. No local Cargo/rustfmt/Go builds or tests.
- No integration gates, SDK runs or binary SHA evidence: those are deferred by this source-port task.
- Query metrics now cover first-decision rejection, buffer overflow, registry errors,
  consistent query deadline, decision/direct/sticky/clear-stickiness/non-sticky latency
  timer+histogram contracts, and dispatch successes/timeouts. Runtime metrics adapters remain TODO(link).

Remaining integration work: concrete execution/persistence/queue/shard adapters,
wire type conversions and live server dispatcher registration. This source-port
inventory does not certify service or SDK parity. CountDLQMessages now retains the Go shard/source-cluster map and exposes
`count_dlq_messages_with_partial` through Handler, rate limiting and both wire
wrappers to preserve a response alongside an error. Existing Result methods remain
available; concrete wire dispatchers must select the outcome method when needed.

CountDLQ follow-up: corrected entries to HashMap<HistoryDLQCountKey, i64>
(shard ID + source cluster); shard adapters return cluster counts directly.
Collect futures in completion order to retain errgroup first-failure behavior.
Outcome methods preserve partial successful counts alongside the mapped error.

## Final source validation (including DLQ outcome correction)

- Run: https://github.com/auenkr/cadence-rust/actions/runs/41924902412
- Source head: `e6353b96fff355f4c4ebf33c743f4baa61ebb847`.
- Conclusion: **success**; fresh uncached dedicated target, workspace Clippy then workspace tests.
- Dispatch: `gh workflow run rust-ci.yml --ref history-port-engine -f clean_target=true -f format_all=true`.
- Applied its exact format-all patch only to owned modules; no local Cargo/rustfmt.
- Checklist cross-check found every engine/handler Go function name in this document.
- 41924249507 was superseded by the DLQ source correction; it is not final evidence.
- Branch stays unmerged and undeleted. Concrete dependency and transport adapters
  remain TODO(link); no integration test or SDK parity claim.
