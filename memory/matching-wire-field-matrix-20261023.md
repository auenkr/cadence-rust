# Matching wire field matrix (2026-10-23)

Scope: every Matching RPC on gRPC (proto) and Thrift. Branch `matching-wire-fields-20261023`.
Not run locally (no cargo); verified by GitHub CI, see bottom.

## gRPC
Rust compiles the same `.proto` files as Go (`go/idls/proto`, `go/proto/internal`) in `cadence-proto/build.rs`, so field numbers and types are identical by construction. Only `TaskListStatus` and both `TaskListPartitionConfig` messages are hand-written (`cadence-proto/src/task_list_status.rs`, `task_list_partition.rs`); they use the same tags as the proto and encode explicit map values like Go gogo (unit tests exist). Go's proto mapper (`go/common/types/mapper/proto/matching.go`) is a 1:1 field copy.

| RPC | Request fields read by Rust (grpc_matching.rs) | Response fields written | Status |
|---|---|---|---|
| PollForDecisionTask | request{domain,task_list,identity,binary_checksum}, domain_id, poller_id, forwarded_from, isolation_group (~1635-1830) | all 21 fields incl. partition_config, load_balancer_hints, auto_config_hint (~1775-1818) | OK |
| PollForActivityTask | request{..task_list_metadata.max_tasks_per_second ~1945}, domain_id, poller_id, forwarded_from, isolation_group | all incl. partition_config, load_balancer_hints, auto_config_hint (~1992-2022) | OK |
| AddDecisionTask | all 8 (~2071-2289) | partition_config | OK |
| AddActivityTask | all but activityTaskDispatchInfo (~2290-2500) | partition_config | OK: Go never reads it either (only type + mappers; `grep ActivityTaskDispatchInfo go/service` empty) |
| QueryWorkflow | request (consistency level ~2553), domain_id, task_list, forwarded_from | query_result, query_rejected, partition_config | OK |
| RespondQueryTaskCompleted | request{task_token,result,worker_version_info ~3300}, domain_id, task_list, task_id | empty | OK |
| CancelOutstandingPoll | all 4 | empty | OK |
| DescribeTaskList | request (include_task_list_status ~3387), domain_id | pollers, task_list_status, partition_config, task_list (~3453-3482) | OK |
| ListTaskListPartitions | domain, task_list | both partition lists | OK |
| GetTaskListsByDomain | domain | both maps | OK |
| UpdateTaskListPartitionConfig | domain_id,task_list,task_list_type,partition_config (~2957-3090) | empty | OK |
| RefreshTaskListPartitionConfig | same (~3097-3180) | empty | OK |
| AcquireSemaphore | all 4 | outcome, token_id | not audited in depth (history-facing) |
| Health | | | OK |

## Thrift (thrift_matching.rs vs go/idls/thrift/matching.thrift, mapper/thrift/matching.go)
Update/RefreshTaskListPartitionConfig and AcquireSemaphore have no Thrift method; Go's thrift client returns BadRequest "Feature not supported on TChannel". Rust likewise has no such procedure (Unknown method). Meta::Health handled in `dispatch_inner`.

| RPC | Message | Field ids | Rust | Status |
|---|---|---|---|---|
| PollForDecisionTask req | 10 domainUUID,15 pollerID,20 pollRequest,30 forwardedFrom,40 isolationGroup | invoke() ~311 | OK |
| PollForDecisionTask resp | 10..170 (taskToken..autoConfigHint) | decision() ~216 | OK (empty `queries` map is emitted where Go nil omits; decodes the same) |
| PollForActivityTask req/resp | 10,15,20,30,40 / shared 10..180 | invoke(), activity() | OK |
| AddDecisionTask | 10,20,30,40,50,59,60,70 | ~361 | OK |
| AddActivityTask | 10..90; dispatchInfo 10..80 | ~382 | OK. Go mapper drops dispatch info field 50 (scheduledTimestamp); Rust ignores it too (parity) |
| QueryWorkflow | req 10,20,30,40; resp 10,20 | ~424 | OK |
| RespondQueryTaskCompleted | 10,20,30,40 | ~448 | OK |
| CancelOutstandingPoll | 10,20,30,40 | ~463 | OK |
| DescribeTaskList | req 10,20; resp pollers 10, status 20, taskList 30 | ~480, description() | OK |
| GetTaskListsByDomain | 10 domain; resp maps 10,20 | ~503 | OK |
| ListTaskListPartitions | 10,20; resp 10,20 | ~536 | OK |
| TaskList (shared) | 10 name, 20 kind, 30 baseName | list_out() | **GAP-1 fixed** |

## Gaps
1. GAP-1 (fixed): Thrift `TaskList` encoder always wrote field 30 baseName, even when empty. Go `FromTaskList` (mapper/thrift/shared.go:6118) sets it only when non-empty. Affects PollForDecisionTask response `WorkflowExecutionTaskList`, queries' task list, DescribeTaskList, and any task list in a thrift response. Fix: `list_out` in `thrift_matching.rs`. Test: `matching_thrift_task_list_omits_empty_base_name_like_go_mapper`.

Gap count: 1 (wire-visible, fixed). Nil-vs-empty differences that decode identically (empty bytes/maps) are noted but not changed.

Limits of this audit: nested api types (HistoryEvent attributes, Header, Payload) were checked only where Matching touches them; no differential test against a Go-encoded fixture was run.

## CI
(filled in below)
