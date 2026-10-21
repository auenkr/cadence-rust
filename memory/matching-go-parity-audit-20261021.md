# Matching Go-parity audit (2026-10-21)

Read-only source audit of Go Matching (`go/service/matching`, `go/client/matching`,
`go/common/dynamicconfig`, `go/common/metrics`) against the Rust Matching
implementation.

## Scope and baseline

* Baseline: committed `HEAD` = `89cdbc9e9aec85561006d6b6c061d18c99e2473e`
  (`master`). All Rust line numbers below are from this commit.
* The root checkout `/home/ubuntu/github/auenkr/cadence-rust` had uncommitted
  edits (`AGENTS.md`, `rust/crates/cadence-server/src/lib.rs`,
  `matching_registry.rs`, `matching_runtime.rs`, untracked
  `matching_shard_processor.rs`). They were **not** read as evidence and were not
  touched. `matching_shard_processor.rs` is not in `HEAD`, so it is excluded.
* **No build, test, or Go run was performed.** Every status below is a source-level
  judgment about whether code exists on the path that serves the RPC. "implemented"
  does not mean tested, and nothing here claims SDK, mixed Go/Rust, or restart
  compatibility.

Status legend:

* `implemented` - Rust code exists on the production path for the RPC and the
  checked Go behavior is present. Still untested here.
* `partial` - Rust code exists, but a Go behavior is missing, or the RPC is served
  by a non-Matching fallback (embedded frontend or execution store) instead of
  Matching logic, or a production path is dormant.
* `missing` - no Rust counterpart found on any production path.

## Production wiring facts that drive the statuses

* Production calls `MatchingRuntime::with_durable_queues_and_domain_cache`,
  `with_durable_queues`, `in_memory_with_domain_cache`, or `in_memory`
  (`rust/crates/cadence-server/src/main.rs:790-872`). Durable queue storage is the
  SQLite-only `matching.db` path or the configured `defaultStore`
  (`main.rs:794-801`, `:822-834`).
* `Matching::new` (non-durable, `grpc_matching.rs:286-299`) is the fallback when
  `run_matching` is false (`main.rs:877-880`). In that mode the poll, add, query,
  describe, and task-list RPCs forward to the embedded frontend or execution store
  (see the per-RPC table).
* `Membership` is attached only via `Matching::with_membership`
  (`grpc_matching.rs:803-809`). The only callers are tests:
  `matching_rpc_tests.rs:2052-2053,2158-2159` and `matching_registry.rs:1795`,
  which sits inside `mod tests` (starts at `matching_registry.rs:1721`). No
  production constructor sets membership, so remote partition forwarding
  (`grpc_matching.rs:1920-1935`) and owner-host lookup
  (`grpc_matching.rs:2087-2096`) are dormant in production.
* `matching_ringpop_provider.rs`, `matching_membership_lifecycle.rs`, and
  `matching_history_membership.rs` are declared (`lib.rs:37,71,72`) but no
  production code constructs them.
* `matching_shard_distributor.rs` is declared (`lib.rs:75`) and referenced by no
  production code in `HEAD`.
* Rust parses Ringpop and `shard-distributor-matching` config
  (`rust/crates/cadence-server/src/config.rs:34-45`) but the comments state
  these are not yet wired into Matching.

## 1. Go Matching RPC table

Go proto service `MatchingAPI` has 13 RPCs
(`go/proto/internal/uber/cadence/matching/v1/*.proto:44-102`). Go Thrift
`MatchingService` has 10 methods (`go/idls/thrift/matching.thrift:142-270`). Go
handler `handler.go` also exposes `Health` (`:106`), `Start`/`Stop` (`:95,:101`),
and serves both gRPC (`go/service/matching/wrappers/grpc`) and Thrift
(`go/service/matching/wrappers/thrift`).

| # | Go RPC | Go handler / engine | Rust gRPC (`grpc_matching.rs`) | Rust Thrift (`thrift_matching.rs`) | Status | Gap / evidence |
|---|---|---|---|---|---|---|
| 1 | AddDecisionTask | `handler.go:179`; `engine.go:497` | `add_decision_task` `:1552`; durable `:1565-1662`; non-durable requeue `:1664-1687` | `:361` | partial | Durable path ports sync-match, forwarded/child-partition handling, and sticky checks (`:1578-1582`, `:1595-1650`). Remote forward dormant (no membership). Non-durable branch calls `store.requeue_decision_task` (`:1670`), not Go matcher logic. No host throttle (row 20). |
| 2 | AddActivityTask | `handler.go:146`; `engine.go:595` | `add_activity_task` `:1689`; durable `:1702-1793`; non-durable requeue `:1794-1818` (`requeue_activity_task` at `:1800`) | `:382` | partial | Same as row 1. |
| 3 | PollForDecisionTask | `handler.go:252`; `engine.go:671-886` | `poll_for_decision_task` `:1233`; durable `:1257-1360`; non-durable forward to embedded `WorkerApi` `:1361-1399` (call at `:1365`) | `:311` (budget/timeout in `:681-700`) | partial | Durable path serves queries on the poll (`:1301-1330`), handles failover release (`:1347-1357`), and cancel (`:1358`). Matcher behavior from `matcher.go` (834 lines) is only partly mirrored. Non-durable path is a frontend passthrough. |
| 4 | PollForActivityTask | `handler.go:212`; `engine.go:888-1012` | `poll_for_activity_task` `:1401`; durable `:1425-1511`; non-durable frontend passthrough `:1513-1550` (call at `:1517`) | `:335` | partial | Same as row 3. Rate applied via `poll_rate` `:1455-1461`. |
| 5 | QueryWorkflow | `handler.go:293`; `engine.go:1014-1091` | `query_workflow` `:1819`; durable offer/forward loop `:1832-1942`; non-durable frontend passthrough `:1944-1954` | `:424` | partial | Offer, forward, and deadline logic at `:1870-1941`; strong-consistency gate `:1831`; `forwarderMaxChildrenPerNode` read `:1856`. Remote hop at `:1920-1935` is dormant (no membership). |
| 6 | RespondQueryTaskCompleted | `handler.go:326`; `engine.go:1092-1108` | `respond_query_task_completed` `:2324`; durable `queries.complete` `:2368-2386`; non-durable frontend `:2387-2393` | `:448` | partial | Token/task-id check `:2337-2349`. Non-durable branch is a frontend passthrough. |
| 7 | CancelOutstandingPoll | `handler.go:354`; `engine.go:1109-1131` | `cancel_outstanding_poll` `:2293-2323` (cancels registered poll by key `:2307-2321`) | `:463` | implemented | Go gets/creates the task-list manager then `CancelPoller`. Rust is a no-op if no poll is registered. Rust rejects `TaskListType` not in {1,2} (`:2299-2301`); Go maps non-activity to decision (`engine.go:1138-1141`). Verify with a test. |
| 8 | DescribeTaskList | `handler.go:382`; `engine.go:1133-1156` | `describe_task_list` `:2395`; durable pollers/status/isolation metrics `:2408-2495`; non-durable `:2497-2514` | `:480` | partial | Rust rejects task-list type not in {1,2} (`:2409-2411`); Go defaults non-activity to decision (`engine.go:1138-1141`). Non-durable partition config read from execution store (`:2503-2508`). |
| 9 | ListTaskListPartitions | `handler.go:411`; `engine.go:1158-1200` | `list_task_list_partitions` `:2055-2107` | `:536` | partial | Enumerates write partitions (`:2077-2086`) like Go `getAllPartitions`. Owner is membership (dormant) or `$HOSTNAME` (`:2076`, `:2096`). Go uses ring ownership via `getHostInfo` (`engine.go:1313`). |
| 10 | GetTaskListsByDomain | `handler.go:441`; `engine.go:1222-1237` | `get_task_lists_by_domain` `:1956`; durable with `matchingReturnAllTaskListKinds` `:1970-1988`; non-durable `:1992-2052` | `:503` | partial | Non-durable path scans all task lists and ignores the all-kinds flag. Go filters to Normal kind unless the flag is set (`engine.go:1231-1234`). |
| 11 | UpdateTaskListPartitionConfig | `handler.go:470`; `engine.go:1239-1279` | `update_task_list_partition_config` `:2151-2229`; scaler-enabled reject `:2164-2173`; kind check `:2174-2179`; root-only `:2184-2188`; durable `:2189-2208`; non-durable `:2210-2228` | not in IDL | partial | Validation order mirrors Go. Scaler check uses the scaler provider wired at `grpc_matching.rs:785,799` (Go: per-domain `EnableAdaptiveScaler`, `engine.go:1254`). Task-list-type default differs: Rust rejects types not in {1,2} (`:2190-2192`), Go maps non-activity to decision (`engine.go:1247`). Owner routing not ported. |
| 12 | RefreshTaskListPartitionConfig | `handler.go:498`; `engine.go:1281-1311` | `refresh_task_list_partition_config` `:2231-2292`; kind `:2243-2247`; root-nil `:2249-2253`; durable `:2254-2272`; non-durable `:2274-2291` | not in IDL | partial | Same type-default difference as row 11 (`:2255-2257`). |
| 13 | AcquireSemaphore | `handler.go:526`; `engine.go:400-446` | `acquire_semaphore` `:2109-2149` | not in IDL | partial | In-memory semaphore registry with 5-minute idle TTL (`matching_semaphore.rs:29`). Enable check is process-wide `frontend.state.semaphore_enabled` (`grpc_matching.rs:2121-2125`, comment `:2126-2127`); Go's `EnableDistributedSemaphore` is per-domain (config). Membership ownership check dormant (`:2130-2132`). |
| - | Health | `handler.go:106`; `wrappers/grpc/grpc_handler.go:40` | no Matching-specific handler; shared frontend gRPC `health` at `grpc.rs:327`; HTTP `handlers.rs:219` | Meta health via yarpc `thrift_matching.rs:670` | partial | Go Health waits for start (`startWG`). Rust has no Matching-scoped readiness gate. |
| - | Start/Stop lifecycle | `handler.go:95,101`; `engine.go:201-219` | `MatchingRuntime` owner: `matching_runtime.rs:53` (in_memory), `:68` (durable), shutdown join `:183-222` | n/a | partial | No membership, shard-distributor, or domain-change lifecycle (`engine.go:1707-1762`). |
| - | Domain change / failover callbacks | `engine.go:1707-1762` | failover subscription `grpc_matching.rs:1242,1410`; blocked poll release `:1348-1350,:1500-1502` | n/a | partial | Only failover-triggered poll release is ported. `domainChangeCallback` is not. |
| - | Shard ownership check | `engine.go:1600-1668` (`errIfShardOwnershipLost`) | none on production path | n/a | missing | Ownership helper exists only via `Membership` (`matching_membership.rs:57-68,91`), which production never attaches. |
| - | Shard-distributor executor | `engine.go:221-300` | `matching_shard_distributor.rs` declared (`lib.rs:75`) but unreferenced by production | n/a | missing | Its `Err(unimplemented("unused"))` stubs (`matching_shard_distributor.rs:1163-1218`) are inside `mod tests`. |
| - | Handler host rate limiter | `handler.go:54` (`errMatchingHostThrottle` "Matching host rps exceeded"), `:68-80` (userRateLimiter, workerRateLimiter). Enforced (`return ServiceBusy` if `!ok`): `workerRateLimiter` on AddActivity `:170`, AddDecision `:203`, PollActivity `:236`, PollDecision `:276`; `userRateLimiter` on Query `:317`, Describe `:402`, ListPartitions `:432`, GetByDomain `:462`, UpdatePartitionConfig `:490`, Acquire `:546`. Called but result ignored: `workerRateLimiter` on Respond `:347` and Cancel `:373`; `userRateLimiter` on Refresh `:519`. | none (`grep` for "host rps" finds nothing in `rust/crates`) | n/a | missing | No `matching.rps`, `workerrps`, `domainrps`, `domainworkerrps` read. Ignored-result calls are not throttles in Go and must not be ported as enforcement. |
| - | Event / task-info logging | `engine.go:1588-1598` (`emitInfoOrDebugLog`), `MatchingEnableTaskInfoLogByDomainID` | none | n/a | missing | No Rust equivalent (`grep` for info_log / event_log empty in matching). |

Transport coverage at dispatch level: all 10 Thrift IDL methods have match arms in
`thrift_matching.rs:311,335,361,382,424,448,463,480,503,536` (`invoke` at `:304`),
and all 13 proto RPCs have `grpc_matching.rs` handlers listed above. Dispatch
presence is not behavioral parity.

## 2. Task-list subsystem (Go `tasklist/`, `semaphore/`, `poller/`, `liveness/`)

| Go component (lines) | Rust counterpart | Status | Note |
|---|---|---|---|
| `task_list_manager.go` (1244) / `matcher.go` (834) | `matching_dispatch.rs` (1345), `matching_registry.rs` (2263), `matching_query.rs` (546), `matching_sync.rs` (468) | partial | Sync match, query offer, and poll lifecycle present. Matcher parity not verified line-by-line. |
| `forwarder.go` (315) | `matching_dispatch.rs:57-94` (`ForwardPoll`), `matching_forward.rs` (105, token and rate primitives), `matching_registry.rs:115-219` | partial | Forward targets go through local queues; cross-node hop needs membership (dormant). |
| `task_reader.go` (578) | `matching_reader.rs` (1083) | partial | Present; read-range and async dispatch timeout keys read (`grpc_matching.rs:505-511`). |
| `task_writer.go` (344) | `matching_writer.rs` (839) | partial | Writer uses live config for some keys (`grpc_matching.rs:793`). `outstandingTaskAppendsThreshold` and `appendTaskTimeout` see `memory/matching-production-gap-audit-20261018.md`. |
| `task_gc.go` (101) | `cadence-persistence/src/matching_gc.rs` (run/ack-level), `matching_queue.rs` `TaskGarbageCollector` | partial | `matching.maxTaskDeleteBatchSize` not read live. |
| `isolation_balancer.go` (425), `adaptive_scaler.go` (441) | `matching_scaler.rs` (722, header cites both Go files) | partial | Logic present. Production scaler provider is wired (`grpc_matching.rs:580-799`). |
| `shard_processor.go` (115) | none in `HEAD` (`matching_shard_processor.rs` is untracked in the root checkout) | missing | Do not count the uncommitted file as evidence. |
| `db.go` (241), `persistence.TaskManager` (`data_store_interfaces.go:59-77`) | `cadence-persistence/src/matching_sqlite.rs` (lease `:243`, update `:279`, create `:301`, get `:330`, size `:365`, complete `:370-375`, delete `:387`) | partial | SQLite only (see section 4). |
| `semaphore/` (semaphore_manager/registry) | `matching_semaphore.rs` (611) | partial | In-memory; see row 13. |
| `poller/manager.go` | `matching_dispatch.rs` `PollLifecycle` | partial | Poller history present (`grpc_matching.rs:1462-1470`, describe `:2419-2433`). |
| `liveness/liveness.go` | idle-tasklist check key only (`matching.idleTasklistCheckInterval`, `grpc_matching.rs` `with_idle_tasklist_check_interval` `:791`) | partial | `matching.maxTasklistIdleTime` not read. |
| `event/logger.go` | none | missing | See row "Event / task-info logging". |

## 3. Dynamic config keys

Go matching-scoped literal keys in `go/`: **58** `matching.*` strings
(`KeyName` values in `go/common/dynamicconfig/dynamicproperties/constants.go`).
Rust literal reads: **44** `matching.*` strings across `rust/crates`
(`matching.db` is a SQLite file name at `main.rs:823`, not a dynamic key).
Also read by Go matching (non-`matching.*`): `system.enableTasklistIsolation`,
`system.allIsolationGroups`, `system.enableDistributedSemaphore`,
`system.tasklistLoadBalancerStrategy`, `system.isolationGroupState*`. Rust reads the
first three (`grpc_matching.rs:677,700,2449`;
`system.enableDistributedSemaphore` is global-only at `grpc_matching.rs:2126-2127`).

### Go matching keys with no Rust reader

| Go key | Go constant / use site | Rust | Effect of absence |
|---|---|---|---|
| `matching.rps` | `MatchingUserRPS`, `config.go` `UserRPS`; `handler.go:68` | missing | Host user RPS throttle not enforced (row "Handler host rate limiter"). |
| `matching.workerrps` | `MatchingWorkerRPS`; `handler.go:76` | missing | Same. |
| `matching.domainrps` | `MatchingDomainUserRPS`; `config.go` `DomainUserRPS` | missing | Same, per-domain. |
| `matching.domainworkerrps` | `MatchingDomainWorkerRPS`; `config.go` `DomainWorkerRPS` | missing | Same, per-domain. |
| `matching.throttledLogRPS` | `MatchingThrottledLogRPS`; `config.go` `ThrottledLogRPS` | missing | No throttled-log emission. |
| `matching.persistenceMaxQPS` | `MatchingPersistenceMaxQPS`; `config.go:34` | missing | Persistence QPS limiter not read. |
| `matching.persistenceGlobalMaxQPS` | `MatchingPersistenceGlobalMaxQPS`; `config.go:35` | missing | Same, global. |
| `matching.enableSyncMatch` | `MatchingEnableSyncMatch`; `config.go` `EnableSyncMatch` | missing | Rust always attempts sync match (`grpc_matching.rs:1059` `sync_match_chain`). Go gates it. |
| `matching.activityTaskSyncMatchWaitTime` | `MatchingActivityTaskSyncMatchWaitTime`; `config.go` | missing | Sync-match wait not dynamic. |
| `matching.maxTasklistIdleTime` | `MaxTasklistIdleTime`; `config.go` `MaxTasklistIdleTime` | missing | Idle GC threshold not dynamic. |
| `matching.semaphoreIdleTime` | `MatchingSemaphoreIdleTime`; `config.go` `SemaphoreIdleTime` | missing | Fixed 5m (`matching_semaphore.rs:29`). |
| `matching.enableTaskInfoLogByDomainID` | `MatchingEnableTaskInfoLogByDomainID`; `engine.go:1588` | missing | No info logging. |
| `matching.excludeShortLivedTaskListsFromShardManager` | `MatchingExcludeShortLivedTaskListsFromShardManager`; `engine.go:1703` | missing | Shard distributor unwired. |
| `matching.percentageOnboardedToShardManager` | `MatchingPercentageOnboardedToShardManager`; used in `go/cmd/server/cadence/server.go` | missing | Server-level onboarding, not in Rust. |
| `matching.errorInjectionRate` | `MatchingErrorInjectionRate`; `go/client/clientfactory.go` | missing | Client-side error injection, not ported. |
| `system.tasklistLoadBalancerStrategy` | `TasklistLoadBalancerStrategy`; `go/client/matching/{multi,isolation}_loadbalancer.go` | missing | Go client-side load balancing, no Rust reader. |
| `system.isolationGroupStateRefreshInterval`, `…FetchTimeout`, `…UpdateTimeout`, `…UpdateRetryAttempts` | `IsolationGroupState*`; `go/common/resource/resource_impl.go` | missing | Isolation group state store timing not read. |

Go matching keys that Rust does read are listed in section 1 and the Rust
`property_values` calls (`grpc_matching.rs:505-511,1856,1979,2449-2452,677,700`).
Note that `matching.matchingReturnAllTaskListKinds` is the actual Go key name
(`constants.go`, `KeyName` for `MatchingEnableReturnAllTaskListKinds`).

## 4. Config and persistence

* Go Matching config (`go/service/matching/config/config.go`) has static fields
  `RangeSize`, `EnableDebugMode`, `HostName`, `RPCConfig`, `TaskDispatchRPS` (100000),
  `TaskDispatchRPSTTL`, `MaxTimeBetweenTaskDeletes`. Rust has `TASK_DISPATCH_RPS =
  100000.0` and TTL 60s (`matching_limiter.rs:21-23`), and the durable runtime's
  100000 argument (`main.rs:816`, `:850`, `:859`; the source labels it
  `"task id range"`). Rust reads a 100000 batch at
  `matching_dispatch.rs:311`. `MaxTimeBetweenTaskDeletes` and `EnableDebugMode` have
  no Rust reader.
* Go `go/service/matching/config` plus `development.yaml:41-52` (rpc ports 7935/7835,
  statsd metrics, pprof) vs Rust: Rust has no Matching YAML section
  (`config.rs` has `ringpop` and `shard-distributor-matching` only). Statsd and
  pprof are not wired for Matching.
* Persistence: Go's task-list DB uses `persistence.TaskManager`
  (`go/service/matching/tasklist/db.go:45,64`), which backs all configured datastores
  through the shared persistence layer. Rust's durable matching store accepts only a
  single-file SQLite `defaultStore`: `plugin_name != "sqlite"` is rejected
  (`rust/crates/cadence-server/src/matching_task_store.rs:45`), and `nShards`
  is restricted to one database (`:48`). Postgres, MySQL, and Cassandra
  task-list storage has no Rust equivalent.
* History peer for durable task dispatch is a fixed loopback endpoint
  (`main.rs:801-803`, `:837-839`) with a 1s timeout, not Go's discovery.

## 5. Metrics

Go Matching emits about 93 metric identifiers (`go/common/metrics/defs.go:3058-3154`,
`NumMatchingMetrics` at `:3154`) plus 16 Matching operation scopes
(`defs.go:1503-1538`) and forwarded/invalid-tasklist counters
(`defs.go:2520-2521`, metadata `:3476-3477`). Emission sites include
`engine.go:1553-1586` (`emitForwardedFromStats`, `emitTaskIsolationMetrics`) and
`handler.go` per-RPC scopes.

Rust Matching has **no metrics emitter** on the matching path. Searches for
`prometheus`, `metrics::`, `opentelemetry`, and `statsd` in `rust/crates` return only
struct names and a comment in `matching_registry.rs`/`matching_scaler.rs`, plus
`ringpop_provider.rs` and `cadence-proto/src/task_list_status.rs`. No Cargo dependency
for metrics exists in the crates. Status: **missing** for every Go Matching metric.

## 6. Discrepancies to verify with tests (not claimed as failures)

1. Task-list type default: Go maps any non-activity type to decision
   (`engine.go:1138-1141`, `:1247-1248`, `:1289-1290`). Rust rejects types outside
   {1,2} with InvalidArgument (`grpc_matching.rs:2299-2301`, `:2409-2411`,
   `:2190-2192`, `:2255-2257`). Relevant for `CancelOutstandingPoll`,
   `DescribeTaskList`, `UpdateTaskListPartitionConfig`, `RefreshTaskListPartitionConfig`.
2. `GetTaskListsByDomain` non-durable path ignores the all-kinds flag
   (`grpc_matching.rs:1992-2052`); Go filters Normal kind by default
   (`engine.go:1231-1234`).
3. `AcquireSemaphore` enables semaphores per process (`grpc_matching.rs:2121`), not
   per domain as Go does.
4. Rust always attempts sync match (no `matching.enableSyncMatch` gate); Go gates it.
5. Non-durable `Matching::new` path serves RPCs from frontend or execution store
   (rows 1-6, 8, 10); that is not Matching behavior and must not be counted as parity.

## 7. Related memory

* [[matching-production-gap-audit-20261018]] - earlier production gap audit (older commit).
* [[matching-persistence-production-audit-20261019]] - persistence backend findings.
* [[matching-shard-distributor-audit-20261019]] - shard-distributor gate.
* [[matching-discovery-contract]] - membership and discovery contract.
* [[matching-rpc-next]] - RPC follow-up list.
