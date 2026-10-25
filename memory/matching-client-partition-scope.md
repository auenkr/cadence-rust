# Matching scope: go/client/matching load balancers, partition-config provider, peer resolver (2026-10-25)

Decision: NO unported piece is needed by the Matching process. Load balancers and partition-config provider are History/Frontend-only (defer). The peer-resolver role Matching needs is covered by Rust `matching_membership` ring. No code changed, so no CI run.

## Go proof
- Wiring is only in `go/client/clientfactory.go:163-179` (`NewPeerResolver`, `NewPartitionConfigProvider`, `NewLoadBalancer`, RoundRobin, Weighted, Isolation, `NewMultiLoadBalancer`, `NewClient`). Matching service holds only a `matching.Client` (`tasklist/forwarder.go:45`, `adaptive_scaler.go:57`, `task_list_manager.go:97`, `handler/engine.go:102`).
- Matching's own calls through that client: forwarder Add/Poll/Query (`forwarder.go:141,155,197,233,251`) and adaptive scaler `DescribeTaskList` (`adaptive_scaler.go:364`).
- Forwarded calls always set `ForwardedFrom` (`forwarder.go:151,166,204,243`). `multiLoadBalancer.canRedirectToPartition` requires `ForwardedFrom == ""` (`go/client/matching/multi_loadbalancer.go:125-127`) so the load balancer returns the task list name unchanged for them (`:66-68`). `DescribeTaskList` never touches the balancer (`client.go:281-296`). Hence Matching-originated traffic uses only `peerResolver.FromTaskList` (`peer_resolver.go:60-68`: ring `Lookup(service.Matching, taskListName)` then named-port address).
- `partitionConfigProvider` is used by the client only to pick partition counts and to Update/Invalidate cache on responses (`client.go:43,48,80,85`); Matching keeps its own partition config manager in the task list manager.

## Rust status
- Ring owner lookup of a task list: `rust/crates/cadence-matching/src/matching_membership.rs:121` (`owner`), `matching_shard_routing.rs:210,226` (`owners_n`, `owner_by_address`), used for peer forwarding in `grpc_matching.rs:1043`. Parent-partition forwarding logic: `matching_dispatch.rs` (`forward_poll`, `forward_backlog_task`, `forward_query_poll`).
- `matching_history_peer_resolver.rs` is the History ring (Matching->History calls), different from Go matching `PeerResolver`.
- No separate Rust client crate exists (`ls rust/crates | grep -i client` empty).

## Deferred to History/Frontend migration
LoadBalancer (default/RR/weighted/isolation/multi) + `PartitionConfigProvider` + `FromSemaphoreBucket`/`GetAllPeers` consumers on the caller side: History (AddTask after decisions/activities scheduled, query) and Frontend (Poll, Query, ListTaskListPartitions). Port these as a client crate when History starts.

## Residual unknowns (not code gaps)
- `GetAllPeers`/`FromSemaphoreBucket` usage by Matching (semaphore ring) was not traced here; grep `FromSemaphoreBucket` callers in Go before declaring semaphore routing complete.
- Go `multiLoadBalancer` is not needed for Matching, but mixed-cluster tests (Go History/Frontend -> Rust Matching) exercise it from the Go side already.
