# History NotifyFailoverMarkers routing (2026-11-13)

Go ref: `go/client/history/client.go` `NotifyFailoverMarkers`, `peer_resolver.go` `FromDomainID`, `common.DomainIDToHistoryShard`.

Change: new `rust/crates/cadence-history/src/wiring/assembly/failover_client.rs`:
`RoutingFailoverHistoryClient` groups tokens by the History host owning `fingerprint32(domainID) % numShards`
(ring lookup via `MembershipPeerResolver`), sends each group in parallel over gRPC `NotifyFailoverMarkers`
(history client TLS), first error wins; resolver error fails the call. Go has no local fast path, so none is kept
(`LocalFailoverHistoryClient` remains only as the fallback when no client is supplied). `env.rs` builds the
membership resolver before the coordinator and passes the routing client. Unit tests cover grouping, error, empty, ring routing, pb conversion.

## CI evidence (head 37078a71101894ea636c4565b5d8905150d46734, branch history-markerroute)
- rust-ci cadence-history: https://github.com/auenkr/cadence-rust/actions/runs/43477963957 success
- 12 history-mixed-gate combos (mysql/postgres/cassandra x grpc/tchannel x two_history true/false), each `GATE SUMMARY failed: []`:
  43479392252 43479398597 43479403436 43479409039 43479415278 43479420744 43479426110 43479431875 43479438221 43479443605 43479448559 43479454494
- frontend-mixed-gate two_cluster=true rust_history=true side=active: 43479461087, `GATE SUMMARY failed: []`
