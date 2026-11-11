# history-auditp1 (2026-11-11)

Branch `history-auditp1` (from master e50090050). Source: memory/history-parity-audit-20261111.md. Nothing was compiled locally; all validation is on GitHub Actions.

## Items
| # | Change | Notes |
|---|--------|-------|
| 2 | `replicate_workflow_execution_signaled` records the request id via `insert_signal_workflow_request`, gated on `TaskGenEnv.enable_strong_idempotency` and a non-empty id. Duplicates are kept once. | Limitation: Rust `WorkflowRequest` has no type/version, so `data` is empty. |
| 4 | Decision critical-attempt timer/histogram/warn log in `fail_decision`. Terminate log plus `WorkflowTerminateCounterPerDomain` counter. | Domain id stands in for the domain name tag. Metrics go through the new `MutableStateMetrics` trait. |
| 5 | Rust already returned Go's "replication queue state is not supported" error. Added a test and a comment. | |
| 6 | `queue_state_string.rs` reproduces Go `processingQueueStateImpl.String()`. `DescribeQueue` uses it. | |
| 8 | File-based dynamic config creation errors now fall back to an empty client, as Go dynamicconfigfx does. ConfigStore failure already fell back to NOP. | The OpenFeature arm still bails. |
| 9 | Ringpop is the only production membership. `NoMembership` is `#[cfg(test)]`. | |
| 10 | Real `EngineEventsReapplier` (port of Go `eventsReapplierImpl`). `is_resource_duplicated` backed by an applied-events set. | See remaining stubs below. |
| 15 | `wiring/server/errors.rs`: Go `FromError` gRPC codes for all mapped types, typed `api.v1` details in `google.rpc.Status`, EventAlreadyStarted handled. | See gaps below. |

## Remaining / not done
- Item 2: encode the request payload and type (needs `WorkflowRequest` type/version).
- Item 10: stubs not edited (unused in production or owned by other workers): `MissingExecutorsProvider`, `UnwiredReplicator`. TODO(link) in queue_deps*/queue_deps_standby (timer standby unsupported, timer active retry_workflow).
- Item 15 gaps:
  - `CadenceError` has no CurrentBranchChanged, InternalDataInconsistency, RemoteSyncMatched, StickyWorkerUnavailable, ReadOnlyPartition or FeatureNotEnabled. Their codes are mapped by name, but nothing produces them yet.
  - ShardOwnershipLost/RetryTaskV2 details (shared.v1) are not attached.
  - tchannel `fail` still emits only BadRequest/Internal/AccessDenied.
- OpenFeature dynamic config client.

## CI evidence
- rust-ci (package=cadence-history) on 86bcd097e: https://github.com/auenkr/cadence-rust/actions/runs/43068305858 — conclusion: success (head 86bcd097e).
- history-mixed-gate mysql/grpc/true: https://github.com/auenkr/cadence-rust/actions/runs/43069650691 — success (head 86bcd097e).
- history-mixed-gate postgres/grpc/false: https://github.com/auenkr/cadence-rust/actions/runs/43069656623 — success (head 86bcd097e).
- Code head 86bcd097e; this memory commit only adds the file.
