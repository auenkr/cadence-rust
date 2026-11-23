# Frontend parity cross-review (Opus), 2026-11-23

Cross-review of `memory/evidence/fe-final-review-sol-20261123.md` on branch `fe-final-review-sol` (base 120ebfeb6). I only read source. No Cargo, Go or Docker commands were run.

| Verdict | Count |
| --- | ---: |
| Confirmed | 6 |
| Adjusted | 2 |
| Rejected | 0 |
| New | 0 |

## Verdicts

1. **Domain replication queue framing (High): CONFIRMED.** Go `ThriftRWEncoder.Encode` writes the preamble `0x59` and `Decode` requires it (`go/common/codec/version0_thriftrw.go:44-70`). The queue uses it for both enqueue and read (`go/common/domain/replication_queue.go:65,167`). Rust `encode_domain_replication_task` writes a bare struct (`rust/crates/cadence-persistence/src/domains.rs:261-271`). It is enqueued from `domains.rs:1939`. Reads in `rust/crates/cadence-service-core/src/admin.rs:1582` and DLQ merges at `admin.rs:1886` call `read_struct` on the raw payload. Neither direction interoperates with Go.
2. **Outbound transport selector (High): ADJUSTED to Medium.** Confirmed that Rust is TChannel-only: `rust/crates/cadence-frontend/src/workflow_backend.rs:1-8,1180-1192` uses fixed TChannel addresses and never reads `system.enableGRPCOutbound` (Go `go/common/rpc/params.go:76`, `go/client/clientfactory.go:117-155`). The severity is lower because Go History and Matching always serve TChannel as well, so a default mixed cluster works. The bug only shows when the TChannel ports are unreachable or when the operator relies on the gRPC port in membership.
3. **Continuation drops VersionHistoryItem (High): CONFIRMED.** Go passes `token.VersionHistoryItem` to `queryHistory` (`go/service/frontend/api/handler.go:1981-1983`). Rust `poll_mutable_state` sends only fields 10, 20, 30 and 40 (`rust/crates/cadence-frontend/src/workflow_matching.rs:863-879`). The call site at `workflow_matching.rs:974` passes no item, so `CurrentBranchChangedError` cannot fire.
4. **History retry policy (Medium): CONFIRMED.** Go `FrontendRetry` proxies the ServiceBusy WorkflowID rate-limit reason, and `IsServiceTransientError` retries InternalServiceError (`go/common/util.go:294-320`). Go's policy is 50ms exponential, 10s cap, 30s expiry (`util.go:54-56`). Rust retries only ShardOwnershipLost and any ServiceBusy, with linear 250ms steps, a 25s budget and 20 attempts (`workflow_backend.rs:36-38,2156-2178`). Rust also re-resolves the owner instead of following the owner in the exception.
5. **Nested schedule/domain calls re-enter the public wrappers (Medium): CONFIRMED.** Go calls the inner `wh.StartWorkflowExecution` (`go/service/frontend/api/handler_schedule.go:286`). Rust goes through `SharedWorkflowApi.call` (`rust/crates/cadence-service-core/src/schedule_flow.rs:371`, `yarpc.rs:268-273`). That path runs authorization, redirection and the rate limiter again (`rust/crates/cadence-frontend/src/handler_workflow.rs:921-930,1072-1080`).
6. **Worker poll stage-ordered wait (Medium): ADJUSTED to Low.** Rust implements an all-or-nothing reserve that refunds tokens (`rust/crates/cadence-frontend/src/wrappers/quota_policy.rs:111-120`), while Go's wait path consumes tokens stage by stage. The only effect is residual token accounting when a later stage is exhausted. No request result or error differs.
7. **Async Kafka TLS serverName (Medium): CONFIRMED.** `server_name` is copied into config (`rust/crates/cadence-frontend/src/async_kafka.rs:46,103`) but `async_kafka_tls.rs` never references it. Go applies it through `ToTLSConfig` (`go/common/config/tls.go:73`).
8. **RemoveTask missing Type (Low): CONFIRMED.** Go returns `ErrRequestNotSet` before delegating (`go/service/frontend/admin/handler.go:308-310`). Rust bridges to `admin::remove_task`, which validates the shard first and uses a different message.

## Independent pass

I spot-checked the paths these findings touch: handler dispatch order, schedule flow, admin RemoveTask and the History poll continuation. I found no additional confirmed behavior differences. This pass was not exhaustive. It did not fully re-audit the redirection policy, pagination tokens, or every admin and domain handler, so it should not be read as a completion sign-off for those areas.

## Final agreed fix list (priority order)

1. **High:** Use 0x59 version framing for domain replication queue and DLQ encode and decode. Tolerate legacy unframed Rust rows on read. Add Go fixture tests for both directions.
2. **High:** Send `VersionHistoryItem` (field 50) in the resumed `PollMutableState` and map `CurrentBranchChangedError` as Go does.
3. **Medium:** Port `FrontendRetry` and `IsServiceTransientError` with the 50ms/10s/30s exponential policy and caller-deadline handling. Follow the owner from ShardOwnershipLost. Route the history paging poll through the same retry path.
4. **Medium:** Add an inner (unwrapped) handler entry for nested calls: the schedule start and visibility reads, and the DeleteDomain list check.
5. **Medium:** Apply `tls.serverName` in the async Kafka TLS client config.
6. **Medium:** Honour `system.enableGRPCOutbound` for History and Matching outbound calls, using the matching membership port.
7. **Low:** Check RemoveTask `Type == nil` first and return `ErrRequestNotSet`.
8. **Low:** Add a stage-ordered consuming wait for worker polls when `maxWorkerPollDelay` is set.
