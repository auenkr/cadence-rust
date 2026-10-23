# Matching Thrift/TChannel lane audit (2026-10-23)

Base: origin/master 5ff4d0ac1. Go yarpc v1.88.0 (`~/go/pkg/mod/go.uber.org/yarpc@v1.88.0`).
Nothing in this file was run against real Go clients: the lane is wired (dry-run only) but not executed.

## 1. How to make Go call Matching over TChannel/Thrift

- `go/common/rpc/params.go:76` reads `system.enableGRPCOutbound` (default true,
  `go/common/dynamicconfig/dynamicproperties/constants.go:4829`).
- `params.go:~103-117` passes it to `NewDirectOutboundBuilder` for History and Matching.
  `outbounds.go:235-248`: true -> gRPC outbound, false -> `tchannel.NewOutbound`.
- The client factory picks the Thrift wrappers when the outbound is not gRPC
  (`IsGRPCOutbound`, `go/common/rpc/outbounds.go:~262`; wrappers `go/client/wrappers/thrift`).
- Peer addresses come from the ring's `tchannel` named port (`direct_peer_chooser.go:199`).
- Same flag moves Go-to-Go History calls to TChannel too (no per-service switch exists).
- Harness: `tests/run-go-service-baseline.py --matching-transport thrift` appends
  `system.enableGRPCOutbound: false` to the Go dynamic config only. Rust Matching's own dynamic
  config is still driven by `--history-transport`. Validated with `py_compile` and
  `--dry-run` only (see Evidence).

## 2. Dispatch audit (Rust vs Go)

| Item | Go | Rust | Result |
|---|---|---|---|
| Procedure names | `MatchingService::<Method>`, `Meta::health` (generated `matchingserviceserver`, `metaserver`) | `tchannel.rs:510-516` `find_method`, service filter `MatchingService`/`Meta` | match |
| Service name | `cadence-matching` | `tchannel.rs:496` | match; wrong service -> code 6 (bad request) like yarpc router miss |
| Encoding header `as` | `thrift` | `tchannel.rs:500` rejects others | match |
| Caller `cn` | `call.CallerName()` | `tchannel.rs:~232` metrics context | match |
| Request app headers (arg2) | `readRequestHeaders` | `tchannel.rs:518-522` parses, rejects trailing bytes | match (headers ignored) |
| Success reply | arg3 = result struct (field 0), `$rpc$-service` echoed (`handler.go:124`) | `tchannel.rs:548,560-569`; void -> empty struct | match |
| App error reply | `SetApplicationError` + `$rpc$-application-error-name/-details` headers, call-res code 1 (`handler.go:139-151`, `encoding/thrift/inbound.go:80-96`) | `tchannel.rs:549-566` | match |
| Error details truncation | 256 bytes total incl. ` (truncated)` suffix (`handler.go:321-327`, `constants.go:33-35`) | **was** cut at 256 with no suffix | FIXED |
| `$rpc$-application-error-code` header | set only if meta has Code (thrift handler leaves it nil for exceptions) | not sent | match |
| No TTL | `ErrTimeoutRequired` system error (`handler.go:~160`) | ttl 0 -> immediate timeout error code 1 | minor, not changed |
| Timeout | system error timeout | `tchannel.rs:543-544` code 1 | match |
| Meta::health | `{Ok:true, Msg:"matching good"}` after `startWG.Wait()` (`service/matching/handler/handler.go:106-111`) | `thrift_matching.rs:~697-702` waits started, `{1:true,2:"matching good"}` | match |
| Oneway | Matching IDL has no `oneway` methods | n/a | n/a |
| Poll timeout | handler runs under caller ctx | `thrift_matching.rs:~717-729` returns empty poll response when budget (deadline-1s, capped by poll expiration) ends | Go-parity behaviour of the gRPC path; not retested on wire |

Exception field IDs per RPC come from `cadence_thrift::service::find_method(...).exceptions`
(generated from the pinned IDL; `matching.thrift` `throws` blocks, e.g. AddDecisionTask has
8 exceptions, ListTaskListPartitions skips id 3). Existing unit test
`matching_thrift_uses_each_methods_exception_field_ids` covers AddDecisionTask/AddActivityTask.

### Error body differences found and fixed

Go maps every error with `common/types/mapper/thrift/shared.go`:
- `FromEntityNotExistsError` (shared.go:2399): always sets fields 2, 3 (pointers, empty string) and
  4 (`required list<string> activeClusters`, shared.thrift:49-54). Rust sent only field 1.
  thriftrw rejects a reply missing required field 4 -> Go client would see a decode error
  instead of a typed `EntityNotExistsError`. FIXED: `thrift_matching.rs` `application_error`
  now emits 2, 3, 4 (from proto details when present, else empty).
- `FromDomainNotActiveError` (shared.go:2191): fields 2-5 always required. Rust only added them
  when proto details decoded. FIXED: always emitted, defaults when details absent.
- `FromServiceBusyError` (shared.go:5473): always sets `reason` (2). Rust omitted it. FIXED.
- `TaskListNotOwnedByHostError` (1-3) and `RemoteSyncMatchedError` (10) were already correct.

Unit tests added: `matching_thrift_exceptions_fill_required_fields_like_go_mapper`
(`thrift_matching.rs`), `application_error_details_truncate_like_yarpc` (`tchannel.rs`).

### Not changed / open

- Fallback mapping for untyped statuses is InvalidArgument->BadRequest, ResourceExhausted->ServiceBusy,
  NotFound->EntityNotExists (when allowed), else InternalService. Go's `FromError` matches by Go error
  type, so a Rust path that returns an untyped status where Go returns another exception type
  (e.g. `LimitExceededError`, `QueryFailedError`, `StickyWorkerUnavailableError`) must attach typed
  details; verified for Remote sync matched and sticky only.
- TChannel server accepts frames/protocol per tchannel-go v1.34.4; not compared with the Go
  client's init/ping/cancel behaviour on a live socket.
- Rust HTTP YARPC (`thrift_matching.rs` `http_handler`) was out of scope (Go Matching has no HTTP
  default). `yarpc.rs` is the Frontend dispatcher.
- Mixed-gate execution with `--matching-transport thrift` is NOT done; wire compatibility with real Go
  clients is unproven until a gate run.

## Evidence
See `commands.md`.
