# Matching: Go wire contracts for nil-field panics (source-derived)

Derived by reading source only (no Go run, no wire capture). yarpc v1.88.0
(go.mod; module cache), tchannel-go v1.32.1. Paths are under `go/`.

## 1. RespondQueryTaskCompleted: Request=Some, Result=None (gRPC)
- Mapper `ToRespondQueryTaskCompletedRequest` (common/types/mapper/proto/api.go:3182-3193)
  dereferences `t.Result.ResultType` -> nil-pointer panic.
- It is called from the generated wrapper
  (service/matching/wrappers/grpc/grpc_handler_generated.go:78-80) BEFORE the
  handler, so handler.go's `log.CapturePanic` is not on the stack.
- The panic is inside the yarpc protobuf unary `Handle`; recovered by
  `transport.InvokeUnaryHandler` (yarpc api/transport/handler_invoker.go:61-85,
  `handlePanic` -> `fmt.Errorf("panic: %v", r)`), a plain non-yarpc error.
- `handlerErrorToGRPCError` / `toGRPCError` (transport/grpc/handler.go:270-282, 319-330)
  return it unchanged -> gRPC `Unknown`, message
  `panic: runtime error: invalid memory address or nil pointer dereference`.
  No typed error name header. Process and connection stay up.
- Observability middleware only emits metrics and re-panics (internal/observability/middleware.go:299-312).
  Cadence inbound middlewares (common/rpc/params.go:161-163) do not recover.
- Superseded: earlier reviewer assumption that Internal / "unknown query completed
  type" is an acceptable divergence. Go returns Unknown with the `panic: ` message.
- Not in scope: nil outer Request (maps to nil), Thrift nested CompletedRequest.

## 2. PollForActivityTask: PollRequest=None
Order (service/matching/handler/handler.go:212-249, engine.go:888+):
1. defer CapturePanic (outermost)
2. hCtx from nil-safe `GetPollRequest().GetTaskList()`
3. worker rate limiter -> `errMatchingHostThrottle` via handleErr
4. `ValidateLongPollContextTimeoutIsSet` -> no deadline gives
   `BadRequestError{"Context timeout is not set."}` (common/util.go:124)
5. engine loop: `common.IsValidContext` first (cancelled -> ctx.Err(); <threshold
   deadline -> DeadlineExceeded), THEN nil deref on `request.TaskListMetadata`
   (~engine.go:915, `request` = nil `req.PollRequest`).
- Panic unwinds past `hCtx.handleErr`, so no `InternalServiceError` wrapping.
  `CapturePanic` (common/log/panic.go:37-54) sets retError to the raw
  runtime.Error: message `runtime error: invalid memory address or nil pointer dereference`
  (NO `panic: ` prefix on this path).
- gRPC: wrapper `proto.FromError` falls to `NewError(CodeUnknown, err.Error())`
  (common/types/mapper/proto/errors.go:34-93) -> `Unknown`, that message.
- Thrift: `mapper.FromError` returns raw err (mapper/thrift/errors.go:28-85);
  tchannel `getSystemError` (yarpc transport/tchannel/handler.go:388-394) ->
  SystemError `ErrCodeUnexpected` = 0x05, same message.
- Superseded: any assumption of Internal/InternalServiceError for this case.
- Matches the Actions oracle 40175488464 raw runtime error log.

## Limits
- gRPC: unchecked whether `protobuf.NewError` adds extra headers/details for Unknown.
- HTTP Thrift inbound untraced; 0x05 applies to tchannel only.
- No wire capture was done; confirm later with a mixed gate if needed.
