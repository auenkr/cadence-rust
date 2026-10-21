# Matching long poll validation and throttled logging

On branch `t3/matching-runtime-owner` at the task start (`c253324be`), Matching's Rust gRPC decision and activity poll handlers had no equivalent of Go `common.ValidateLongPollContextTimeoutIsSet`. Go returns `common.ErrContextTimeoutNotSet`, a `types.BadRequestError` with message `Context timeout is not set.`, and logs `Context timeout not set for long poll API.` through the handler's shared throttled logger. Go configures that logger with `matching.throttledLogRPS` (default 20).

Rust now checks the incoming `grpc-timeout` metadata through `matching_sync::request_deadline` in both poll handlers before consuming the tonic request. Missing/unparseable timeout metadata returns gRPC `InvalidArgument` with the Go message. A single logger instance is initialized from the Matching dynamic config overlay and shared by both methods. It has an injectable clock and sink; production output uses `eprintln!`.

Also corrected `matching_quota::DynamicBucket::refresh`: Go's `rate.Limiter.SetLimitAndBurst` updates against its last recorded time and does not first advance to the configuration refresh time. The extra `tokens(now)` call was removed. Added unrun regressions for both missing-deadline RPCs, shared poll log throttling, and a depleted bucket whose rate changes at refresh.

No Cargo/build/test/service commands were run. The `grpc-timeout` request metadata remains the existing Rust transport deadline source; verify this behavior under real inbound tonic gRPC and native TChannel transport during the Matching compatibility pass. Current RPC regressions are source only and have not been executed.
