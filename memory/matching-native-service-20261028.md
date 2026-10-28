# Native Matching service implementation attempt, 2026-10-28

## Scope and result

The assigned `matching_service_native.rs` currently contains the service shell,
the explicit required integration callback contract, lifecycle/admission
sequencing helper, failure-metric helper, and tonic deadline extraction. It
does **not** implement `NativeMatchingApi`; this is not a completed or
registerable service. The requested 13 RPC coverage and focused tests remain
unimplemented, and the service shell is not verified to compile.

## Runtime contract captured

`NativeMatchingIntegration` requires explicit startup, admission, request and
failure metrics, ownership, and domain-name callbacks, and inherits the
existing add, poll, query, and control engine context traits. Construction is
`NativeMatchingService::new(engine, integration)`. `enter` orders startup,
admission, then request metric. Deadline extraction uses
`matching_sync::request_deadline`.

## Gaps and validation

No RPC methods have been implemented and no tests have been added. The trait's
13 descriptor-generated method signatures and Go's exact mapper/admission/
quota/domain/ownership/metric order still need to be wired to engine methods.
Typed `NativeQueryError`, cancellation guards, control RPCs, and all response
paths remain outstanding. No Cargo, Go, rustfmt, build, test, or CI command was
run. `git diff --check` is the only requested validation to run before handoff.
