# Matching channel state and attempt-time diagnostics

Candidate source branch: `t3/matching-channel-state-attempt-diagnostics`

Base: `6c73d9aa447171d4eeda37d71dd0a955d0996a9b` (`origin/t3/matching-grpc-connection-diagnostic-565c8fc`).

## Change

`tests/matching-service-compat.cjs` now captures a timestamped gRPC channel connectivity timeline before, during, and after `ListTaskListPartitions`. It also starts a direct TCP connect when the diagnostic RPC is started, with a one-second timeout. The timeline has a 32-record cap and reports how many samples were omitted. Socket and sampling interval cleanup occurs on the RPC callback or synchronous call failure.

This is diagnostic-only. It does not change channel options, proxy behavior, credentials, RPC deadline, readiness criteria, or SDK/recovery assertions. Existing proxy diagnostics remain limited to environment variable presence and whether `NO_PROXY` is configured; no effective proxy or `NO_PROXY` matching is reported because the implementation does not reuse grpc-js resolver behavior.

## Limits and validation

The TCP probe checks the direct host and port in the gRPC target, so it does not establish whether a proxy route is usable. Connectivity sampling runs every 50 ms and can miss shorter transitions; output is capped at 32 records. The one-second TCP timeout bounds probe lifetime. No Cargo, Go, build, format, test, runtime validation, or CI dispatch was run, as requested. `git diff --check` was used for whitespace inspection only.

## Timestamp review correction

Review finding against source `07c3f09e6ce4e5bb47fa19874adfc7a97b8474d3`: the diagnostic's `completedAt` was assigned after awaiting the TCP probe, so it could trail the gRPC callback by up to the probe's one-second timeout. The correction captures `completedAt` at callback entry, records `diagnosticsAssembledAt` after the probe settles, and starts `startedAt` immediately before the RPC invocation. The RPC deadline, assertions, probe timer, and timeline bounds are unchanged. Remaining timestamps retain their intended scope: connectivity sample timestamps mark sampling, `diagnosticStartedAt` anchors elapsed diagnostic sampling, and TCP probe elapsed time starts when its socket is opened.

This correction is unvalidated: no local validation or CI dispatch/poll was performed. Source SHA: `07c3f09e6ce4e5bb47fa19874adfc7a97b8474d3`.
