# Matching gRPC client and endpoint routing review (2026-10-30)

Scope: source `4a5da9fb0b9709e40d1117244c1faf82bc60a27f`, consolidated checkout `a47e4032d94134b250ce0670cbd5f791d32b285b`, PostgreSQL gate evidence `/mnt/storage/service/ci-evidence/matching-postgres-41100032910`. Reviewed endpoint mapping, grpc-js channel reuse/close, subprocess environment, proxy diagnostics and HTTP/2 evidence. No production change.

## Findings

- **No configured port mismatch found.** Evidence `ports.json` reserves Go Matching gRPC `24856` and Rust Matching gRPC `29438`. `rust-matching-command.json` passes `--matching-grpc-port 29438`; Rust logs `127.0.0.1:29438`; readiness diagnostic targets `127.0.0.1:29438`. The Go Matching port remains only a reserved port because this lane runs Go Frontend/History/Worker and Rust Matching.
- **No demonstrated grpc-js close/reuse defect.** The readiness helper constructs one direct `MatchingAPI` client before its retry loop, so calls in that loop share a channel. It closes the client when readiness succeeds, on early child exit, or after the final failure; the successful path returns immediately after close. No close occurs mid-retry and no later use of that readiness client is shown. Other direct clients are generally explicitly closed or closed in the final cleanup.
- **No proxy-routing evidence.** The process is spawned with inherited `process.env`. The captured diagnostic reports no recognized `*_proxy`/`*_PROXY` variables and `noProxyConfigured: false`; therefore this artifact does not implicate proxy routing.
- **TCP success plus grpc-js `Waiting for LB pick` does not identify the fault.** The direct call to `127.0.0.1:29438` timed out in grpc-js while its sampled channel state was `1` (`CONNECTING`). The independent TCP connect succeeded immediately. An HTTP/2 probe recorded no remote SETTINGS within 2 seconds, but that is only a short observation and is not proof of incompatible preface/settings. The Rust log contains `ListTaskListPartitions` stages through `engine-complete`; the trace is not correlated to the exact failed callback, so it does not establish that the failed grpc-js call received a response.
- **The readiness client does not appear to be accidentally closed by the readiness loop.** In `tests/matching-service-compat.cjs`, `primaryReadinessClient.close()` is after successful Frontend and direct-owner checks, immediately before return; failure branches keep retrying with that same client until the overall wait ends, then close it.

## Next discriminating check

On one reproduction, correlate a single direct grpc-js RPC with Rust-side accepted-connection/HTTP/2 frame evidence: capture the same TCP 4-tuple through the client preface, server SETTINGS, grpc-js connectivity transition, and Rust handler entry/exit. If a gRPC client preface reaches the listener but no server SETTINGS follow, investigate server accept/HTTP2 serving. If SETTINGS and request HEADERS arrive and the handler reaches `engine-complete`, investigate response frames/status delivery or correlation to grpc-js's channel/subchannel. Capture effective resolver/proxy routing only if a proxy is configured; the inspected run shows none.

No CI dispatch/poll, local validation, runtime, Node, Go or Cargo command was run. This is an evidence note only; it does not claim Matching acceptance or establish a production defect.
