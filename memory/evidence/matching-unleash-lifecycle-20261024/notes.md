# Matching Unleash client lifecycle evidence (2026-10-24)

## Implementation

Source commit: `cbd1b41ea10403f437e2dbc40ca89f98157bf34d` (`t3/matching-unleash-lifecycle`). The implementation is in `rust/crates/cadence-service-core/src/openfeature/unleash.rs`; focused formatting coverage was added to `.github/workflows/rust-ci.yml`.

The lifecycle now:

- Sends `/client/register` with the v4.5.0 Go client schema and request headers, generated connection UUID, built-in strategy names, configured refresh/metrics intervals, spec version, and instance ID.
- Counts enabled/disabled evaluations and selected variant names. In keeping with Go `Client.GetVariant`, an enabled feature with no selected variant increments `yes` and variant `disabled` because the fallback variant has `FeatureEnabled=true`.
- Rotates metrics buckets every scheduled send. On unsuccessful status, it merges counts and variant counts back and restores the attempted bucket's start time. HTTP 401/403/404 skip the next 10 intervals; 429/5xx apply the capped incremental backoff. A successful post reduces the error skip count.
- Sends `If-None-Match`, stores ETags only after a successful decoded response, and retains the cache on 304.
- Joins both background threads on shutdown and stops/joins the poller if metrics-thread creation fails. As in Go v4.5.0, shutdown does not force an immediate final metrics POST.

Fake-server tests cover request bodies/headers, arbitrary ordering of initial GET and registration, evaluation and variant counts, a failed metrics POST followed by a retry with retained counts, bucket boundary reset/restore, ETag/304 snapshot retention, and shutdown.

## Go reference

Pinned source: `/home/auenkr/go/pkg/mod/github.com/!unleash/unleash-client-go/v4@v4.5.0`.

Reviewed `metrics.go` (`newMetrics`, `sendMetrics`, `resetBucket`, `countVariants`, `getClientData`), `repository.go` (`fetch`, 304 handling, ETag update and backoff), `client.go` (`NewClient`, `GetVariant`, `Close`), and `utils.go` (`generateInstanceId`).

## CI evidence

Both runs used the required fresh, uncached runner target (`/home/runner/work/_temp/cargo-target-final`) on standard GitHub-hosted Ubuntu runners. No Cargo command was run on the development host.

- Service crate: https://github.com/auenkr/cadence-rust/actions/runs/40088570879
  - SHA: `cbd1b41ea10403f437e2dbc40ca89f98157bf34d`
  - Result: success; strict `cadence-service-core` clippy, focused rustfmt check and crate tests passed.
- Whole workspace: https://github.com/auenkr/cadence-rust/actions/runs/40088842690
  - SHA: `cbd1b41ea10403f437e2dbc40ca89f98157bf34d`
  - Result: success; workspace strict clippy, focused rustfmt check and workspace tests passed.

Superseded unsuccessful runs were caused by formatting and fake-server test assertions and were corrected before these final passing runs.

## Known parity limits

- Go reports its own runtime metadata (`platformName:"go"`, `runtime.Version()`) and exposes registration/sent/count/error callbacks. This provider does not expose the Go listener/channel API; its metrics payload identifies the actual runtime as `platformName:"rust"` with OS/architecture in `platformVersion`.
- The generated UUID connection ID has the same UUID v4 wire format. The default instance ID follows `USER` plus the system hostname on this host; exact `os/user.Current` lookup/failure behavior and Go's seeded random fallback are not replicated on every operating system.
- Metrics POST network errors retain counts and retry on the next interval, matching Go's network-error path; only response status codes drive HTTP backoff. Listener warning/error reporting is not exposed through this provider API.
