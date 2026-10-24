# Unleash repository polling parity update (2026-10-24)

Source baseline: `ae442535c1d4ed708daeb38face1ab0e9d84f88a`. The original `t3/unleash-repository-backoff` branch was merged into isolated branch `t3/unleash-repository-backoff-1`; corrected code SHA validated: `dd4d9b41629b6b276584c9f447f143e7191f936d`.

## Changes

- Backoff consumes skips on fixed refresh ticker events, matching Go `repository.sync`: after one 503, one ticker is skipped and the following ticker performs the next request. HTTP 401/403/404 set skips/errors to 10; 429 and 5xx increment to 10; other statuses, transport failures, and malformed bodies leave the error count unchanged. A decoded success decrements the error count and sets skips to the new error count. HTTP 304 preserves the snapshot and backoff error count.
- Unix username lookup now matches Go `os/user.Current` with `getpwuid_r(getuid())`, retries `ERANGE`, and avoids the shared static record from `getpwuid`. The hostname buffer is explicitly NUL terminated before constructing a `CStr`. Fallback usernames use Go's `generated-<0..999999>-<pid>` shape.
- A deterministic fake HTTP sequence exercises the production request function with controllable ticker events: decoded success with ETag, 503, one skipped ticker, 304, transport failure, malformed JSON, and decoded recovery. It checks request scheduling, conditional ETag headers, snapshot retention, and backoff recovery without timing assertions against real intervals.

Evaluator and metrics behavior were preserved.

## CI evidence

All Cargo validation ran on GitHub-hosted Actions with `clean_target=true` and a fresh uncached target. Final service-core and workspace validation were sequential and used the same code SHA.

- Service-core: https://github.com/auenkr/cadence-rust/actions/runs/40091168846 — head `dd4d9b41629b6b276584c9f447f143e7191f936d`, conclusion **success**. Strict clippy, formatting, and all service-core tests passed.
- Workspace: https://github.com/auenkr/cadence-rust/actions/runs/40091493245 — same head SHA, conclusion **success**. Strict workspace clippy, formatting, and workspace tests passed.
- Earlier attempts on this branch: run `40090533161` failed formatting; run `40090791899` exposed incorrect assertions in the new deterministic tests. Both were corrected before the successful final SHA above.

## Remaining verification

No mixed Go/Rust cluster validation was run specifically for this change, and no mixed-gate Rust binary SHA256 is available. Matching completion still requires its wider mixed-cluster, recovery, and parity audit evidence.
