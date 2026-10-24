# Unleash repository polling parity update (2026-10-24)

Source baseline: `ae442535c1d4ed708daeb38face1ab0e9d84f88a`. Implementation branch: `t3/unleash-repository-backoff`, final source SHA validated: `4f02ea1ab6bcce488c346517b2957574e284442b`.

## Changes

- Added a fetch backoff state helper to the service-core Unleash repository poller. HTTP 401/403/404 set the error/skip count to 10; HTTP 429 and 5xx increment to 10; other HTTP statuses, transport failures, and JSON decode failures leave state untouched. A successful decoded response decrements the error count and makes skips match it. HTTP 304 retains the feature snapshot and does not count as a decoded successful fetch.
- Poll delay is the configured interval multiplied by `skips + 1`.
- Replaced `USER` and `/etc/hostname` identity defaults with OS user/hostname lookups (`getpwuid`/`gethostname` on Unix), and a generated random PID-bearing fallback username when the account lookup fails.
- The existing local fake HTTP polling test covers a decoded fetch, a 503 while preserving cached state, a malformed body while preserving cached state, headers, and URL escaping. A new deterministic state-helper test covers throttling, recovery, 304's no-transition classification, ignored statuses, and cap behavior.

No callbacks without Rust consumers were added as public API; evaluator and metrics behavior were preserved.

## CI evidence

All Cargo validation was run on GitHub-hosted Actions with `clean_target=true` (fresh uncached target).

- Service-core run: https://github.com/auenkr/cadence-rust/actions/runs/40089725841 — head `4f02ea1ab6bcce488c346517b2957574e284442b`, conclusion **success**. Clippy, formatting, and service-core tests passed.
- Workspace run: https://github.com/auenkr/cadence-rust/actions/runs/40089966602 — same head SHA, conclusion **success**. Workspace clippy, formatting, and tests passed.
- Earlier service-core attempt on `8f61f56edc4876faf33903f7f54822b7c7b4636a` failed only the formatter step; formatted follow-up was pushed and the two final runs above passed.

## Remaining verification

No mixed Go/Rust cluster validation was run specifically for this change. Matching completion still requires its wider mixed-cluster, recovery, and parity audit evidence. Existing Unleash fake-server test uses 503 and malformed JSON cases; the `304` distinction is currently exercised through the production backoff helper, not a dedicated end-to-end fake HTTP sequence.
