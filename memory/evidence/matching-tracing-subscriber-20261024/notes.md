# Matching poll tracing: subscriber wiring (2026-10-24)

## What changed

- `rust/crates/cadence-server/src/main.rs` and `rust/crates/cadence-matching/src/main.rs` install a
  `tracing_subscriber::fmt` subscriber before `launcher::run`:
  - filter: `EnvFilter::try_from_default_env()`, falling back to `warn` when `RUST_LOG` is unset or invalid;
  - writer: stderr (stdout is untouched, so the `println!` lines the harness and operators read are unchanged);
  - format: `compact()` with `with_ansi(false)`.
- `tracing-subscriber = { version = "0.3", features = ["env-filter"] }` added to both crates.
  `Cargo.lock` is not regenerated locally (no cargo on this host); CI resolves it (no `--locked`).
- Both mains are changed, not just `cadence-server`: the gate launches the `cadence-matching` binary
  (`tests/run-go-service-baseline.py:743`, `cadence-matching start --services matching ...`), so a subscriber
  only in `cadence-server` would leave the gate's poll events inert.
- No `warn!`/`error!` tracing calls exist in `rust/crates`, so the `warn` default adds no output
  unless `RUST_LOG` is set.

## Events

All poll events use target `matching::poll` (`rust/crates/cadence-matching/src/matching_dispatch.rs`):

- `poll started` (debug): `poller`, `window_ms`
- `poll returned empty` (debug)
- `poll claimed task` (debug): `task_id`, `sync`, `forwarded`
- `poll delivering task` (debug): `task_id`

## How to use RUST_LOG in the gate harness

Run the harness with the variable exported (example):

```sh
RUST_LOG=matching::poll=debug flock /tmp/cargo.lock /mnt/storage/service/bin/work-safe \
  python3 tests/run-go-service-baseline.py <existing gate flags>
```

- `RUST_LOG=matching::poll=debug` alone also silences everything else, because `EnvFilter` turns
  unlisted targets off. To keep the default `warn` for other targets, use
  `RUST_LOG=warn,matching::poll=debug`.
- Output goes to the Rust matching process stderr. The harness launches it with an environment built from
  `os.environ` (`tests/run-go-service-baseline.py` probe env, around line 990), so the exported variable should
  reach it. **Not yet verified:** confirm on the first gate run by grepping the log for `matching::poll` and
  checking that lines appear. If none appear, check the environment the Rust matching process was started with.
- Look for the events in the log file that captured that process (`probe.log` or `server.log` under the
  gate work dir): `grep 'matching::poll' <log>`.
- Format: compact, no ANSI, one event per line, fields after the message (`poller=... window_ms=...`).

## Verification

Build/test/clippy: GitHub Actions `rust-ci.yml` on this branch (run id and result recorded in the commit
message / PR, not here). No local cargo was run.
