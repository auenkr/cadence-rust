# Matching CI evidence status — 2026-10-26

Evidence-only inspection; no CI run or gate was dispatched, cancelled, or rerun. No local Cargo, Go, formatting, or runtime commands were run. The checkout is on the separate branch `t3/ci-evidence-audit`.

## Single status snapshot

Inspected once with `gh run view` at approximately 2026-10-26 18:00 UTC. The two Rust CI runs and the earlier adaptive mixed gate were all still in progress. Since no run was completed, no artifact download or claim based on log contents was possible.

| Run | Branch | Exact head | Snapshot | Observed completed steps |
|---|---|---|---|---|
| [40373450969](https://github.com/auenkr/cadence-rust/actions/runs/40373450969) `rust-ci` | `matching-append-guard-f6b29a5` | `f6b29a558a6716f9c428e9a321b7fd8d8252eb63` | in progress; Test in progress, Go/Rust comparison pending | Go oracles, format checks, changed Matching runtime formatting check, Clippy all successful |
| [40373190531](https://github.com/auenkr/cadence-rust/actions/runs/40373190531) `rust-ci` | `master` | `f6b29a558a6716f9c428e9a321b7fd8d8252eb63` | in progress; Test in progress, Go/Rust comparison pending | Go oracles, format checks, changed Matching runtime formatting check, Clippy all successful |
| [40373145351](https://github.com/auenkr/cadence-rust/actions/runs/40373145351) `Matching production validation` | `t3/matching-pin-eecb6bef8` | `eecb6bef8b5b5c6c36e3e9c7336fa01f451a3f97` | in progress; Mixed Go and Rust validation in progress | Fresh Rust Matching build and dependency probes successful; PostgreSQL probe client skipped |

## Evidence still needed

When these exact runs are complete, download their artifacts under `/mnt/storage/service/ci-evidence` using `TMPDIR` set to a temporary directory and verify from the actual downloaded logs/records:

- For both `f6b29a558a6716f9c428e9a321b7fd8d8252eb63` runs: build/target identity; clippy, test, format-check and runtime-format exits; the new 724 test, delayed test and subprocess evidence; hashes where recorded.
- For `eecb6bef8b5b5c6c36e3e9c7336fa01f451a3f97`: actual build and gate/probe exits, built binary SHA256, scaling, static listing and all 24 SDK scenario evidence.

This is a pending-status record only. Green status, step names, and partial step completions are not proof of the requested gate assertions. The combined workspace run `40373705020` at `10692b2d` belongs to its parent task and was not inspected or altered here.
