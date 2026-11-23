# Cargo lockfile repair evidence — 2026-11-23

## Cause

The Matching dependency-oracles gate at source SHA `efd0f6baf20aaffddbdfded1242fa9546e59f693` failed both locked builds (`history-tchannel-build` and `postgres-tls-build`). Their logs report that Cargo cannot update `rust/Cargo.lock` because `--locked` was passed. A workspace-only lock sync added the missing `cadence-server` dependency entries for `tikv-jemallocator` and `tower 0.5.3`. No third-party package versions changed.

## Change

- Lockfile repair commit: `9be4125d7ba861fc330736fd4f277fc5ff5f26b8`
- `rust/Cargo.lock` changes only the `cadence-server` dependency list, adding the two workspace dependency entries above.
- Lock sync ran on GitHub Actions using `cargo update --workspace`; no Cargo commands ran on the development host.
- GitHub requires a `workflow_dispatch` workflow to exist on the default branch before it can be dispatched. The initial branch-only temporary workflow was therefore not dispatchable (404). Instead, a temporary `sync_lock_only` input/job was added to the existing `rust-ci.yml` on this branch, used to produce the lockfile artifact, then removed before the lockfile commit. No helper workflow changes remain in the final branch state.

## CI evidence

All runs used branch `fix-cargo-lock` at head `9be4125d7ba861fc330736fd4f277fc5ff5f26b8`, except the lock-sync run which used the temporary workflow commit `1b0db0e59`.

- Lock sync: [run 45141498594](https://github.com/auenkr/cadence-rust/actions/runs/45141498594) — success; produced `rust-cargo-lock` artifact.
- Matching dependency oracles: [run 45141686444](https://github.com/auenkr/cadence-rust/actions/runs/45141686444) — success; 349-second job.
- Rust CI: [run 45142545712](https://github.com/auenkr/cadence-rust/actions/runs/45142545712) — success; clippy, workspace tests, PostgreSQL pool wakeup check, and Matching emission comparison passed; fmt passed. Job duration 716 seconds.
