# Frontend workflow API port — 2026-10-31

## Current branch and skeleton state

- Implementation checkout: `t3/frontend-workflow-api` at `94e79ff98`.
- `frontend-core` is a separate local worktree at `/home/auenkr/.t3/worktrees/cadence-rust/t3-57c063ab`, also at `94e79ff98`.
- `git fetch origin frontend-core` failed because there is no remote ref named `frontend-core`.
- The frontend crate currently contains only its launcher placeholder, `lib.rs`, and `main.rs`; neither worktree has the promised core handler/module files yet.
- The project instructions call for keeping implementation under `rust/crates/cadence-frontend/src/` and not colliding with the core skeleton.

## Landed in this checkpoint

- Added `wrappers/clusterredirection.rs` with Go policy names, V1/V2/V3 allowlists, deprecated-domain allowed API set, policy parsing, and pure forwarding selection (strong consistency, active-active domain, all-domain target match and selected API fallback).
- Added policy unit tests for the exact allowlist version deltas, all-domain fallback behavior, and deprecated-domain API list.
- Exposed the module through `wrappers/mod.rs` and `lib.rs`.

## Validation and evidence

- `git diff --check` passed.
- No Cargo command was run locally, per requirement.
- CI has not yet been dispatched; no run URL, head SHA, or conclusion exists for this checkpoint.
- This module is an initial pure-policy slice and does not establish full cluster redirection parity. The stateful Go behavior (domain cache, active-cluster manager lookups, config gates, fallback on `DomainNotActiveError`, logging, and metrics) remains to be connected to the frontend handler interfaces.

## Remaining work

- Reconcile with the `frontend-core` handler skeleton when it lands; import `handler_workflow*.rs` without overwriting its existing files.
- Port throttling, metering/error translation, version checking, access control, redirect call wrappers, and Thrift/gRPC wrapper behavior against the concrete Rust handler, config, errors, metrics, and transport APIs.
- Port `go/service/frontend/api/handler.go` workflow methods and mirror their Go tests, including workflow lifecycle, polling/responding, visibility, history, and list/scan/count behavior.
- Run the requested `rust-ci.yml` CI dispatch for `cadence-frontend`, record the run URL, exact head SHA and conclusion here, and report any parity gaps before claiming completion.
- Mixed Go/Rust SDK compatibility and restart/recovery gates remain outstanding.
