# Frontend integration — 2026-11-01

Frontend is incomplete. User retains Go Worker, uses Rust Matching, and now permits at most two workers at normal speed: Luna for implementation; gpt-6.1-terra only to unblock difficult issues (at most medium reasoning). This supersedes the earlier Claude restart instructions. The prior restart automation was disabled when work resumed.

## Integrated source

Branch frontend-integration-20261101, merge head ace9aefd49431bdb709d66be77c036797e5200d9, combines master with origin/fe-workflow-service (d9dff7484, including unverified caller-header propagation) and origin/fe-admin-domain (0d55d29cd).

Merge resolutions preserve workflow backend/matching/visibility modules plus scheduler wiring, shared matching infrastructure plus schedule modules, admin qualified dispatch plus caller-header forwarding, and the Admin branch's domain audit failover-info handling. No source branch was deleted. Existing untracked progress-dashboard.html was untouched.

CI https://github.com/auenkr/cadence-rust/actions/runs/41327276590 tests cadence-frontend with format_all=true at ace9aefd49431bdb709d66be77c036797e5200d9. Authoritative observation at 16:38 UTC: in_progress. This is not evidence of success. Obtain its result, inspect/apply formatting artifact if needed, fix integration failures, then validate Matching and the full workspace as appropriate. All Cargo/Go validation remains CI-only.

Subsequent result: 41327276590 FAILED clippy with E0201 duplicate get_domain/list_domains in matching_domain_metadata.rs (both source branches added SQL read-through). Fixed by retaining one identical implementation pair, and applied CI artifact format-all.patch (no local formatting). Fix head b967e1442cfc431c1d8253df17044755febeb685. New cadence-frontend CI: https://github.com/auenkr/cadence-rust/actions/runs/41327559111, pending result.

## Assigned independent slices

- Luna medium, serviceTier default: t3/e6c456a1, worktree /home/auenkr/.t3/worktrees/cadence-rust/t3-e6c456a1. Owns synchronous gRPC StartWorkflowExecution and SignalWithStartWorkflowExecution routing through WorkflowApi, plus focused tests. Delegate ae067989-ea62-4e95-bb25-5d5fcc5d3bb7 (command namespace 8fb5dc75-901f-4600-9ec7-e385efe369a5).
- Luna medium, serviceTier default: t3/dfe56271, worktree /home/auenkr/.t3/worktrees/cadence-rust/t3-dfe56271. Owns Admin GetReplicationMessages and GetDLQReplicationMessages handlers/transports/converters, plus focused tests. Delegate 558f4985-8f2d-41ad-8b9f-96f965e356be (same namespace).

Both launched successfully after earlier parent_not_active errors. Follow completion notifications; do not poll delegate runs or start replacements without terminal evidence. Neither slice establishes overall Frontend parity.

## Required continuation

Mixed baseline dispatched: https://github.com/auenkr/cadence-rust/actions/runs/41327420652, workflow ref frontend-integration-20261101, pinned rust_ref b59401a49. No other mixed gate was live at dispatch (previous three listed runs were terminal failures). Await actual result; a dispatched run proves no compatibility yet. Workflow uses a fresh target, SHA256 capture, bounded MySQL container and cleanup trap.

Subsequent result: 41327420652 FAILED before build: actions/checkout interpreted abbreviated SHA as branch/tag. No binary or runtime evidence. Use exact full SHA when pinning workflow rust_ref. Run 41327569882 was dispatched with a mistyped full SHA and cancelled immediately; do not use as evidence. A corrected mixed run was dispatched at b967e1442cfc431c1d8253df17044755febeb685; retrieve its URL from workflow run list.

Source-confirmed next slice: launcher.rs Frontend TChannel serve path calls `tchannel::serve(tchannel_addr, tchannel_state)` without Ringpop handler. tchannel.rs rejects service `ringpop` when handler is absent; only Matching serve paths currently accept a Ringpop handler. This supports the earlier mixed-gate join failure report and needs a dedicated Frontend discovery/lifecycle port after a worker slot frees. Preserve advertised role/membership semantics from Go; simply accepting join messages is insufficient.

Integrate focused worker commits after checking actual diffs and CI evidence. Remaining scope includes asynchronous/gRPC history/visibility hook routing, advanced/archived and Cassandra persistence, History routing/discovery, wrappers, remaining Admin replication/DLQ/cross-cluster and config/membership/audit gaps, SDK and Go Worker compatibility, restart/recovery. Reconcile branch-local gap lists against current code. Run existing frontend-mixed-gate with Rust Frontend+Matching and Go History+Worker (no Go Frontend/Matching); record binary SHA256 and actual failure artifacts. Prior gate failures are in frontend-mixed-gate-20261101.md. Full parity audit and fresh CI clippy then workspace tests remain necessary before completion.
