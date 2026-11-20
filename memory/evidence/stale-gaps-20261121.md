# Stale open-gap check (2026-11-21)

Both gaps were already fixed and merged to master; the dashboard/progress.md entries were stale. Verified from existing evidence only (no new runs; no code change).

## (a) History soak stall: RESOLVED
- Notes: memory/history-soakstall-20261114.md, history-soak-20261113.md.
- Merged: b8af03b6d (history-soak-tail).
- Evidence: mysql 20 min soak run 43558072143 SUCCESS 4800/4800, 0 failed, p50 4111ms; recovery gates mysql 43561066779, postgres 43574115408, cassandra 43577811696 all `GATE SUMMARY failed: []`; workspace clippy+tests 43576413232 SUCCESS.
- Later: frontend soak 70m PASS on both lanes (60dd12fab).

## (b) Mutable-state JSON shape: RESOLVED
- Notes: memory/history-mutstate-json-20261114.md (supersedes history-goreplfollow-20261114.md item 4 and history-goreplgate-20261114.md).
- Merged: 924bcf463 (history-mutstate-json).
- Evidence: History CI run 43591791527 (917 passed, 0 failed, five mutable-state JSON fixtures); MySQL replication gate 43592469144 `GATE SUMMARY failed: []`. The gate's version-history normalizer was removed, so Rust and Go output compare raw.
- Remaining documented limitation: user-approved nil/empty presence difference.

No new CI dispatched. Nothing run locally.
