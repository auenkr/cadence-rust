# History runtime continuation — 2026-11-09

Base: master c42480cdc. History is incomplete. Rust Matching and Frontend are required in acceptance lanes; Go Worker remains.

## Work and findings
- Hard worker audit recommends a lossless owned-state bridge before replacing EngineMutableState; see history-mutstate-unification-plan-20261109.md. Preserve heartbeat/timer masks, opaque blobs, buffers, CAS, and transaction cleanup semantics.
- First Luna activity-status worker stopped with uncommitted partial work; replacement worker is completing it in the same checkout. Nothing from that slice is merged yet.
- First live-queue worker implemented only a fail-fast startup guard on t3/wire-history-queues, not executor assembly. Not merged. Split the actual construction into narrow config/domain adapters then processor/executor assembly.
- Added history-mixed-gate.yml and --rust-history-binary mode to the existing SDK harness: Rust History/Frontend/Matching and Go Worker only; supports MySQL/PostgreSQL/Cassandra lanes, two History hosts with SIGKILL/restart, gRPC/TChannel outbound selection, fresh CI target, binary hashes, process commands and source SHA evidence. This is validation infrastructure, not a passing acceptance claim.
- Existing Frontend gate mode still uses Go History/Worker. The new mode reuses the existing SDK suite, which must be expanded for History replication, conflict/failover, CAN/retry/cron, and persistence failure cases before full parity can be claimed.

## Validation
- Python AST parse and shell syntax checks passed locally; no Cargo/rustfmt/Go builds or tests run locally.
- Runtime gate not yet dispatched; record exact run/head/conclusion once available.
- Final requirement remains fresh dedicated CI target, clippy followed by workspace tests, and full runtime/parity evidence. No History service completion is claimed.
