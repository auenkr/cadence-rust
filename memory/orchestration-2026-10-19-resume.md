# Matching orchestration checkpoint — 2026-10-19

Main source remains 139757411. Matching is incomplete; History has not started.
Updated root AGENTS.md has been reread; the user's edit is preserved uncommitted.

Explicit gpt-6-luna implementation workers:
- recover_workers: runtime owner in matching-dns-provider worktree; sole heavy-build grant. Focused matching_runtime tests compiling in tmux cadence-rust, followed by actual production-binary lifecycle/shared-listener probes and strict Clippy. Source not accepted yet.
- outbound_luna: SWIM runtime source in t3-6f14baa6; no Cargo grant. Pinned Go oracle passed; Rust source/runtime probe still pending validation.
- cache_luna: cache draft committed as 66006fd0f, not integrated. Reports 10 focused tests and strict Clippy passing, with same-input Go trace evidence. Acceptance review remains pending; production wiring remains open.

Do not run competing heavy link jobs. Preserve warmed targets listed in the previous handoff; inactive t3-e426148a target was removed. No combined regression has run on current main; last frozen full evidence remains 3c8c5f98a (34 stages, 568 tests).

Next acceptance gates: review cache, finish runtime and SWIM validation, wire production membership/cache/discovered History, then actual production cadence-server mixed Go/Rust SDK failure/restart/recovery validation and full Matching audit. Component tests do not establish service completion.

## Following acceptance pass

Parent found production-bin compile errors not exercised by library tests; runtime worker is fixing main Option/mut assembly and running bin check before linking. Failed build/check logs retained. Cache fixes now include inherited scan deadline, retry terminal/error preservation and lifecycle ownership; actual SQLite busy-deadline behavior still requires regression validation before acceptance.

New T3-owned worker explicitly uses codex/gpt-6-luna, medium reasoning, serviceTier=default (normal speed), isolated worktree, no build grant. Task: Matching's discovered History dependency routing, not migration of History service. Scope excludes main/runtime/TChannel/Ringpop/cache. Task ID:
`node:delegated-task:command%3Amcp%3A6a1e03e1-ed6f-4ea2-b645-7d7964f28ed9%3Adelegate-task%3Amatching-history-routing-luna-20261019-r1`
Track this task with T3 task_status/task_cancel; do not duplicate it. Build queue after runtime: cache acceptance, then SWIM. Heavy builds remain serialized.
