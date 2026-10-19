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

Round 1 History resolver returned source-only seam, leaving constructor-channel RPC routing unchanged; not accepted as the requested implementation. Round 2 now owns completion of actual opt-in resolver-aware Matching History calls and Go multi-peer placement evidence in a new T3 worktree, same explicit standard-tier Luna model. Task ID suffix replaces `r1` with `r2` in the full task ID above. Prior source is retained at t3-04a109d4 for carrying forward; no builds authorized for this worker.

Runtime check passed after Option/mut fixes, then worker released build slot while correcting drain/listener lifetime. Cache has sole grant for focused persistence/cache tests and strict Clippy, using t3-a33b4846 target. Parent verified live persistence test build PID1593320. No combined current-main validation or service acceptance yet.

## Cache and routed History review continuation

Cache component validation reached 9 persistence tests (1 pre-existing Go-file-dependent test ignored), 14 cache tests, strict all-target Clippy, and final Rust/Go trace equality. Worker then corrected generic SQL scan/driver error retry classification; those checks must be rerun before acceptance. Parent also requested unbiased uniform jitter matching Go rand.Intn. Cache retains sole build slot until frozen/committed.

History routing rounds 2 and 3 remained partial (per-peer channels and membership test source, but no ownership redirects or executable placement oracle). Neither is accepted. Current implementation task is standard-tier gpt-6-luna high reasoning, narrowly implementing redirects across all five Matching outbound methods, carrying prior source from t3-c63f01e1. Task ID:
`node:delegated-task:command%3Amcp%3A6a1e03e1-ed6f-4ea2-b645-7d7964f28ed9%3Adelegate-task%3Amatching-history-redirect-luna-20261019-r4`
Outbound native Luna independently owns actual Go placement oracle in its existing SWIM worktree; no Cargo grant. Runtime worker is source-ready after listener ownership changes; needs regrant after cache and actual mid-drain Go RPC verification. No component patches integrated yet.

## Accepted cache and placement oracle

Main d40c84e48 contains accepted cache source (worker 66006fd0f + 921fe51a7); 0a214e47c records parent byte comparison and evidence scope. Main 28365d4ae contains actual Go three-host History placement oracle (worker9061f072b); parent verified all recorded Go/test hashes against main. No combined main regression yet.

Redirect task r4 completed source at `/home/ubuntu/.t3/worktrees/cadence-rust/t3-12810cdd`: all five calls use typed ownership redirects with shared timeout, plus two-tonic-peer test source. Uncommitted and unbuilt, not accepted. Cache Luna read-only review confirms five workflow key paths but flags added hard client-side timeout versus prior header-only timeout and Go caller-context semantics; confirm/fix before acceptance. Runtime injection remains separate (main loopback unchanged), TChannel named-port adapter absent. No Cargo grant for routing patch yet.

Runtime worker holds sole build slot. Spawned listener assembly failed E0597 because borrowed matching RPC config crosses tokio::spawn; worker instructed to own/clone config and use bin check before link. Failed artifacts retained. SWIM source remains unbuilt; queue after runtime. User AGENTS.md change remains uncommitted and preserved.

## Deadline fidelity and runtime probe continuation

Parent integrated the worker's precise Go timeout audit as memory/matching-history-timeout-audit.md. Only Record*TaskStarted get fixed per-attempt child deadlines; mutable-state/refresh/standby-describe rely on caller context. Routing r4 remains unaccepted for its fixed deadline across all five methods. Standard-speed high-reasoning Luna task now corrects these semantics and adds optional caller context propagation/tests, carrying prior source from t3-12810cdd:
`node:delegated-task:command%3Amcp%3A6a1e03e1-ed6f-4ea2-b645-7d7964f28ed9%3Adelegate-task%3Amatching-history-timeouts-luna-20261019-r5`
No routing build grant. Cache Luna now independently reviews unbuilt SWIM source, read-only.

Runtime production binary and focused 3 tests pass. Production Go probe first failed relative overlay path, then Go TChannel transport constructor tuple handling; worker fixed both and is rerunning with retained failed logs. Sole build grant remains runtime; no actual successful mid-drain evidence yet. No component/service completion inferred from the build.

## Runtime shared-listener and SWIM review evidence

Parent inspected runtime worker combined-run2.log/exit0: real generated Go Matching Thrift + Ringpop JSON on same TChannel port passed. Earlier Go probe constructor/context compile errors were fixed with failing logs retained. Runtime strict Clippy/final commit still pending; no full mixed cluster claim.

Withdrawn SWIM review finding: reviewer initially claimed Go RemoveMember swaps the last element. Parent read exact pinned ringpop-go v0.10.0 memberlist.go175-179: append(list[:i], list[i+1:]...) preserves remaining order and tuple assignment clears old final slot. Rust Vec::remove is correct. Reviewer acknowledged mistake; outbound owner made no source edit. Do not resurrect a swap_remove fix from the earlier message.

## Runtime accepted; SWIM build grant

Main56142ba02 accepts runtime-owner worker97f1dd008. Parent compared final main/runtime/TChannel/probe source and tests byte-identical after cherry-pick; lib retains independently accepted cache export. Worker76140e924 doc-only follow-up fixes command cwd/target reproducibility. Component evidence includes production binary, 3 focused tests, actual Go Thrift mid-drain/repeated SIGTERM/bind failure/TLS probes, same-port Matching+Ringpop probe, strict server all-target lint. Combined merged-main regression remains pending.

Sole heavy build grant transferred to outbound_luna for SWIM, existing t3-975b4b2d target, jobs1. Parent verified no jobs at transfer. No routing builds yet.

Routing r5 source completed unbuilt at t3-13f816cc: Record* redirect chains share per-attempt budget; mutable/describe/refresh no invented fixed deadline, optional caller-deadline methods, poll deadline passed into mutable-state call in grpc_matching.rs. Source tests added; describe/refresh outer interface propagation still open. Carry this latest source (not earlier t3-12810cdd) into next routing validation task. No component acceptance yet.

## Next implementation lane

Runtime Luna now owns SOURCE-ONLY production Matching domain-cache integration in its existing matching-dns-provider worktree. It may carry accepted cache commits a972cad3f+d40c84e48, wire Go-shaped defaultStore SQLite selection, runtime ownership/readiness and actual domain consumers/failover signals. It must not touch History routing/Ringpop/TChannel and has no Cargo grant while SWIM runs. Existing query timeout block in grpc_matching.rs is owned by routing draft t3-13f816cc; coordinate eventual merge. Shared-store Go seed/update evidence is required; unsupported backend must not silently use seeded snapshots. This does not waive other backend parity or mixed-cluster gates.
