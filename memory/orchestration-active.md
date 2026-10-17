# Recovery round 3 — 2026-10-17

All three standard-2 task_status results were cancelled, with no completed
validation or accepted implementation. Read their full messages; progress is
preserved at t3-7d86aaeb (maintenance), t3-c1c1f616 (HTTP), t3-f079b92a (membership).
New isolated standard-speed Luna tasks use task prefix
`node:delegated-task:command%3Amcp%3A23bdfb7a-603d-4d45-b778-557c3aff6443%3Adelegate-task%3A`
and suffixes:
maintenance-codex-luna-standard-3, http-codex-luna-standard-3,
membership-codex-luna-standard-3. Briefs carry original tasks, prior findings,
recovery paths, and require dedicated tmux windows and warmed build cache reuse.
No changes integrated yet. Await completion notifications; review completed tests.

# Luna speed preference — 2026-10-17

User requires NORMAL speed, never fast mode for Luna. All future delegated Luna
calls must explicitly pass target.options.serviceTier = "default" (Standard).
Cancelled the three recovery-codex-luna-1 workers solely to apply this setting.
Replacements launched with explicit Standard setting, same task ID prefix below:
- maintenance-codex-luna-standard-2
- http-codex-luna-standard-2
- membership-codex-luna-standard-2
Replacement briefs preserve original scope and require recovering partial work
read-only from prior checkouts t3-0ecf04fe, t3-54977afe, t3-b242067c respectively.

# Codex Luna orchestration — 2026-10-17

Current parent thread: a9ff5617-bf6c-4726-b79f-f5fecab3f665.
User explicitly requests orchestrator with Codex Luna implementation subagents,
continuing through the entire migration. Live catalog supports codex/gpt-6-luna.
Three isolated async recovery tasks launched; completion wakes this parent.
Task ID prefix: `node:delegated-task:command%3Amcp%3Aab1fb17e-9814-4e58-8a3e-2e4a624940b1%3Adelegate-task%3A`
Suffixes:
- `maintenance-recovery-codex-luna-1`
- `http-recovery-codex-luna-1`
- `membership-recovery-codex-luna-1`

Each task inspects preserved earlier work, repairs Go parity, finishes tests,
and returns commits. No earlier implementation accepted yet. Old task IDs below
belong to another thread and cannot be queried by this parent (task_not_found).
Keep existing three dirty timeout implementation files untouched during recovery.
Review and integrate returned commits, then run accumulated compatibility gates.
Whole migration remains incomplete; service order Matching, History, Frontend,
Worker remains authoritative. Async turn yielding does not mean migration complete.

# Free-model orchestration — 2026-10-17

## User-authorized provider change: Luna

Maintenance round 1 returned incomplete, no commit and no passing tests. Partial
diff preserved at /home/ubuntu/.t3/worktrees/cadence-rust/t3-b023cffb (queue,
grpc_matching, registry), plus memory/matching-maintenance-live-config.md.
Continuation isolated task:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-maintenance-luna-r2-finish`.
Read/reuse old diff without modifying old worktree; finish Go timer/GC semantics,
tests and commit. Do not invent MaxTasklistIdleTime consumption if Go has none.

HTTP round 1 returned 03ad06b1c, NOT integrated. Validation blocked on missing
go/idls in child worktree. Parent review also found matching_service consumed by
gRPC future but cloned in HTTP future, ignored TLS/procedures, default bind parity
gap. Worker reported unsafe restoration of unrelated formatting changes in its
own isolated worktree; main checkout dirty files remain present. Repair task
explicitly forbids restores/resets and requires submodule setup + finished tests:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-http-luna-r2-fix`.
Must not silently serve plaintext for configured TLS. No production HTTP parity
claim until validated.

Lifecycle round 1 returned commit 581d908bb, NOT integrated. Tests were only
launched (cold compilation incomplete). Parent review found impossible 300ms
update expectation under 1s throttle, closed-event busy loop, startup cleanup,
idempotence, potential async-trait Send compile issue and shutdown/mapping gaps.
Repair/verification assigned to isolated Luna task:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-peer-lifecycle-luna-r2-fix`.
It must cherry-pick initial implementation in its own worktree, repair in a
separate commit, finish actual tests/Clippy, then return both commit IDs.

Discovery Luna research completed: commit 3175f78b0, cherry-picked as 7b02c1af5.
Reviewed key Go config validation, provider role/status/port mapping and refresh
constants against source. Artifact: memory/matching-discovery-contract.md.
No production membership completion claimed; real Ringpop wire provider is absent.
Next bounded implementation task is running on Luna, isolated worktree:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-peer-lifecycle-luna-r1`.
Scope new peer-provider/resolver lifecycle + member mapping and tests; avoid
main/config/queue/history overlap. Must document wire interoperability as remaining.

User: "Okay just use luna from cli proxy provider". Live catalog contains
cliproxy/gpt-5.6-luna and cliproxy/gpt-6-luna. Chose newer
`cliproxy/gpt-6-luna`, through OpenCode provider instance `opencode`.
Cancellation requested for Beam maintenance and free-router HTTP tasks before
replacement. Do not delete their worktrees; Luna briefs request read-only review
of any partial work. Three new async isolated tasks are running:

- Maintenance: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-maintenance-luna-r1`
- HTTP: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-http-luna-r1`
- Discovery research: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-discovery-luna-r1`

The free-only restriction below is historical, superseded by this explicit
authorization. Source review, focused tests and compatibility integration gates
remain unchanged. Parent remains orchestrator/reviewer; actual slices delegated.

User requests orchestrator-led completion using OpenCode/OpenRouter free-model
subagents. Preserve service order Matching -> History -> Frontend -> Worker and
require actual Go compatibility evidence; no whole-service completion claims yet.

Pre-existing dirty files (not orchestrator edits):
- rust/crates/cadence-server/src/grpc_matching.rs
- rust/crates/cadence-server/src/matching_history.rs
- rust/crates/cadence-server/src/matching_registry.rs

These contain ongoing recordTaskStartedTimeout work. Preserve them during integration.

## Active T3-owned tasks

All use provider instance `opencode`. Async completion wakes the parent thread.

- Audit: `openrouter/nvidia/nemotron-3-ultra-550b-a55b:free`, read-only shared checkout.
  Task ID: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-audit-free-r1`
- Maintenance live configuration: `opencode/beam-free`, isolated worktree.
  Task ID: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-maintenance-free-r1`
  Scope: updateAckInterval, idleTasklistCheckInterval/maxTasklistIdleTime,
  maxTaskDeleteBatchSize; original Go semantics and focused regressions.
  Expected handoff: commit plus memory/matching-maintenance-live-config.md.
- HTTP listener: `opencode/deepseek-v4.1-flash-free`, isolated worktree.
  Task ID: `node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-http-free-r1`
  Scope: dedicated configured Matching HTTP startup/transport isolation; avoid
  queue/registry/history edits. Expected handoff: commit plus
  memory/matching-http-listener.md.

## Integration gates

Discovery Laguna attempt failed with account-wide OpenRouter error:
`Rate limit exceeded: free-models-per-day. Add 10 credits to unlock 1000 free
model requests per day`. No findings returned. Do not rotate OpenRouter models
against this shared daily limit, buy credits, or silently use paid workers.
Discovery research is blocked on free quota availability. Existing Beam
maintenance and OpenRouter HTTP task have not returned terminal results; HTTP
task_status confirmed running at this handoff, so do not duplicate its work.

Discovery Big Pickle attempt failed with rate limit, no findings. Replacement:
`openrouter/poolside/laguna-s-2.1:free`, isolated worktree, async,
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-discovery-contract-r4-laguna`.
Maintenance Beam task status checked during this handoff: still running.

Audit round 2 returned, but failed quality review: unsupported completeness
estimate, shared HTTP routes misidentified as dedicated listener, and wrote
ANALYSIS_REPORT.md/IMPLEMENTATION_PLAN.md despite read-only instruction. Those
artifacts were moved under memory/free-audit-r2*-unverified.md with warnings.
Source confirms Membership is injectable and main does not install it. No
implementation accepted from the audit; discovery contract needs further research.

Narrow discovery-contract follow-up uses `opencode/big-pickle`, isolated worktree:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-discovery-contract-r3-pickle`.
Only permitted artifact is memory/matching-discovery-contract.md; asks for exact
Go provider/config/update/eviction evidence, not interface-only parity claims.

HTTP round 2 (GLM) also failed with free-tier rate limit, no implementation.
Round 3 uses `openrouter/openrouter/free`, isolated worktree, async:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-http-free-r3-router`.

Audit round 1 failed with `Upstream error from Nvidia: Service temporarily
overloaded`, without findings. Reassigned read-only audit to
`openrouter/cohere/north-mini-code:free`, async:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-audit-free-r2-cohere`.
Replacement excludes maintenance/HTTP scope already assigned to other workers.

HTTP round 1 failed with provider `Rate limit exceeded. Please try again later.`
No implementation result returned. Reassigned the same bounded task to
`opencode/glm-5.3-flash-free`, isolated worktree, async:
`node:delegated-task:command%3Amcp%3A3a218386-d0a0-4ce4-8301-3b1173af99d3%3Adelegate-task%3Amatching-http-free-r2-glm`.

Inspect child diffs against Go, resolve conflicts without discarding existing
changes, run focused tests, workspace tests and strict Clippy, then source-frozen
full compatibility runner in tmux session `cadence-rust`. Catalog availability
does not prove provider requests succeed; handle actual free-tier failures without
silently switching workers to paid models. Children must not push or delegate.
