## Dissemination repair returned without validation 2026-10-18

Repair 187f4aeff atop 93d1d94af (equivalent 6c71b6774) fixes address-based bump and passes local source into full sync. No confirmed Rust checks due Cargo target lock; original-Go fixtures still omitted. Both remain UNINTEGRATED. Split narrowly:
- node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Adisseminator-go-fixtures-standard-18-20261018 (Go only, no Cargo)
- node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Adisseminator-repair-check-standard-19-20261018 (Rust checks, avoid active target lock)
Both Luna Standard accepted running. HTTP repair standard-16 checked running no pending question. Next combine actual oracle consumer and validated repairs before integration.

## Dissemination draft review 2026-10-18

Candidate 6c71b6774 returned 2 focused tests/Clippy/wire/member regression passed but NO dissemination Go oracle. UNINTEGRATED. Parent checked original disseminator.go: Go bumps current queue entry by address even if replaced since snapshot; candidate incorrectly skips. Full sync omits local source identity contrary MembershipAsChanges.populateSource. Also maxP float formula boundary parity needs Go vectors. Repair+original-Go differential assigned Luna Standard:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-dissemination-parity-standard-17-20261018
Accepted running. HTTP permissive repair standard-16 remains active. No new production discovery integration.

## Permissive HTTP TLS review 2026-10-18

Candidate 30805ee32 from t3-02f97237 returned all ten Go methods plaintext/TLS same port, 3 focused tests and strict server Clippy passing. NOT INTEGRATED: parent found one-shot 5-byte peek misclassifies fragmented TLS and accept serially waits sniff/handshake (slow client blocks all new connections). Direct dependency tokio-rustls has no lock update. Repair plus fragmentation/concurrent-client validation assigned Luna Standard:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Ahttp-permissive-repair-standard-16-20261018
Must verify revised default/disabled/enforced semantics from original Go, bounded handshake tasks/shutdown and locked build. Existing parent test edits from prior worker remain unstaged/preserved; integrate via reviewed commit later. Ringpop disseminator standard-15 remains separate active task.

## Deterministic member core integrated 2026-10-18

Final parity standard-14 passed original Go fixture regeneration/no drift, Rust oracle consumer, 4 unit tests, strict common all-target Clippy, Go wire oracle + 11 Rust wire tests. Integrated exact validated stack as 86a5c8221, dcd28da6f, f36646209, 0a8ee08c1, 429d7da02. Production Ringpop/SWIM still absent.
Next Luna Standard isolated implementation task:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-disseminator-standard-15-20261018
Scope deterministic change dissemination core with original-Go oracle; allowed bounded queue/counter slice if needed, no main wiring. HTTP permissive standard-11 checked running with no pending question; preserve visible parent test edits. Full accumulated workspace/mixed runner still not rerun on latest integrated source.

## Expanded oracle ready; final combined gate 2026-10-18

Consumer ce179937a ran original Go generator successfully and found exactly known precedence mismatch against UNREPAIRED candidate; other comparisons passed. Parent reviewed consumer assertion accumulation and failure assertion. Final isolated Luna Standard task combines base 7f86a56c3 + repair 610dd4fe9 + oracle 07b654686 + consumer ce179937a, reruns Go fixture generation/no drift, Rust differential/unit, strict Clippy and wire runner:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-final-parity-standard-14-20261018
Accepted running. Integrate only after confirmed combined passes. Consumer currently normalizes Some(empty) to None; pure checksum semantics same but nil-v-empty wire evidence is separate existing wire runner. No production SWIM claim.

## Repair verified; oracle comparison pending 2026-10-18

Repair-save standard-12 completed. Candidate base 940e8b468 + repair 610dd4fe91acd218c7820ca49f940065c45b3ff7 (equivalent original base 7f86a56c3). Parent reviewed Less=>false, Equal=>signed labels comparison and regression. Worker confirmed 4 focused tests, strict common all-target Clippy, existing wire runner with Go fixtures + 11 Rust tests, all exit 0. Still UNINTEGRATED pending expanded actual-Go differential consumer standard-13 (checked running). Next validation task must combine base + 610dd4fe9 + oracle 07b654686 + returned consumer commits, run differential tests against repaired implementation, then integrate only passing stack. HTTP standard-11 remains active; preserve parent test edits.

## Original-Go member fixtures returned 2026-10-18

Oracle candidate 07b654686 executed original unexported methods successfully in temporary package; not yet integrated. Parent review found coverage holes: precedence case uses higher status, nil maps collapse to empty, full status matrix absent. Assigned fixture coverage + Rust consumer task (implementation repair remains separate):
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-differential-consumer-standard-13-20261018
Luna Standard, accepted running. Repair-save standard-12 and HTTP permissive standard-11 checked running. No integration/parity claim yet.

## Parity round incomplete; split acceptance 2026-10-18

standard-10 stopped with uncommitted precedence repair at t3-0df1b5d9 (HEAD 2d27bcab0). Build still running in cadence-rust:ringpop-member-repair; no confirmed checks/oracle. Candidate remains unintegrated. Split into bounded Luna Standard tasks:
- node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-go-oracle-standard-11-20261018 — actual Go generator/fixtures only.
- node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-member-repair-save-standard-12-20261018 — recover repair, confirm focused tests/Clippy, commit; no duplicate oracle work.
Both accepted running. HTTP standard-11 remains separate active work; preserve visible test edits. Next combine oracle + repaired Rust and add consuming differential tests before integration.

## Member core review 2026-10-18

Candidate 7f86a56c3 remains UNINTEGRATED. Worker passed 3 unit tests but omitted Go differential fixtures/wire runner and could not confirm Clippy. Parent found concrete should_process_gossip bug: lower status precedence falls through to label-checksum comparison instead of immediate false (original member.go). Repair + full bounded acceptance assigned to Luna Standard:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-member-parity-standard-10-20261018
HTTP permissive standard-11 still running. Its test edits currently visible in parent checkout; preserve, do not stage unrelated active-worker source.

## Ringpop core continuation 2026-10-18

Research a905c8b8f completed and reviewed against original member.go checksum/merge rules. It was already on parent master when returned (despite isolated request); empty cherry-pick skipped, no changes lost. New worker explicitly told stay in assigned worktree.
Active member-core implementation with actual-Go differential fixtures:
node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-member-core-standard-9-20261018
Codex gpt-6-luna serviceTier=default. Scope pure checksums/merge primitives; no partial provider readiness. HTTP permissive standard-11 still running, no pending question. Production SWIM and full migration remain incomplete.

## Async continuation 2026-10-18

Checkpoint eac0ba82a pushed origin/master; Ringpop JSON boundary integrated bfce02825 + 3aed4e915. Prior permissive standard-10 interrupted, no pending runs.
Two new isolated Codex gpt-6-luna workers, serviceTier=default:
- HTTP recovery/implementation: node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Ahttp-permissive-standard-11-20261018
- Production Ringpop source contract/next slice research: node:delegated-task:command%3Amcp%3A4600deb0-cdd0-4f71-a308-c325211a2e8e%3Adelegate-task%3Aringpop-provider-contract-standard-8-20261018
Both accepted running, no pending questions. Read results on notification; do not duplicate. No broad accumulated regression yet on latest parent.

# Disk recovered / JSON validation resumed — 2026-10-17

JSON final round6 remained incomplete: candidate duplicate 88df45ace, no final
live probe/Clippy due filesystem 100%. Worker cleanup rejected by guard. Parent
inspected processes (no cargo/rustc active), used standard cargo clean on inactive
t3-7a9c12cc/rust/target: removed 18.0GiB generated files only, preserving source,
commits/worktrees/logs/fixtures. No direct deletion bypass used.
New Standard Luna task `ringpop-json-validated-standard-7` with task prefix
`node:delegated-task:command%3Amcp%3A4e49b790-fcf6-4e93-ab99-4c6e7e3eea46%3Adelegate-task%3A`
continues exact original brief, final live null/{} headers/cancellation/deadline
and existing Thrift/TLS probe + strict Clippy. Do not integrate candidate yet.

# TChannel JSON candidate requires final live rerun — 2026-10-17

994a12bde candidate returned: injectable JSON boundary, Go probe + Thrift probe
passed before final null/{} header refinement; final Clippy passed, live rerun
blocked by disk full (parent df shows 1.2GB available). NOT integrated yet.
Assigned Standard Luna `ringpop-json-final-standard-6` with task prefix
`node:delegated-task:command%3Amcp%3A44df19db-da43-408d-9a20-ab543ad502ac%3Adelegate-task%3A`: reclaim
ONLY reproducible build artifacts of terminal workers after process checks,
preserve all source/worktrees/evidence, reuse warm cache and rerun final Go JSON
and current Thrift/TLS regression plus strict Clippy. Partial work t3-0a3d52d6.
Actual SWIM/provider not implemented; injectable transport only.

# Enforced HTTPS integrated — 2026-10-17

Parent reviewed and cherry-picked feddeed2f + 514ed3059 (equivalent enforced TLS
candidate+repair). Three config tests, strict server all-target Clippy and complete
Go transport runner passed: all ten methods/TChannel/both HTTP paths, allowlist/
isolation, invalid/missing/mismatched cert failures, trusted Go TLS health and
plaintext TLS-alert rejection. Enforced missing/disabled TLS now rejects startup.
Advanced CA/client-auth/host-verification/permissive remain unsupported.
New Standard Luna `http-permissive-standard-10` (23bdfb7a prefix) implements exact
Go mode/default/disabled/permissive behavior with real bound Go-client probes.
Full accumulated workspace/mixed-service compatibility still due; no role complete.

# Beam rate limit / JSON task reassigned — 2026-10-17

beam-ringpop-tchannel-json-4 failed: "Rate limit exceeded. Please try again later."
No completed evidence accepted. Reassigned original bounded task plus partial-work
recovery to Codex Luna Standard: `ringpop-tchannel-luna-standard-5` (23bdfb7a prefix).
No Beam quota retries/rotation. Scope still injectable TChannel JSON boundary and
actual Go client tests; real SWIM/discovery remains separate and incomplete.

# Enforced TLS candidate review — 2026-10-17

Round8 candidate 81cda2028 implements typed config/basic enforced cert-key TLS.
Worker reports real trusted health handshake/plaintext rejection and strict
all-target Clippy. NOT integrated: parent found enforced + missing/disabled TLS
returns plaintext early, mode parser needs Go validation, old compatibility runner
still expects unsupported-TLS error. Assigned Standard Luna
`http-tls-review-standard-9` (23bdfb7a prefix) to fix and run complete live Go HTTP
probe and actual production TLS configuration path. CA/client-auth/permissive and
other advanced TLS settings remain explicitly unsupported; no full parity claim.

# Ack/idle integration; TLS continuation — 2026-10-17

Accepted maintenance split 0ef88e59c: ack+idle only, GC retains prior default.
143 Matching/2 GC tests + strict workspace Clippy passed in worker. Cherry-picked
with conflict resolved preserving isolation_provider_for_test plus existing staged
timeout helper annotation. No rejected GC nonpositive/saturation semantics added.
TLS round7 incomplete, no commit/finished tests; draft preserved t3-0ae06c81.
New Standard Luna task `http-tls-enforced-standard-8` (23bdfb7a prefix) narrowed to
working enforced TLS + typed cert config and real handshake/plaintext rejection
regression. Unsupported remaining modes/config must stay explicit; never plaintext
fallback. No TLS draft integrated, full accumulated checks still due.

# Plaintext HTTP transport integrated — 2026-10-17

http-live-standard-6 completed bc1f410e5/af5e5e8c6/efca7b995; parent reviewed and
cherry-picked. Real Go clients passed all ten Matching methods over TChannel and
both HTTP path spellings, allowlist/JSON isolation/TLS fail-closed checks, disabled
Matching HTTP listener checks and strict server all-target Clippy. Full accumulated
workspace/compatibility still due. JSON catch-all previously leaked Matching and is
now gated by matching_http_isolated; combined Frontend Meta semantics need review.
New Standard Luna task `http-tls-standard-7` (23bdfb7a prefix) implements typed
TLS/YARPC modes and real handshakes; verifies combined Frontend Meta behavior.
UP-interface bind selection and production membership still open. No role complete.

# Lifecycle integrated / maintenance withheld — 2026-10-17

Lifecycle validation round5: 9 tests + actual-Go address oracle passed; strict
all-target Clippy blocked by existing unused isolation_provider_for_test helper,
allow-dead_code run passed. Parent integrated candidate stack through 6c4035c42,
including 0a241e4e5 timing/drop/mapping repairs and be49d3459 oracle expectation.
No production Ringpop discovery/startup claim; full accumulated gates still due.
Maintenance round4 reported 143 Matching/5 GC tests and strict workspace Clippy,
but parent REJECTED InvalidGcBatchSize nonpositive rejection/u32 saturation as
Go logic deviations. No maintenance commits integrated. New Standard Luna task
`maintenance-go-semantics-standard-5` (23bdfb7a prefix) must preserve signed/range
semantics or split acceptable ack/idle work leaving GC explicitly unfinished.

# Ringpop wire models integrated — 2026-10-17

Parent reviewed and cherry-picked bbd18983b + 6fd77c22d. Empty label maps now
omitted for direct construction. Worker completed full actual-Go runner (11 Rust
tests) and strict common all-target Clippy. Scope models/fixtures only, not SWIM.
Followup Beam `beam-ringpop-tchannel-json-4` (23bdfb7a prefix) builds injectable
JSON TChannel transport boundary with actual Go-client probe, no fake production
membership responder. Preserve existing Thrift and running Luna scopes. No pushes.

# Beam wire model candidate review — 2026-10-17

Candidate bbd18983b adds common/ringpop_wire models, Go generator/fixtures and runner;
8 focused tests/common all-target Clippy reported passed. Not integrated yet.
Parent source review found Some(empty labels) serializes {} despite Go omitempty;
normalization only occurs on deserialize. Beam follow-up `beam-ringpop-model-review-3`
(23bdfb7a prefix) fixes direct construction, verifies timestamp/null handling and runs
full actual-Go fixture runner plus common Clippy. No duplicate research/test-fix
commits needed; parent already contains them/equivalents. No remote writes permitted.

# HTTP round5 / prerequisite integrated — 2026-10-17

Round5 returned bd1316159 (history test-only compile fix), dc7bd7e35 (duplicate
b53f0df9), e27016fa7 (duplicate ccbaaea6). Four matching_history tests passed;
HTTP work itself unfinished, no strict Clippy/live tests. Parent inspected and
cherry-picked ONLY bd1316159, unblocking tests for subsequent worktrees.
New task `http-live-standard-6` (23bdfb7a prefix), Standard Luna, narrowed to
real bound HTTP/Go-generated client/allowlist/JSON isolation evidence and fixes.
CARGO_BUILD_JOBS=1 required after reported OS compile kill. TLS/modes and exact
UP-interface flags remain explicitly open; no HTTP candidate integrated yet.

# Lifecycle repair result awaiting executable tests — 2026-10-17

Round4 returned repair 06041e5c3 after candidate d8776890a/56fd25c16/3a79b10e4
(cherry-pick duplicates f0f235e92/0f0f06962/fe2c5ca0e). Parent reviewed repair:
Go address-change-only timestamp, hint coalescing, Drop abort and parser changes.
Go oracle/check --lib/library Clippy passed; Rust tests/all-target Clippy blocked
by checkpoint timeout-test compile errors. Not integrated yet.
Assigned `membership-lifecycle-validation-standard-5` (23bdfb7a prefix), Standard
Luna, explicitly authorized minimal separate test-only fixes, reuse existing fix
if available, execute regressions/oracle/all-target Clippy and repair failures.
Preserved round4 worktree t3-312cf341. Real Ringpop remains absent.

# Beam wire contract result — 2026-10-17

Research d339c6774aebf498cbab553bd425d744e2ff15d8 accepted as source inventory
only after parent spot-check of join/ping/Change JSON fields and application errors;
cherry-picked into parent. No protocol implementation/parity claimed. Caveat:
plan suggests skipping empty Vec values, incorrect for Go fields without omitempty;
new task must correct this and distinguish nil=null from empty=[].
Worker reported pushing t3/ringpop-wire-contract despite explicit no-push brief.
No remote branch deleted. New Beam task `beam-ringpop-model-fixtures-2` (23bdfb7a
prefix) explicitly forbids Git network writes, implements isolated serde models
and actual Go golden fixtures, preserves nil/empty/tombstone/timestamp semantics,
and returns local commits plus focused test/Clippy evidence. Do not trust research
as exhaustive until generated Go fixtures verify its serialization assumptions.

# Beam Free worker authorized — 2026-10-17

User additionally authorizes OpenCode Beam Free. Luna remains Standard speed.
Live capabilities: opencode available/canRunChildTask, models list empty; explicit
opencode/beam-free delegation accepted running. Task suffix
`beam-ringpop-wire-contract-1` (23bdfb7a prefix), isolated research-only checkout.
Scope exact pinned Go Ringpop TChannel JSON join/ping/ping-req wire inventory;
only memory/ringpop-wire-contract.md permitted, commit artifact, no code changes.
Review source evidence before accepting. This does not replace active Luna tasks.

# HTTP follow-up blocked validation — 2026-10-17

http-parity-standard-4 completed partial work: 860344fb duplicates b53f0df9,
ccbaaea6 adds bind scoring and TLS contract findings. Not integrated or validated.
if-addrs lacks UP flags, TLS still unimplemented. Validation blocked by saved
timeout draft tests (MetadataValue Option mismatch and client() shadowing).
New Standard Luna task `http-parity-standard-5` (23bdfb7a prefix) explicitly owns
minimal prerequisite test fixes in a SEPARATE first commit, then completes real
HTTP/TLS/bind implementation/tests. Parent can integrate prerequisite separately.
Preserved worktree: /home/ubuntu/.t3/worktrees/cadence-rust/t3-b6a636f7.

# Maintenance result and review — 2026-10-17

standard-3 maintenance completed candidate 14df0c8c05eb00a761b40d4220d18cefe6a5a8d3;
141 focused tests and strict workspace Clippy reported passing. Not integrated.
Parent flagged live deadline vs flush fallback precedence, idle TTL/cadence sampling,
GC conversion/provider reads and lack of background-loop regressions; large formatting
diff also needs reduction. Standard Luna task `maintenance-parity-standard-4` with
23bdfb7a prefix now repairs and validates these against actual Go behavior. Brief
explicitly preserves checkpointed recordTaskStartedTimeout draft during overlap.

# HTTP listener result and continuation — 2026-10-17

standard-3 HTTP completed candidate b53f0df940581c8610857c88a7ebd58d8d4fe72a.
Worker reports 3 focused tests and strict server all-target Clippy passed.
Parent source review: actual configured separate listener and allowlist exist;
TLS only rejected, default bind still wildcard, no live bound-listener evidence.
Not integrated. Assigned codex Luna Standard continuation suffix
`http-parity-standard-4` (23bdfb7a task prefix): exact Go bind/TLS semantics,
real HTTP/client/isolation tests, explicit bounded gaps if TLS remains unfinished.
No whole-service completion claim.

# Membership lifecycle result and review — 2026-10-17

standard-3 membership completed: commits d8776890a, 56fd25c16, 3a79b10e4.
Worker reports 6 focused tests and strict server all-target Clippy passed.
Not integrated yet. Parent reviewed source and assigned exact Go parity follow-up:
refresh timing/pending hints, failed snapshot throttling, Drop during hung snapshot,
and actual-Go address parsing differentials. New task suffix:
`membership-lifecycle-review-standard-4` with round-3 prefix (23bdfb7a).
Codex Luna explicitly Standard speed. Candidate remains lifecycle abstraction only;
production Ringpop provider/startup integration and multi-process tests remain open.

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
