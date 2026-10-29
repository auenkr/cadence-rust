## Matching docs checkpoint — 2026-10-29

Shared master was clean at `52c4ec14261257242cab9a0f93a0c3f90cc8298a` before this docs-only update. Run [40841353614](https://github.com/auenkr/cadence-rust/actions/runs/40841353614) is terminal success on exact source `e84e51a1c5a08cc89df0a04ccf69a77e284bd1c4`: CI Clippy, formatting and tests passed; retained per-command exits are all 0. See `memory/evidence/matching-ci-recheck-20261029.md` for the inspected artifact and exact per-binary totals. In particular, semaphore manager binary = 14 passed; the separate native engine Go tests C binary = 16 passed, 4 ignored, 30.01 seconds. That duration is not evidence of a native service no-deadline 30-second regression; no such standalone native service result appears in this run. The no-deadline Go-handler case passed in its separate 15-test binary.

This evidence applies only to `e84e51a`. It does not validate later ownership regression `12c7099c3` or future native scenario/config fixes. Focused run [40842468415](https://github.com/auenkr/cadence-rust/actions/runs/40842468415) is owned by the future scheduler and is deliberately not inspected here; `721e196d31b2cd7876fc09086c8ac4b2a8995ba3` is its exact source while docs HEAD `1228a9e3f5bd05f486aae2e3ca72bede0d0e94a2` is not exact-validated. Frozen `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` remains unchanged and there are no native mixed-lane passes. Terra workers `b07bde6f` (config-map coverage) and `baad2545` (native scenarios) remain active; wait for integration before source freeze and fresh workspace validation. Matching remains incomplete.

## DNS accepted; explicit Luna restart — 2026-10-19

Main87e3f1352 (pushed) integrates DNS/SRV worker9ff5d527d; source/manifest/lock/fixture byte-identical checked. Final15focused, actual localGo/Rust lookup, injectedGo provider oracle and strictserverClippy pass; durable memory/evidence/ringpop-dns-provider/final.md. Linux cgo/pureGo/platform and resolver-config reload differences remain explicit audit gaps.

After user interruptions all previous native agents disappeared. Explicit fresh normal-speed gpt-6-luna workers now: recover_workers (DNS finished; NEXT new MatchingRuntime owner/main managed shutdown/shared TChannel source-only in existing DNS worktree branch from87e3f1352), outbound_luna (existingt3-6f14baa6, outbound sender/group finalfocused running; exclusively owns heavy build slot), cache_luna (matching-domain-metadata existing cache branch, source-only cache/Go oracle work). Parent remains integration/review. No native old discovery_review/relay_completion/state_transitions remain live. Recovery found ALL old paused PIDs terminal; no intentional stopped processes remain. Parent prior SIGKILL attempt timed out; do not signal stale PIDs. Host responsive again,25GBfree at recovery. User AGENTS.md uncommitted preserved.

Builds must use parent explicit grants, jobs1, tmux cadence-rust. Consistent targets: runtime/DNS matching-dns-provider/rust/target; outbound t3-975b4b2d/rust/target; cache must record one target once granted. Previous mistaken double-link caused severe host timeouts; don't rely on stale handoff/process observations. Matching incomplete; actual-production-binary mixed gate and combined frozen regression still outstanding.

## Metadata accepted; cache next — 2026-10-19

Main85f58ab2c integrates Go-compatible read-only SQLite domain metadata (worker67ea2980b -> df42703eb) and durable evidence (worker23476a912 ->85f58ab2c). Source/tests byte-identical after clean cherry-pick. Eight focused tests, actual Go seed/update -> Rust read-only -> Go reopen oracle, strict persistence all-target Clippy pass; memory/evidence/matching-domain-metadata contains final logs/exits. Known invalid-UUID/page0 panic-vs-error boundaries remain documented. No production cache wiring or Matching completion claimed.

state_transitions now assigned Matching domain cache source-only, existing worktree new branch from85f58ab2c, no main/registry edits. Metadata-first/page200/version ordering/callback/cache-miss/lifecycle original-Go semantics and differential required. DNS discovery final focused/system checks still running; Clippy passed after fixes. Relay outbound PING-REQ source/Go counter oracle complete, Rust focused test active. Two heavy builds accidentally overlap (DNS cargo1501430 and relay1506320); allow current jobs finish, no additional heavy build until terminal. DNS accidentally used own rust/target; preserve active outputs until complete, then clean inactive duplicates if necessary. Disk28GB free last checked.

Latest user AGENTS edit emphasizes orchestrator token efficiency; preserve their uncommitted AGENTS.md edit. Use normal-speed Luna implementation workers and batched acceptance reports. User instruction interruptions interrupted native workers; list_agents identified this and followup_task resumed them, do not mistake interrupted agents for running/stuck. Matching remains incomplete; last combined frozen source3c8c5f98a still does not cover main.

## Accepted relay and next work — 2026-10-19

Main4ae1e0fee integrates inbound PING-REQ baseline90358b167 and final repair4ae1e0fee (workere1f379685+a0c5c3b27). Parent verified production source/example/Go test/runner byte-identical to worker-tested version; cherry-picks conflict-free, diff-check clean, pushed. Evidence memory/evidence/ringpop-ping-req/: final4 Rust5focused tests, final5 fresh Go->Rust->Go probe/ReadJSON oracle, strict server all-target Clippy all exit0. Ready zero/null Go request panics on blank peer while Rust returns false: explicitly recorded edge divergence, not exact end-to-end parity. No combined regression yet and no Matching completion.

relay_completion now owns next source-only outbound PING-REQ/indirect group task in existing worktree, refreshed from4ae1e0fee. Must preserve error-only sender callback, false-status response merging, uncancelled late helpers and sparse pool behavior; actual Rust requester->Go relay->Go target required. No timers/main edits in this slice.

state_transitions has actual Go domain-store oracle compiling/seeding successfully; focused persistence tests running tmux cadence-rust (/tmp/cadence-domain-metadata-cargo.*), then shared DB assertions/update/reopen. It owns build slot. discovery_review DNS/SRV source ready, Go injected oracle passes; direct Linux getaddrinfo path added after NSS review, literal spelling/zone and empty-host behavior handled. Discovery receives next build slot, relay remains source-only. Linux cgo path does not prove all Go pure-Go resolver-selection parity. Whole migration remains active, current service Matching.

## Live orchestration checkpoint — 2026-10-19 (post-compaction)

Main starts at42d78bf1e, clean. Matching remains incomplete and the whole migration goal active. Native workers confirmed running; no duplicate tasks started.

- relay_completion in t3-6f14baa6 owns exclusive heavy Rust build slot. Final3 focused PING-REQ tests passed5; actual Go oracle/Go->Rust->Go integration rebuilding/running, then strict server Clippy. Logs /tmp/pingreq-relay-final3-*. Request decode repairs empty/null/single streaming JSON value, plus stopped/destroyed relay behavior and failure dissemination tests. Baselinee1f379685 and repairs remain unintegrated pending complete evidence/review.
- discovery_review in matching-dns-provider continues DNS/SRV implementation and injected Go fixtures. Hold Rust build pending relay release. Parent additionally requested actual literal-IP, hosts-file/NSS and IPv6-zone resolver compatibility review.
- state_transitions in matching-domain-metadata has source and focused tests for read-only Go-compatible metadata; real shared SQLite oracle work continues. Next heavy build slot after relay. Tests not yet run; no acceptance claim.

Disk35GB free. Keep heavy linking serialized, jobs1, long processes in tmux cadence-rust. Production binary/runtime gate remains outstanding; last frozen full regression3c8c5f98a does not cover main. Next inspect/integrate completed relay, then indirect-probe/runtime slice, continue domain and DNS checks. No History migration until full Matching gate.

## Authoritative integration checkpoint — 2026-10-19

Main6977c326d now integrates timer core a1f802896 (worker8e4e993eb) and static/file discovery b73abb5ed+c735426a3+6977c326d (worker056ee6890+0eba7a373+481471c4f). Parent checked integrated source/tests byte-identical to worker-tested versions. Timer actual pinned-Go differential,3Rust focused,strictcommonClippy passed; accepted log memory/evidence/ringpop-state-transitions/accepted.*. Discovery actualGo oracle,6Rust focused,strictserverall-targetClippy passed; evidence memory/evidence/ringpop-bootstrap-provider/parity-final-*. No full combined regression on main yet. Matching still incomplete.

Worker reuse/new independent tasks (normal-speed Luna):
- /root/relay_completion: saved t3-6f14baa6, final focused realGo->Rust->Go success/failure +Clippy underway, heavy build slot. Fresh probe harness/advertised listener identity repaired, no accepted relay commit yet.
- /root/discovery_review: NEW DNS/SRV implementation task from6977c326d; own discovery module/resolver/oracles, no main/SWIM overlap. Source work first; hold heavy builds pending relay release.
- /root/state_transitions: NEW read-only Go-compatible SQLite domain metadata store/codec from6977c326d, needed by Matching shared domain cache. Own persistence+Go DB interoperability tests; no Frontend/main/cache migration. Source work first; hold heavy builds pending relay release.

Important source findings preserved: Go JSON validates entire syntax before type errors; malformed UTF8 and lone surrogates replaced only in strings; root oversized numbers are type errors; Go highprecision time.ParseDuration float fraction rounding differs from rational truncation. Timer cascade at15s is real (faulty callback at5s schedules10s later), NOT an event to deduplicate. Core runtime adapter must implement it; not yet wired. Outgoing indirect ping Go inconclusive guard compares errors to CONFIGURED size, not actual selected helper count, so sparse helper pools fall through to suspect; preserve quirk when porting.

Actual cadence-server binary production gate remains memory/matching-production-binary-gate.md; current mixed test starts an ignored test executable. Last full frozen evidence3c8c5f98a does not cover current integrated source. Build concurrency caused10min relay compile timeout; processes verified terminal, build slots now serialized. Disk35GBfree,975/e426/a33 preserved. Full migration goal active; do not start History before Matching completion audit.

## Authoritative continuation checkpoint — 2026-10-19

Parent source365d9b219. Whole migration goal remains active; Matching incomplete, no History migration until full service parity and mixed original-Go verification. Current orchestration uses native Luna workers (normal Standard speed), not stale T3 task71/72 IDs owned by another thread.

Active workers:
- `/root/discovery_review`, isolated `t3-367ce881`, baseline056ee6890+0eba7a373 and retained uncommitted decoder changes. Expanding actual Go raw-byte, oversized number, surrogate and whole-document syntax-before-type oracles. Fresh post-final-edit Rust focused tests and strict server Clippy still required. Nothing integrated.
- `/root/relay_completion`, isolated `t3-6f14baa6`, baselinee1f379685 and retained relay/harness changes. Real Go->Rust->Go PING-REQ success/failure/state/dissemination checks, exact fresh probe selection. Nothing integrated. Relay intentionally ignores reverse full sync per actual Go handler.
- `/root/state_transitions`, isolated `ringpop-state-transitions` fromec059ca8e. New deterministic common timer core and actual pinned-Go clock oracle. Parent required deadline ordering across addresses and token/locking contract; runtime integration remains open.

Builds checked live (no stale lock inference): relay Cargo PID1337500, discovery Cargo1337885/rustc1337912, common Cargo1339966 and Go oracle1341378. Linkers are active; host IO pressure high. Do not start duplicate jobs or cancel just due observation timeout. Long jobs tmux cadence-rust, jobs1. Native worker status remains running.

Storage cleanup recovered36.9GB;36GB currently free. Active975/e426+a33 caches preserved. Old ce1e72cb target removed, source remains. Exact report memory/storage-recovery-20261019.md.

Parent added memory/matching-production-binary-gate.md: existing mixed tests launch ignored matching_delivery test executable, not actual cadence-server main. Main still loopback History/snapshot-only domains/unmanaged maintenance/no production membership wiring. Must add actual binary validation and complete production dependencies before service-complete claim. Last full frozen regression remains3c8c5f98a (34 stages/568 tests), not current source. Freshness issue also identified in tests/run-ringpop-outbound-ping-compat.py (reuses global /tmp probe); repair before relying on future evidence.

Next: review and integrate only finished validated worker commits; assign outgoing indirect probes/DNS-SRV/runtime-owner and remaining Matching production gaps in independent bounded slices, then full actual-binary mixed recovery audit. Preserve source/evidence and push checkpoints after compaction.

## Authoritative resume checkpoint — 2026-10-18

Parent source 0a6b6b336; Matching remains incomplete. Service order Matching -> History -> Frontend -> Worker. Workers use Luna normal speed, serviceTier=default, isolated async worktrees. No future-service migration yet.

Integrated production writer live batch/timeout and signed capacity/rendezvous fixes; outbound JOIN/PING, selector, concurrent JOIN group and asynchronous cancellable JoinCluster retry controller. Latest controller commits 17d7b6015/b5b783bbe. Ready/gossip/startup wiring remains open.

Latest full frozen validation is source 3c8c5f98a: 34 compatibility stages, 568 tests passed (2 ignored), strict workspace Clippy; Rust Matching with original Go Frontend/History/Worker and no Go Matching. Later JOIN group/controller slices have focused Go interoperability checks. Actual SIGKILL/restart with persisted task and original Go peers passed; artifact selection repaired in 593182641.

Discovery candidate d55ad694aa3e924387c7390f021a8aaed8c4e5f2 (worktree t3-8bb4397c) is NOT integrated. Review null JSON compatibility, oracle coverage, Go source overlay placement and config parsing before acceptance. DNS/SRV/runtime remain open.

Active task matching-ringpop-ping-req-standard-69-20261018 is running, no pending question. Task prefix node:delegated-task:command%3Amcp%3Ac49dd1a0-b936-47ad-840e-e6e5f75e64a4%3Adelegate-task%3A. Implements real inbound relay with actual Go requester/target; no scheduler/main wiring. Do not duplicate.

Disk only 4.4GB free: inspect active processes/caches before building; preserve sources, commits and evidence. See orchestration-active.md and matching-production-gap-audit-20261018.md. Next finish discovery review/repair and relay review, then remaining production membership wiring and audit, rerun combined mixed validation before History. Historical notes below are superseded.

## Resume checkpoint — 2026-10-18 (after handoff)

Matching remains current and incomplete. Follow service order Matching -> History -> Frontend -> Worker. Root AGENTS.md already records service-by-service mixed original-Go testing and storage rules.

Parent source fbe5b6fbc is clean. Integrated since frozen combined regression: inbound JOIN/PING, checked outbound TChannel JSON, live signed GC config, writer original-Go oracle. Latest full combined evidence remains source3072698e: 34/34 compatibility stages,524 tests,strict workspace Clippy,mixed original Go Frontend/History/Worker + Rust Matching with no Go Matching. Later slices have focused validation only.

Writer repair41 returned INCOMPLETE, uncommitted, and edited server matching_writer.rs instead of persistence candidate actor. Preserve worktree t3-3dfb4a95, do not integrate it. Recovery43 explicitly starts candidate6c1bd6fa409ec4eabaf792b0fcb9b0ecf8fcce39, repairs persistence primitive and executes real Rust-vs-Go traces before queue integration.

JOIN state repair42 remains running; candidate9770e3870 is not integrated. Must record local accepted refutation and propagation max events; preserve newest integrated transport parser when transplanting.

Full task prefix node:delegated-task:command%3Amcp%3Ac49dd1a0-b936-47ad-840e-e6e5f75e64a4%3Adelegate-task%3A applies to matching-join-state-parity-standard-42-20261018. Recovery43 task ID is recorded in orchestration-active.md after acceptance. Workers Luna normal speed serviceTier=default. No duplicate active work. Disk33GB free; active caches preserved.

Next: review completed slices against actual Go, integrate only validated code, wire accepted writer primitive into durable queue, finish discovery and remaining production audit gaps, rerun frozen full combined validation. No service completion claim.

## Current checkpoint — 2026-10-18

Integrated Ringpop injectable TChannel JSON boundary as bfce02825 and deadline repair as 3aed4e915. Worker validated 4 TChannel tests, strict all-target Clippy, actual Go JSON headers/null/empty, deadlines, malformed JSON, application errors and unknown service/method; ten-method Matching TChannel/HTTP and enforced TLS regression passed. This is NOT production Ringpop/SWIM.

HTTP permissive task standard-10 ended interrupted, no pending child runs; recover its work before implementation. Existing enforced TLS, maintenance ack/idle and membership lifecycle integrated. Full combined workspace/mixed runner has not been rerun on latest integrated source.

All services remain incomplete. Continue Matching -> History -> Frontend -> Worker. Remaining Matching includes real Ringpop discovery, production wiring, permissive/advanced TLS, exact interface UP selection, live GC signed backend semantics and other documented config/shard-distributor gaps. Never claim full migration from focused adapter tests.

Workers: Codex gpt-6-luna with explicit serviceTier=default (normal speed). Long commands in named windows of tmux cadence-rust; CARGO_BUILD_JOBS=1, reuse warmed caches, no broad formatting or worker remote writes. Disk currently 12GB free; preserve source and evidence.

See orchestration-active.md and ringpop-tchannel-next.md for evidence/history.

## User-requested progress checkpoint — 2026-10-17

Checkpoint includes the previously uncommitted recordTaskStartedTimeout draft in
grpc_matching.rs, matching_history.rs and matching_registry.rs. This draft is
UNVALIDATED: no passing compile/test evidence is claimed. Review and finish it
before treating it as an accepted Go-parity change. Saved unverified audit reports
retain their warning headers and are not authoritative migration plans.

Parent remains orchestrator; implementation uses codex/gpt-6-luna with explicit
Standard speed (target.options.serviceTier="default"), never Fast/Priority.
Current three isolated tasks were checked running, with no pending questions:
- maintenance-codex-luna-standard-3
- http-codex-luna-standard-3
- membership-codex-luna-standard-3
Full task ID prefix:
`node:delegated-task:command%3Amcp%3A23bdfb7a-603d-4d45-b778-557c3aff6443%3Adelegate-task%3A`
Worktrees respectively: t3-ef455bb6, t3-7a9c12cc, t3-77b82231 under
/home/ubuntu/.t3/worktrees/cadence-rust/. They recover previous partial work;
no returned implementation from these rounds has been integrated.
Earlier standard-2 workers were repeatedly cancelled before completed tests;
the returned statuses do not explain the cancellation cause. Preserve their work.

Next: read task_status on completion, inspect returned diffs against original Go,
integrate only reviewed commits without losing the timeout draft, finish focused
checks and full SDK/mixed-service compatibility through tmux cadence-rust.
All four service roles remain incomplete. Continue Matching -> History -> Frontend
-> Worker, with the final requirement of a Rust cluster needing no Go server.
See orchestration-active.md for task history and recovery instructions.
Checkpoint validation: git diff --check passed; no builds/tests run for this save.

## Auto-config hint checkpoint (2026-10-17)

Pushed e559bfa73. Every Matching poll now carries Go's `types.AutoConfigHint`:
`matching.enableClientAutoConfig` (default false, domain/task-list/task-type
filtered) plus the elapsed poll wait, applied to durable delivered
decision/query responses, empty decision responses, activity responses
(delivered = activity type, empty = decision type per Go's engine.go:937 quirk)
and the non-durable embedded-engine polls, which previously always reported
`None`.

FULL compatibility runner PASSED all stages (Go differentials, public/internal
gRPC, Go SDK over TChannel, Go-services delivery, mixed Go
Frontend/History/Worker + Rust Matching):
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-auto-config/results.json,
/tmp/compat-autoconfig.exit = 0. Workspace tests (162 server lib) and strict
workspace/all-target Clippy pass.

Whole Matching remains incomplete. Remaining live config: updateAckInterval,
idleTasklistCheckInterval/maxTasklistIdleTime, maxTaskDeleteBatchSize,
maxTaskBatchSize/outstandingTaskAppendsThreshold, appendTaskTimeout,
recordTaskStartedTimeout, shutdownDrainDuration. Larger gaps: production
membership discovery/runtime wiring in main, dedicated Matching HTTP listener,
shard-distributor integration. See matching-config-next.md.

## Live Matching reader/dispatch config checkpoint (2026-10-17)

Pushed 11a6cbc4a. The native Matching owner now reads Go's live
`matching.getTasksBatchSize` (with the non-positive fallback to 1000),
`matching.readRangeSize` (unfiltered integer) and
`matching.asyncTaskDispatchTimeout` where Go's `taskReader` reads them, and
`matching.minTaskThrottlingBurstSize` once per owner dispatch limiter. The
persistence scan range signature is now `u32` so a non-positive configured
range makes no progress in the bounded ten-step scan, like Go.

FULL compatibility runner PASSED all stages including Go differentials, public/
internal gRPC, the Go SDK over TChannel, Go-services matching delivery and the
mixed Go Frontend/History/Worker + Rust Matching probe:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-reader-config/results.json
and /tmp/compat-reader-cfg.exit = 0. Workspace tests (161 server lib) and strict
workspace/all-target Clippy pass. This run also covers the two preceding
commits (membership channel cache and live override-RPS), which had only unit
validation before.

Whole Matching remains incomplete. Next candidates: remaining live config
(`matching.updateAckInterval`, `matching.idleTasklistCheckInterval`,
`matching.maxTasklistIdleTime`, `matching.maxTaskDeleteBatchSize`,
`matching.maxTaskBatchSize`, `matching.outstandingTaskAppendsThreshold`,
`matching.appendTaskTimeout`, `matching.recordTaskStartedTimeout`,
`matching.enableClientAutoConfig`), production membership discovery/runtime
wiring in main, dedicated Matching HTTP listener, and the shard-distributor
integration. See matching-config-next.md.

## Live override-RPS / membership channel cache checkpoint (2026-10-17)

Pushed 5db03a5eb and 88abc0766. Matching now caches one lazy gRPC channel per
owner address (retained only while the address stays in the ring) and honors
Go's live `matching.overrideTaskListRps` precedence on both decision and
activity polls, reporting the worker rate to the shared dispatch limiter.

A re-entrancy defect was found during validation: the first `poll_rate`
implementation held the admin dynamic-config mutex while calling
`limiter.report_limit`, but the owner's live partition-count provider locks the
same mutex, so every poll self-deadlocked and the full unit suite hung. The
override read is now scoped to a block; a new regression uses a
`try_lock`-based partition provider so the bug fails loudly instead of hanging.
All 159 server lib tests and strict workspace/all-target Clippy pass. The broad
compatibility runner has NOT been rerun for these two commits. Whole Matching
remains incomplete; next scoped gap is live reader/dispatch configuration
(getTasksBatchSize, readRangeSize, asyncTaskDispatchTimeout,
minTaskThrottlingBurstSize). See matching-config-next.md.

## Verified Matching transport / membership checkpoint (2026-10-17)

Current implementation commit ce242f295 is pushed. All 154 server tests and
strict workspace/all-target Clippy pass. Original Go-generated native Thrift
clients pass all ten methods via TChannel and both HTTP mount spellings.
Go/Rust ring placement agrees across 4,590 lookups. Real cross-host gRPC tests
cover backlog delivery, sync adds without duplicate persistence, cancellation,
and query completion on the accepting host.

Final boundary and mixed Go/Rust probes BOTH PASS:
/tmp/matching-mixed-final.exit = 0; artifacts matching-deadline-go-boundary and
matching-deadline-mixed. Mixed test covers SDK workflow/activity completion,
query dispatch, isolation, native partition config, idle unload and recovery.
Broad runner's other 30 stages passed in matching-transport-membership-verified;
its failed boundary stage was rerun successfully with source frozen. Earlier
workspace suite passed; final server suite/Clippy and native transport were
rerun after deadline changes. This is combined verification, not a claim that
the earlier failed runner invocation exited zero.

Whole Matching remains ACTIVE / INCOMPLETE. Continue only Matching. Next audit:
local parent poll forwarding must use full handler policies (query pickup,
per-hop deadlines/config); production membership discovery/runtime wiring;
shared persistence/domain cache; shard-distributor and remaining live config;
dedicated configured HTTP listener, metrics and outgoing channel limits/cache.
See matching-membership-next.md for detailed evidence and outstanding scope.

## Latest Matching deadline checkpoint (2026-10-17)

Pushed ce242f295: Go-style empty-poll deadline tailroom and live long-poll
expiration in durable and in-memory Matching. All 154 server tests and strict
workspace Clippy pass. Native Go Thrift probe passes after durable deadline
change. Whole-service scope remains active and incomplete.

Broad artifacts matching-transport-membership-verified passed static, native
Thrift, listener isolation, standalone public/internal gRPC and Go SDK checks,
but Go delivery boundary failed after live source edits invalidated its prebuild
(activity response empty, likely schedule-to-start expiry during recompilation).
Source is now frozen. Running independent final Go boundary + mixed SDK probes
in tmux matching-mixed-final: /tmp/matching-mixed-final.log and .exit; artifacts
matching-deadline-go-boundary and matching-deadline-mixed. Inspect authoritative
results before claiming compatibility. See matching-membership-next.md for all
remaining ownership/provider/config and forwarding audit work.

## Current Matching continuation (2026-10-17)

Native Matching Thrift is implemented and original Go clients pass all ten
methods via TChannel, /matching and /matching/. Commits 6eed2c7ef and 027792fb1.
See matching-thrift-native.md.

Membership placement, ownership guards/unloading and cross-host forwarding are
being implemented; see matching-membership-next.md for exact status, live test
handles and required remaining work. Whole Matching is NOT complete. Important:
new dynamic-config audit found 29 unwired keys previously assumed covered.
The injected membership implementation is NOT configured by main yet.

## Matching whole-service continuation (2026-10-17, current)

Read `matching-standby-failover-query.md` before older Matching completion notes.
User requires ALL Matching first, not a single-node completion bar. Current
work adds durable manager listing, standby query-only polls and task completion,
local domain-failover disconnects, cancellation-safe query waiter accounting,
single query delivery and accepting-partition query tokens. Original Go oracle
produced 72 standby state decisions in tests/fixtures/matching-standby.json.
Workspace tests (145 server tests), strict workspace Clippy and the new Go
standby/query-version differential pass. Full runner continues in cadence-rust:
/tmp/matching-verified.log, /tmp/matching-verified.exit; matching-standby-verified
artifacts. Persistent whole-Matching goal is ACTIVE.
Do not claim the whole service is complete: native Matching Thrift transport,
cross-host forwarding/membership/ownership and production config/cache/metrics
still need porting and validation. Keep work on Matching.

Matching strong/sticky query version gates (2026-10-17): read
matching-query-next.md. `SupportsStickyQuery` is now ported exactly (uber-go/
uber-java/empty impl >= 1.0.0), and the previously missing
`SupportsConsistentQuery` gate (>= 1.5.0; empty impl treated as supported) is
enforced: `QueryOutcome::Worker` carries worker version info and
`query_workflow` fails a strong-consistency query from an unsupported worker
with `ClientVersionNotSupportedError`. Unit tests cover both gates. Committed
after full lib tests + strict Clippy.

Matching semaphore manager + registry (2026-10-17): read
matching-semaphore-next.md first. Ports Go service/matching/semaphore:
Identifier, Registry (get_or_create/unregister/by_identifier/all_managers/
sweep_idle), and Manager (Created->Starting->Running->Stopped, startup scan
rebuild, conditional grant with confirm-hold/stale-index handling, NoSlot).
Adds the persistence token/owner row surface (scan/get_ownership/grant/release)
and rewires AcquireSemaphore with Go-compatible error mapping; serve_with runs
a 30s idle sweeper. 7 unit + 1 gRPC end-to-end test, full cadence-server --lib
=131 passed, strict Clippy clean. Committed with the adaptive scaler in
d5a00e110.. Remaining genuine single-process gaps: standby task completion,
strong/sticky query version gates, handler error taxonomy, domain-failover
poller disconnect, Thrift transport. Multi-host routing/membership out of scope.

Matching adaptive scaler + isolation balancer (2026-10-17): read
matching-scaler-next.md first. Ports Go tasklist/adaptive_scaler.go and
tasklist/isolation_balancer.go: Sustain, upscale/downscale thresholds with
hysteresis, dense write-map rebuild, read-drain, isolation-group goals, and a
per-root scaler daemon (live config, weak-state lifecycle, versioned partition
updates), plus the manual-update guard. 7 scaler unit tests + registry
integration/guard tests pass, full cadence-server --lib =123 passed, strict
Clippy clean. Uncommitted until this checkpoint. Remaining genuine single-process
gaps: standby task completion, strong/sticky query version gates, handler error
taxonomy, semaphore lifecycle, domain-failover poller disconnect, Thrift
transport. Multi-host routing/membership still out of single-node scope.

Matching completion (2026-10-16): read matching-sync-forwarding-next.md first.
Committed d501f3779 (forward limits, backlog waits, query walk, standalone
durable wiring) on top of ca062c861/a9531588d. FULL 31-stage compatibility
passes on the committed tree (matching-complete artifacts,
/tmp/opencode/full-compat.exit=0), 92 focused matching tests + strict Clippy
clean, standalone --data-dir smoke + restart pass. Single-node matching is
done; only multi-host membership/routing and partition autoscaling remain
(explicitly out of single-node scope). Next: History service.

Previous sync/local-forwarding continuation (2026-10-16): read
matching-sync-forwarding-next.md first. Sync committed a9531588d; uncommitted
local forwarding draft passed workspace tests (105 server unit tests including
cancellation regression) and workspace strict Clippy. Full runner now on Go
differential stages, cadence-rust:forward-gates,
/tmp/opencode/forward-gates.log and .exit. Check outcome; no complete-service
claim. Cross-host routing, backlog/query forwarding and remaining gates open.

Latest partition protobuf follow-up (2026-10-16): read
matching-native-partition-next.md. Public/internal partition config map encoders
now preserve explicit empty message presence for pinned Go. Exact-byte Rust
tests, actual-Go nil/presence decoder checks and native manager Go-oracle fixture
test pass. Expanded mixed Go services/Rust Matching passes nativePartitionConfig
through Go Frontend and all existing SDK scenarios. Strict all-target Clippy
passes. Markers partition-presence.exit, partition-presence-mixed.exit and
partition-presence-clippy.exit under /tmp/opencode are all0. Full30 baseline
previously passed on native manager06c6996fa. Sync module still unwired.
No service complete; continue sync/forwarding and remaining gates.

Latest native partition manager continuation (2026-10-16): read
matching-native-partition-next.md first. Native manager caches/startup/update/
refresh/local notifications, RPC Describe and isolation consumers now use leased
queue metadata. Go audit fixed dispatch division to READ partitions, while list
enumeration still uses configured writes. Cache-enable flag controls cached
read counts and typed drained-write rejection. All30 compatibility stages passed:
/tmp/opencode/native-partition-full.exit=0;409 workspace tests and strict Clippy,
existing SDK and mixed Go services/Rust Matching passed. Notifications are still
local-only pending cross-host routing/membership. Sync source and protobuf
partition-presence macro are UNWIRED scratch files, not compiled/validated.
Next wire and test partition map presence + native actual-Go oracle, then sync
matching completion/cancellation coordination and remaining gates. No role done.

Latest native partition storage continuation (2026-10-16): read
matching-native-partition-next.md first. DurableTaskQueue partition config
read/update API now serializes with ack/lease writes and only publishes after
fenced persistence succeeds. Three new regression tests and extended actual-Go
SQLite interop passed; 406 workspace tests and strict all-target Clippy passed.
/tmp/opencode/partition-storage-verify.exit=0 and
/tmp/opencode/partition-native-interop.exit=0. RPCs still use executions metadata;
next migrate manager caches, startup/refresh/notifications and all consumers
together before switching native RPC storage. Sync matching/forwarding and all
remaining service gates remain incomplete. No complete migration claim.

Latest verified continuation (2026-10-16): read
matching-isolation-dispatch-next.md first. Group-aware backlog buffers/handoffs,
Go-oracle policy/timing fixtures, live drain/recency checks implemented. A mixed
probe exposed zero-metric protobuf map presence incompatibility with pinned Go;
custom TaskListStatus encoding fixes it without IDL changes. Current workspace:
401 tests (87 server) and strict all-target Clippy passed. Full30-stage runner
PASSED: /tmp/opencode/isolation-final.log and .exit; artifacts
matching-isolation-dispatch-final. Mixed probe isolationDispatch and Go SDK pass.
Follow-up enable-toggle reload is implemented with3 focused tests passed;
full30-stage verification PASSED at /tmp/opencode/isolation-reload-full.log and
.exit=0. Current workspace403 tests (89 server) and strict all-target lint pass.
Read matching-isolation-dispatch-next.md for details before continuing.
Reload committed0c4c63323; backlog dispatch committed63f374479. Follow-up
dispatch-only partition metadata rewrite now implemented with21 Go oracle cases
and persisted-original regression passing. Full runner in progress:
/tmp/opencode/isolation-metadata-full.log and .exit; artifacts
matching-isolation-metadata-full. Await result before claiming full validation.
First metadata full run hit ENOSPC at server build after tests/lint/oracles
passed; cleared36GB generated rust/target/debug/incremental ONLY. Latest retry
uses CARGO_INCREMENTAL=0: /tmp/opencode/isolation-metadata-final.log and .exit,
artifacts matching-isolation-metadata-final. Check this run, not failed full.
Metadata retry PASSED all30 stages, marker0;403 workspace tests,89 server,
strict all-target lint, Go SDK and mixed isolationDispatch pass.21 metadata
rewrites match actual Go. Next substantial Matching gap: sync matching and
forwarding/partition routing; durable partition config remains in execution
metadata and must migrate to native queue metadata (already blob-codec capable).
Matching still incomplete; sync matching,
forwarding/partition routing/scaling, semaphores/membership/standalone remain.

Prior verified continuation (2026-10-16): read matching-isolation-next.md, then
matching-config-next.md. BOTH full runners PASSED all29 stages. Latest frozen
isolation reporting run:395 workspace tests,84 server lib tests, strict Clippy,
Go SDK and mixed Go services/Rust Matching passed;
/tmp/opencode/cadence-isolation-full.exit=0. Commits f4bb9a5d5 and e57d4c077.
Task-list/type config scoping and isolation group observations implemented.
No role complete; continue remaining Matching gates before History.

Prior continuation: Task-list/type
filtered partition-count and QPS providers implemented; 2 persistence tests,
83 server lib tests and strict all-target workspace Clippy passed. Full runner
running under tmux cadence-rust; /tmp/opencode/cadence-scoped-full.exit is the
completion marker. No full pass claimed yet. Next isolation-group poller
attribution/Describe metrics, then remaining Matching gates. No role complete.

Latest scalar rate-report checkpoint:11 actual-Go sequences,30 common tests and
strict all-target workspace Clippy passed. See matching-rate-next.md. New runner
has28 stages; full runtime last passed367 tests/27 stages before scalar addition.
Next complete common/clock/ratelimiter reservation/timing semantics, then wire
shared queue-owner limits into reader pre-handoff wait, activity reports and
DescribeTaskList. Scalar aggregate currently unconnected to dispatch enforcement.
Matching remains active; no service complete.

Latest checkpoint: queue writer/DB/state lock separation validated (367 tests,
strict Clippy, all27 compatibility stages; /tmp/cadence-lock-full.exit=0). See
matching-db-lock-next.md. Next rate-report oracle/scalar draft under/tmp documented
in matching-rate-next.md; oracle was interrupted, rerun serially before claiming
it validated. All services remain incomplete; Matching is active.

Latest: Matching rewrite retry validated (366 tests, strict lint, all27 stages;
/tmp/cadence-rewrite-full.exit=0). See matching-rewrite-next.md. Next concrete gap:
queue state lock spans lease backoff, blocking existing-task admission; source
audit and planned correction in matching-db-lock-next.md. Then rate limiter
port (matching-rate-next.md) and remaining Matching gates. No role complete.

Latest: Matching background reader/local backlog handoff validated and connected
to native owners. See matching-background-next.md. 364 tests, strict Clippy,
all27 compatibility stages incl mixed Go services passed. No role complete.
Next: writer-backed rewrite retry, then rate/isolation/sync/query/forwarding gates.

Latest checkpoint: Matching reader query retry and independent reader lock.
See matching-reader-next.md. 349 tests, strict lint, all 27 compatibility stages
including mixed Go services passed. User reconfirmed complete Matching before
History; no service is complete. Next: background reader and matcher rendezvous.

# Current resume checkpoint (2026-10-16) — arrival QPS tracker

Read matching-qps-next.md first. Go's `common/stats` EMA fixed-window QPS tracker
is ported (`cadence_common::qps::EmaQpsTracker`, clock-driven with per-window
attribution) and wired into the durable matcher: non-forwarded
`AddDecisionTask`/`AddActivityTask` report arrivals (grouped by the task's
`isolation-group`), and durable `DescribeTaskList` reports
`newTasksPerSecond`. Window length comes from `matching.qpsTrackerInterval`
(default 10s). 35 cadence-common + 81 cadence-server lib tests and strict
all-target Clippy pass. The full 29/29-stage runner passed:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-qps-full/results.json.
Next Matching gate items: `LoadBalancerHints`,
`isolation_group_metrics`, forwarding, partition routing/scaling, isolation
groups, semaphores, membership/ownership, standalone lifecycle, and wiring the
durable matcher into the production standalone server.

# Current resume checkpoint (2026-10-16) — sticky availability

Read matching-sticky-next.md first. Sticky task lists now fail fast like Go: a
durable `AddDecisionTask`/`QueryWorkflow` on a sticky list with no poller in the
last 10s returns `StickyWorkerUnavailableError` (gRPC UNAVAILABLE + proto
detail), so History clears stickiness and retries non-sticky instead of leaving
the task/query stranded. 80 cadence-server lib tests + strict all-target Clippy
pass; full 29/29-stage runner PASSED, including the Go SDK sticky scenario
against Rust Matching:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-sticky-full/results.json.
Remaining Matching gate items: forwarding, partition routing/scaling, isolation
groups, semaphores, membership/ownership, standalone lifecycle, and wiring the
durable matcher into the production standalone server.

# Current resume checkpoint (2026-10-16) — durable query delivery

Read matching-query-next.md first. The durable matcher now delivers Go-style
direct queries: `QueryWorkflow` offers an ephemeral query task, a decision
`PollForDecisionTask` receives it on the same long poll (ahead of the durable
backlog) with a query task token and mutable-state metadata, and
`RespondQueryTaskCompleted` completes the waiting call. 79 cadence-server lib
tests + strict all-target Clippy pass. Mixed Go Frontend/History/Worker ->
Rust Matching PASSED with a new `queryDispatch` scenario (the existing Go SDK
run in the same probe also passed):
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-query-mixed/probe.log.
The full 29/29-stage runner also passed:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-query-full/results.json.
Next Matching gate items: forwarding, partition routing/scaling, isolation
groups, semaphores, sticky availability, membership/ownership, standalone
lifecycle, and wiring the durable matcher into the production standalone
server. All four roles remain incomplete.

# Current resume checkpoint (2026-10-16)

Read matching-partitions-next.md first (latest: write-partition provider from
dynamic config), then matching-limiter-next.md and matching-rate-mixed.md. The wired dispatch
limiter passed all 30 full-runner stages (incl. Go Frontend/History/Worker routed
to Rust Matching). A mixed activity-pacing scenario was added to
tests/matching-service-compat.cjs (`taskListMetadata.maxTasksPerSecond = 1`,
assert last backlog poll >= 500ms, delivered set, Describe ratePerSecond === 1);
its first run failed only on a probe ordering assumption (dispatch is not
identity-FIFO), the fix is committed, and a re-run must be confirmed before the
rate gate is claimed. The native queue-owner dispatch limiter is now
wired: shared `TaskListLimiter` per owner, reader pre-handoff `Wait` under the Go
`newDispatchContext` deadline, activity rate reports and `DescribeTaskList`
`rate_per_second`. 5 new limiter tests + 70 cadence-server lib tests + strict
all-target Clippy pass (/tmp/cadence-lim-lib.exit=0, /tmp/cadence-lim-clippy.exit=0).
Full 29-stage runner and a mixed Go-services/Rust-Matching activity-pacing scenario
are still required before the rate gate is claimed.

Follow root AGENTS.md. The user requires continuing the entire Go-to-Rust rewrite,
service by service: Matching -> History -> Frontend -> Worker. All roles remain
incomplete. Keep working autonomously; use tmux session cadence-rust for long
processes. After compaction save progress, commit and push. Never equate adapter
coverage or a boundary probe with complete service parity.

## Latest lease retry checkpoint

Read matching-retry-next.md first. Initial/rollover lease operations now use Go's
persistence retry policy and previous-error return convention. Every terminal
renewal failure stops writer admission; CreateTasks remains single-attempt.
Retry timestamps advance after the exact initial caller timestamp. Ten actual-Go
retry outcomes and real SQLite transient/terminal tests pass.
Final full regression PASSED: 348 workspace tests, strict all-target Clippy and
all 27 compatibility stages, including mixed Go services/Rust Matching SDK cases:
/tmp/cadence-retry-full.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-retry-verified/results.json.
Next: matcher rendezvous + reader buffering/stop coordination, reader/rewrite
transient retries, injectable clocks, dynamic config; then query/sync/forwarding,
isolation/rates, production routing/discovery/transports and remaining role gates.
All four service roles remain incomplete. Preserve the whole rewrite objective.

## Latest startup concurrency checkpoint

Read matching-startup-next.md first. Per-key startup gates keep lease I/O and
stopped-owner cleanup outside the registry map lock. Shutdown prevents publication
of a delayed startup. Real SQLite contention, all 15 registry tests, strict lint,
and mixed Go services/Rust Matching with seven SDK scenarios PASSED:
/tmp/cadence-startup-check.exit, /tmp/cadence-startup-lint.exit,
/tmp/cadence-startup-mixed.exit = 0. Artifacts:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-startup-mixed/results.json.
Next: lease renewal retry policy, reader buffering, dynamic config, sync/query/
forwarding and remaining Matching service gates. All four roles incomplete.

## Latest writer checkpoint

Read matching-writer-next.md first. Sequential bounded writer now owns native
AddTask and failed-delivery rewrite persistence, Go batch semantics, per-request
source domains/timestamps, timeout errors and cancellation-independent completion.
PASSED 345 workspace tests, strict all-target lint and all 26 compatibility stages:
/tmp/cadence-writer-full.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-writer-verified/results.json.
Actual mixed Go services/Rust Matching and seven SDK scenarios pass.
Next: per-owner startup/map-lock concurrency, reader buffering, dynamic config,
lease retry policy, then sync/query/forwarding and remaining Matching gates.
All four service roles remain incomplete. Preserve the entire rewrite goal.

## Teardown/idle liveness continuation

Read matching-idle-next.md first. Prior cbd02ab7c is pushed. New atomic queue stop,
out-of-registry-lock teardown, replacement-safe cleanup, idle liveness, half-TTL
checks, new-owner/fatal-stop controller wakeups are implemented. Liveness is only
refreshed by AddTask/new GetTask waits, not describe/empty storage retry loops.
Closed pumps now return empty successful polls, matching actual Go engine code.
The mixed probe adds blocked-idle-poll and pending-task unload/reload checks.

Final frozen-source validation PASSED: 342 workspace tests, strict workspace
all-target clippy and all 25 compatibility stages. /tmp/cadence-idle-verified.exit
= 0; /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-idle-verified/results.json.
Mixed probe confirms empty idle poll through Go Frontend, durable queued-task
recovery after unload and all seven existing SDK scenarios without Go Matching.
This checkpoint is being committed/pushed with these notes. Earlier
matching-idle-final was intentionally interrupted for the closed-pump correction.

Next: background task reader buffering/writer batching and per-owner startup/
controller concurrency, dynamic per-list configuration, sync matcher/query
broker/forwarding, remaining controllers/transports and production discovery.
Go Manager.Stop does not wait for all History RPCs (see matching-idle-next.md);
do not add stricter behavior. Transport drain/concurrent shutdown still need
validation. No role gate is complete; retain the entire rewrite objective.

## Latest GC/final acknowledgement checkpoint

Read matching-gc-next.md first. Prior checkpoint 0d0952ff8 is pushed. Native queue
now owns Go-style thresholded task GC: try-lock, cursor, batch/time thresholds,
full-batch drain, error cooldown, forced teardown cleanup. Dispatcher uses the
Go defaults (100 rows, one second), replacing unconditional 1000-row deletes.
Final reader acknowledgement can persist after writer stop, with SQL lease fencing;
stopped-owner replacement/unload/shutdown drain completed rows before release.

PASSED fresh frozen-source full validation: 333 workspace tests, strict workspace
all-target clippy, all 24 compatibility stages. /tmp/cadence-gc-verified.exit = 0;
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-gc-verified/results.json.
Fourteen actual-Go GC fixtures plus deterministic time/error tests pass; actual
mixed Go services + Rust Matching and seven existing SDK scenarios pass.
This checkpoint is being committed/pushed with these notes.

Next: strict writer-stop/unregister ordering vs concurrent RPCs, in-flight
shutdown coordination, immediate fatal-owner watchers, idle unload, dynamic
per-task-list configuration; then queries, sync matching/forwarding and remaining
controller/transport work. Registry still holds its map lock during teardown,
while Go unregisters first. No service gate is complete; preserve the whole goal.

## Latest queue maintenance checkpoint

Read matching-maintenance-next.md first. Prior checkpoint 0e7dbae0e is pushed.
New native registry controller persists due acknowledgements with per-owner
load/update-based deadlines, resets after operations, keeps healthy queues alive
on another owner's fencing, removes stopped owners, replaces stopped writers
on the next get, and wakes dispatchers on unload/shutdown. The live fixture owns
a maintenance pump using Go's 60-second default and awaits its final shutdown.

PASSED: /tmp/cadence-maintenance-final.exit = 0 (20 focused Matching tests plus
strict workspace all-target lint); /tmp/cadence-maintenance-mixed.exit = 0 (actual
mixed Go services + Rust Matching, status probe and seven existing SDK scenarios).
Artifacts: /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-maintenance-mixed.
This incremental checkpoint is being committed/pushed with the notes. Full prior
workspace/23-stage validation is documented below, not rerun for this increment.

Next implement Go task_gc.go cursor/threshold/try-lock/drain behavior and reader
shutdown GC ordering; then idle unload, in-flight shutdown coordination, dynamic
per-task-list configuration, queries and sync delivery/forwarding. Preserve the
whole four-role rewrite goal. No role is complete and no production native
Matching selection/membership is available yet.

## Poller state continuation

Read matching-pollers-next.md first. Prior RPC/mixed SDK checkpoint ad9ebf44a is
pushed. New common poller manager matches ten original-Go reference snapshots.
Native registry/poll/DescribeTaskList integration and queue status are added.
A source review corrected EndPoll timing: dispatcher wait cycles end BEFORE
History start, and restart after failed deliveries. New focused test asserts it.

Final verification PASSED:
- /tmp/cadence-poller-workspace-final.exit = 0: all 327 workspace tests.
- /tmp/cadence-poller-timing-tests.exit = 0: 16 focused Matching tests and strict
  all-target workspace clippy, including corrected pre-History EndPoll timing.
- /tmp/cadence-matching-poller-full.exit = 0: all 23 compatibility stages;
  /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-poller-final-20261016/results.json.
  Native live stages compile the corrected dispatcher and check DescribeTaskList
  through Go Frontend, plus all seven expanded SDK scenarios with no Go Matching.
Poller state checkpoint is being committed/pushed with these notes.

Next: configured periodic ack maintenance, fenced owner unload, query delivery
and sync matching/forwarding. Rates/isolation/QPS/config/membership/semaphores
and internal Thrift transport remain partial. All four roles are incomplete.

## Latest verified Matching RPC checkpoint

Read matching-rpc-next.md first. Opt-in Matching::with_durable_queues connects
AddTask/poll protobuf handlers to the durable registry and Go History RPC client,
without local workflow execution lookups. The production default is unchanged.
Actual mixed Go Frontend/History/Worker + Rust Matching probe PASSED with no
Go Matching process, using test-only Fx discovery and seeded Rust domain metadata.
Existing Go SDK over Go Frontend TChannel also passed. Full accumulated runner:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-final-20261016/results.json
and /tmp/cadence-matching-mixed-full.exit = 0: all 22 stages, 324 workspace tests,
strict lint. Expanded SDK (echo, activity retry, child, workflow-run/task retry,
activity timeout and signal) separately passed:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-sdk-expanded2/results.json.

This checkpoint is being committed/pushed with these notes. Matching remains
incomplete. Next: native poller history/DescribeTaskList, periodic ack maintenance,
owned queue lifecycle; then query delivery, sync matching/forwarding, rate limits,
partition/isolation/semaphores, production domain cache/config/membership and
internal Matching Thrift/TChannel. Do not move to History prematurely.

Build the Go routing harness with --rust-matching, or reuse:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-mixed-20261016/cadence-server.
Combined runner uses --go-mixed-server-bin for that test binary; --go-server-bin
is only for ordinary original Go CLI (/tmp/cadence-go-mixed/cadence-server).
Serialize heavy linking; Rust integration tests are now precompiled by the
baseline runner before services start. First two failed attempts were build
contention and an invalid discovery seed; see matching-rpc-next.md.

## Verified checkpoints

- 90e14cfac: Go-compatible raw Thrift/Snappy Matching blobs and SQLite schemas,
  leases, fenced atomic writes, recovery and Go/Rust shared-database interop.
  Full 19-stage runner passed with 303 workspace tests.
- eedeb54d9: durable queue ID allocation/read/ack/expiry/rewrite behavior,
  original-Go reader fixtures, History client and cancellation-safe dispatcher.
  Full 21-stage runner passed, including an 11-event workflow against actual Go
  History and completion through Go Frontend. Evidence:
  /home/ubuntu/.cache/cadence-rust-test-artifacts/matching-dispatch-verified/results.json.
- bddc9003c: independent queue registry, concurrent single lease acquisition,
  ack flush/shutdown and identity-checked unload. Eleven focused Matching tests
  and strict workspace all-target clippy passed:
  /tmp/cadence-registry-unload.exit = 0.

## Latest source-domain continuation

Go CreateTasks uses each task's source DomainID for its SQL row, while fencing
TaskListInfo's target domain. Rust create_tasks_with_domains and queue
append_in_domain now preserve that distinction. Extended actual-Go SQLite
interop verifies both directions and rejected-lease rollback:
/tmp/cadence-source-domain-interop.exit = 0.
Seventeen focused persistence tests and strict workspace lint passed:
/tmp/cadence-source-domain-final.exit = 0.

Full accumulated runner PASSED all 21 stages and 321 workspace tests:
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-registry-source-domain/results.json;
/tmp/cadence-registry-source-domain-full.exit = 0. Includes strict lint, original-Go
differentials, standalone SDK checks and actual-Go History delivery. Used unchanged
original Go binary /tmp/cadence-go-mixed/cadence-server. Source-domain checkpoint
is being committed/pushed with these notes.

## Known validation/resource details

Use disk-backed artifact dirs under /home/ubuntu/.cache/cadence-rust-test-artifacts;
/tmp is tmpfs and retained server binaries consume RAM. Serialize heavy links.
The dynamic-policy differential oracle outside the combined runner has a known
fixture enum mismatch and must not be represented as passing. Preserve the Go
one-minute failover cooldown; combined-runner rapid-failover checks use an explicit
test-only admin override. No PR exists for this work.

## Native Matching Thrift checkpoint (2026-10-17)
Uncommitted transport work checkpointed after context handoff: all ten Matching Thrift methods, shared TChannel listener and HTTP routing, generated method inventory, original Go client probe. Two codec unit tests pass. Original Go TChannel probe passes; HTTP probe currently routes incorrectly (EntityNotExists instead of expected BadRequest). Rebuild and diagnose HTTP path before claiming compatibility. Whole Matching remains incomplete: distributed forwarding/ownership and production integration still require implementation and tests.

## Parent-handler checkpoint (2026-10-17, latest)
Same-host parent polls now use the full Matching handler, including root queries,
per-hop deadline tailroom and filtered expiration. All 156 server tests and strict
workspace Clippy pass. Frozen-source live chain in tmux cadence-rust window
matching-parent-handler-live: native Go Thrift probe passed; boundary/mixed probes
still running at checkpoint. Inspect /tmp/matching-parent-handler-live.exit and
log for final outcome. Full implementation details in matching-membership-next.md.
Whole Matching remains incomplete; production discovery/runtime wiring and other
listed parity gaps remain. Next focused operational config gap: Go's
matching.overrideTaskListRPS, read on each poll in task_list_manager.go:715.

## CI status — 2026-10-29 (terminal update)

Run [40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628)
completed `success` at exact expected head
`d4ee53f00d505246dc0b7fbbe353d7e989a91c49`. Selected `cadence-matching`
Clippy/test exits are 0/0; the library suite reports 973 passed and 2 ignored.
The advisory workspace-format exit is 1 due to an unrelated diff in
`rust/crates/cadence-api/src/admin.rs:371`; scoped Matching runtime/query/TLS
formatting checks passed. The run restored a Rust cache, so it is not a fresh
uncached workspace gate. Compact evidence is in
`memory/evidence/matching-startup-candidate-ci-40840392628/`. This candidate's
log contains initial cold-domain/runtime startup coverage, but no later
semaphore startup-timeout, expanded adapter/lock, or two-thread regressions.
Do not attribute candidates `25e`, `90cd`, `e84e`, or `8cc` to this run.
Matching remains incomplete; preserve frozen `bf9f3d3`, its failed PostgreSQL
attempt before SDK execution, and all ten required mixed lanes. Terra integration
and review, then fresh-target workspace CI, must precede serial mixed gates.
See `matching-native-checkpoint-20261029.md` and
`matching-native-resume-state-20261029.md`.
