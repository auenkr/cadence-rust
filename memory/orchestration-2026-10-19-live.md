# Live Matching orchestration recovery

Parent starts at f6e6363d7. Matching remains incomplete; no later service work
is authorized by the service parity gate. User requested ongoing orchestration
with normal-speed Luna workers. User's uncommitted AGENTS.md is preserved.

Native gpt-6-luna, medium reasoning workers (no nested delegation):

- `/root/swim`: retained t3-6f14baa6 worktree, SWIM runtime and actual Go probe.
  Sole heavy build slot; jobs=1, tmux cadence-rust. Must observe ALIVE while
  Rust peer is running, rather than weaken the assertion after process exit.
  Adaptive gossip cadence and waiting for in-flight shutdown remain review gates.
- `/root/runtime_cache`: matching-dns-provider worktree, recover production
  defaultStore/domain-cache wiring. Source only pending build grant. Existing
  draft preserved; parent has requested actual shared Go-store evidence and
  eventual actual production-binary mixed harness rather than adapter evidence.
- `/root/history_routing`: t3-83afa06b worktree, reconcile retained routing
  source with recovered r7 commit 98e559da4 on t3/matching-history-client-parity.
  Source only pending grant. The r7 worktree itself was removed, but commit is
  available. Parent found additional retry review issues: previous error must
  be captured before the operation, including cancellation during backoff;
  factory timeout must not be mistaken for ThrottleRetry WithOperationTimeout.
  Worker was instructed to prove/fix against actual Go behavior.

Root git status initially failed because .git/modules/idls/config core.worktree
pointed at deleted t3-ef455bb6. Parent repaired local metadata to root go/idls;
submodule is clean at pinned bfbf273b41b9e36ce21e47a49600d22d73e4614c.
No tracked submodule source changed. Disk had 60 GB free at recovery.

No new component accepted, no current-source combined validation yet. Prior
service/production gaps and frozen-evidence limitations still apply.

## Build-slot transfer

SWIM cold baseline build and live Go interop passed; the harness now observes
ALIVE before the Rust peer exits. Worker is implementing exact uniform-reservoir
adaptive cadence and awaitable in-flight shutdown, source only. No acceptance
of the uncorrected runtime. Its warmed target is kept local and unshared.

Parent and worker verified no active builds, then granted runtime_cache the
sole heavy slot for persistence/cache tests, actual binary check, shared Go
store production probe and strict relevant Clippy. Only SWIM had a target on
disk; cache may create one, jobs=1, tmux cadence-rust. Disk about 57 GB free.

History routing source candidate fad59909a is committed, unvalidated. Parent
requested caller-deadline propagation through outer Record retry/backoff and
nonpositive configured timeout behavior review before acceptance. Factory
30-second timeout now has a separate explicit override builder.

## Source review and validation continuation

History source chain in t3-83afa06b: fad59909a +25dbbcbf3 (caller deadlines,
zero Record timeout), then438007ce0 +08265d333 (live Ringpop History adapter,
snapshot-consistent gRPC endpoint). Source formatting/diff only, no compilation
or acceptance. Native worker finished and is available for reuse. Adapter's
shared Host snapshot has no TChannel field; named TChannel resolution remains
unsupported rather than reading a second inconsistent snapshot. Root and
cache draft do not yet instantiate/bootstrap Ringpop in main; shared listener
API capability exists but is not yet used by main.

Cache validation initially failed compilation, then six persistence tests.
Worker repaired raw nested Thrift decoding and fixture struct shape, preserving
failure logs. Parent rejected a production 5ms early timeout-classification
tolerance. Worker removed it; final persistence rerun is10passed/1ignored.
The Go-file-dependent ignored test still needs the actual Go fixture. Server
cache compilation now active, sole heavy slot, private target at
/home/ubuntu/.cache/cadence-rust-test-artifacts/matching-production-domain-cache/target.
Long commands run in tmux cadence-rust; no component accepted yet.

SWIM source adds adaptive schedule/reservoir and cooperative async shutdown,
including shared completion for concurrent stop and start rejection during
shutdown. Actual pinned-Go blocked-ping Stop oracle added; all final builds
and new oracle execution pending regrant. Parent caught the initial concurrent
shutdown race before acceptance. Worker remains source-ready, idle build lane.

## Further review checkpoint

Root integrated the reviewed persistence audit as7e71de29c+3d9db3e75,
distinguishing logical task-list shard IDs from physical database routing.
No implementation accepted in this recovery round yet.

Cache earlier gates reached10persistence/16cache tests, bin check/build,
strictClippy and production native-Go probe all passing. These predate latest
source corrections and are NOT final-source acceptance. Parent caught the
raw SQL store vs DomainManager boundary: raw optional blobs may be opaque,
but Go DomainManager validates bad-binaries/isolation/async/replication blobs
before cache publication. Latest source preserves raw data/deferred error and
rejects invalid typed conversion atomically in the cache. Raw-store oracle and
valid production cache probe now use separate DBs. Parent also required
no-config development mode retain its old in-memory domain behavior and
--data-dir durability; explicit unsupported configured stores still error.

SWIM reached58Rust tests and live Go/Rust interoperability including actual
blocked-ping Go Stop proof. Parent required observed Sample().Size and
unclamped50ns p50 oracle (not hardcoded/floor-only evidence). Latest source
changes further fix stop-before-start, cancellation-safe owned cleanup and
transition timer ordering during in-flight shutdown; fresh tests/lint pending.
Source-ready worker is to remain idle until regrant.

Sole heavy slot now granted back to runtime_cache for its corrected final
gates. Next SWIM, then History routing. Both use warmed targets, jobs1, tmux
cadence-rust. Latest disk44GBfree. Original user AGENTS edit remains untouched.

## SWIM component accepted

Integrated worker c5a87b35 as c31ab6641. Final evidence: 8 common memberlist
tests, 59 server Ringpop tests, strict common/server all-target Clippy, live
Go/Rust peer interop and actual Go blocked-probe shutdown all pass. Current
cadence fixture is reproduced in compat-final (sample size10, p50 rate50ns,
floor1ns); go-oracle-final is an older retained trace despite its filename.
Corrected stale source-only/untested paragraphs in the component memory.
Production main still needs concrete Ringpop lifecycle wiring. Cache retains
sole heavy slot, final attempt20261019T200114Z-1846622. History worker is
preparing source-only validation/integration review while waiting.

## Production cache accepted and build lane transferred

Accepted worker e43410611 as e1b78e5f1; integrated source equals tested source
for cache/persistence/main/dispatch files. Pushed to origin. Final timestamped
attempt20261019T200502Z-1851423 passes all nine gates, including actual binary
Go native Thrift/WAL failover and separate raw malformed-blob oracle interop.
Earlier attempt had only needless_update Clippy failure, repaired before final.
This remains component evidence, not full mixed service/SDK completion.

History worker now owns sole heavy slot, rebasing its routing commits plus
test-attribute repair/runner/new RPC-boundary tests onto e1b78e5f1 before
validation. Can reuse cache warmed target after release. SWIM worker is
source-only implementing concrete provider, prebound listener, local label/status
and self-eviction primitives; production main recipe separate. Cache worker
source-only plans configured SQLite task persistence next; no extra builds.

## Routing and provider review continuation

History rebased source chain onto e1b78e5f1: 7e2214bff,9cbd46f03,e116c1f4a,
c4777e095,9652ecff3,c55c60a8d,6ad604825 plus compilation fixes. Shared RPC
boundary test does not establish dispatcher/query end-to-end deadline proof.
First builds found inference/missing farmhash dependency, then test fixture
shadowing/Status comparison issues. Worker fixes in own worktree; sole heavy
slot remains History. Final acceptance requires passing locked frozen-source
gates, strict lint and Go oracle. No routing implementation integrated yet.

Provider source-only candidate e6fc0fd79 in t3-6f14baa6 adds prebound shared
listener/provider/local mutation/self-eviction/watch changes. Parent rejected
250ms polling; replaced by membership watch. Further review caught a stale
service==self guard that drops History members and local-index/IP-selection
assumptions. Worker is correcting and preparing actual node interoperability
tests; no build grant. Provider edits were accidentally made in shared root;
worker saved patch/hashes, rebased isolated worktree onto783e020f1, copied and
verified exact hashes, restored ONLY owned root paths. Root remains only
user-modified AGENTS. No build ran on dirty root provider source.

Task-store worker moved clean worktree onto91431dd31 and is source-only
implementing defaultStore-configured single-shard SQLite queue factory. Parent
required exact database path preservation, writer journal/busy options rather
than reader exceptions, existing schema validation and actual Go read/write
proof. Explicit unsupported backend/multishard errors are partial migration,
not all-backend parity. No new target directories needed;42GBfree.

## Shared hash defect and next frozen candidates

Actual Go placement fixture failed for invalid shard1114112 (Go rune U+FFFD).
Parent found farmhash1.1.5 short-byte unsigned handling differs Go signed int8.
History74c540e6d fixes common fingerprint32 (<=4 signed bytes), Ring and
Ringpop checksum/labels and History workflow/domain placement. Parent caught
initial rotate-left vs Go rotate-right error before build. Actual generated Go
vectors and original Ring oracle (extended U+FFFD) pass in final expanded
attempt20261019T203141Z-JSv0Tg, as do ringpop_member tests and common Clippy.
History server tests/lint/oracle still pending rebuild. Worker sole build owner.

Provider source chain e6fc0fd79+954b88540+7c90e1d6f+32fc49bd6+cfd5e9dce
is clean and unvalidated, includes Rust-Rust shared-listener bootstrap/evict/
restart and actual Go provider overlay harness with higher recovery incarnation.
No main integration. Source-only task-store chain7fb11795f+a4ad328ac+
520ec008d+b2989bec1 clean, unvalidated. Fixed initial Go taskID0 skipped by
ack0: probe now distinct Go ID2 after Rust ID1 and asserts delivery plus bounded
GC. Staged real-discovery mixed binary harness avoids Fx Matching decorator,
but waits production main/provider and discovered History injection; output
explicitly preliminary/acceptanceEligible:false. Both workers idle awaiting
grants, not running background builds. Current free disk37GB.

## History routing accepted

Merged validated worker f8646963d into root219f86054. All13 final exit files
in attempt20261019T204205Z-ZuRMtP are0, before/after source hashes identical;
root Rust source byte-identical to worker final source. Includes Go byte hash
fix, Go Ring oracle, Ringpop member regressions, History client14/resolver5/
membership4, dispatcher regression, strict common/server Clippy, pinned Go
History placement oracle equivalence. No production resolver injection or
TChannel adapter/E2E poll-query caller deadline proof claimed.

SWIM now sole heavy slot, rebase providerd0cc154f5 chain onto219f86054 then
validate combined source plus actual Go peer. Cache source-only rebase its
configured store/harness chain onto219f86054 for next lane. History source-only
next audits pinned Matching shard-distributor APIs/slices before implementation.
No competing build processes at transfer. Root only dirty user AGENTS.

## Provider validation continuation and SD draft

SWIM rebase chain now includes sourcee86a9f9c3 (unit match arms),925a3e436
(invalid-port fixture),6984564a0 (Go malformed-address valid-port fallback).
Active frozen run cadence-rust:matching-swim-final4 pane%14; worker sole slot.
Earlier attempts204800/205216/205609 retained failures; cancellation lifecycle
tests passed, mapping boundary revealed real None-vs-:port discrepancy. Source
still not accepted. Memberlist/common hash gates pass.

Configured-store/harness clean rebased candidate647f1672d onto55dec7f3d
awaits next slot; runner locked and honors shared target. History source-only
SDeecb1a1c9 adds config+standalone client owner but NOT registry/main wiring
or accepted behavior. Parent review flagged start-after-stop/stop-before-start,
concurrent/cancelled shutdown and first-snapshot waiter termination; worker
correcting plus fake spectator server coverage, no build grant. No service
completion claim. Root remains only user AGENTS dirty.

## Concrete Ringpop provider accepted

Merged validated fcad9c78e into rootb07398ac2. Root Rust source equals tested
worker. Core hashes210514 and211342 equal; focused common/lifecycle11/
provider4/Ringpop41 tests and strict common/server all-target Clippy pass.
Final211342 detailed pinned Go log verifies first labels+ports, self-eviction,
same-address restart with higher incarnation. Fixed harness race by explicit
stdin phase acknowledgements; old timing-sleep failure retained. No main
assembly/full mixed-service SDK claim. Root user AGENTS remains untouched.

Build lane transferred to cache worker: rebase647f configured-store/harness
chain onto b07398ac2, validate frozen combined source incl two-way Go queued
tasks/prod native probe/options/schema/lint. SWIM source-only next audits
remaining Ringpop healer/partition/bootstrap parity before new implementation.
History source-only SD chain eecb1a1c9+2e713ebed remains unvalidated config/
standalone owner only; no registry/main ownership wiring. Current free36GB.

## Resumed checkpoint after compaction

Accepted root b1a7f52e6 remains pushed; user AGENTS.md edits preserved.
Configured-store worker remains sole build owner. Rust gates passed frozen
961913605; actual two-way Go probe has not passed. Fixture fixes through
17b3b6adb include SQL plugin/driver registration and embedded TaskInfo.TaskID2.
Current tmux matching-two-way-taskid run retains original tested Rust binary.
Parent flagged cached max_read_level: inserting Go ID2 after Rust writer ID1
may exceed the live queue read bound. Worker must distinguish invalid concurrent
same-lease fixture from product defect; prefer Go seed before queue open/restart
if required, without changing Go semantics. Earlier failures retained.

Unmerged healer chain65d46a130..9e5591fef and SD clienteecb1a1c9+2e713ebed
remain source-only/unvalidated. Idle workers resumed for bounded source audits:
SWIM remaining provider parity/validation runner; History production main wiring
contract. Neither may build while configured-store worker owns lane. Sole warmed
target /home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target retained;
obsolete17GB cache target removed after process check; about48GB free.
Matching remains incomplete; production provider/history assembly and actual
mixed Go/Rust SDK/recovery acceptance still outstanding.

## Healer accepted; configured-store capture pending

Accepted healer chain d782a11ed..4721fb465 cherry-picked to rootc9c1f4f5b,
pushed. Root provider/node sources equal tested worker source. Corrected run
memory/evidence/ringpop-healer-validation-20261019T215532Z has all11 command
exits0, actual Go fixture comparison, Rust live partition/reverse-sync,
blocked real JOIN response proves stop waits, provider/discovery/memberlist
regressions and strict common/server Clippy. Initial missing-helper compile
failure215112Z retained. This remains component evidence, no full service claim.

Configured-store candidate through0d3406fd4 remains unmerged. Frozen Rust
961913605 passed8 SQLite/4 factory tests/build/strict lint; final native Go
probe220052Z passed legitimate Go lease1/task2 -> Rust lease2/task100001
transition, Go readback of Rust task/blob, delivery+GC, native Thrift transports,
TLS/drain/failover/private production database assertion. Earlier live insertion
beyond writer read bound was invalid fixture in both Go and Rust; embedded
startup directory reuse caused a false private-DB assertion. Both corrected.
Final probe output was consumed interactively: parent requires captured rerun
before accepting evidence. Worker owns sole lane for raw stdout/stderr+exit
rerun, then source-only production main assembly on separate commits. Candidate
small evidence package distinguishes historical failed results from passing
Rust gates. Exact defaultStore lookup differs: Rust trims name in task/cache
factory, Go keys exact name. Recorded unresolved config parity gap.

History worker now implements outbound Matching History TChannel path,
source-only branch t3/matching-history-tchannel in t3-83afa06b. Committed typed
container codec638004c68, transport7af33eea6+43f4441e6; all5 operation adapters/
error mapping/integration remain incomplete. Parent narrowed next checkpoint
GetMutableState+VersionHistories field-preserving converters. Temporary nested
schema child finished/released so runtime owner could resume; no new spawning.

SWIM next source-only rebase/review unvalidated SD clienteecb1a1c9+2e713ebed
onto rootc9c1f4f5b; no build until cache releases. Runtime owns main assembly;
SD registry callbacks/config integration remain later. Sole warmed target stays
t3-6f14baa6/rust/target jobs1,47GB free. User AGENTS edits untouched.

## Recovered worker state and configured-store acceptance (2026-10-20)

Worker messages/handoff were stale: actual cache branch had already captured
passing probe62d9abf21/01a2a3f46/0cdd87e2c and main sourcebbd411812+b850bdc7f.
Parent inspected raw attempt2 output, metadata exit0, before/after hashes and
reviewed drain window change5s->30s (RPC must still complete during drain).
Accepted factory chain through0cdd87e2c into root160fcc6f3, pushed; relevant
main/factory/config/lib/sqlite/probe files byte-equal tested source. Go task
readback and lease transition verified. Root now contains both independently
validated healer and configured factory; combined main validation remains next.

Main candidate is unbuilt. Parent review found taking runtime Option in tuple
if-let drops embedded runtime when membership absent; worker instructed fix
before gates. Also verify Go dynamic bool precedence/default and advertising
gRPC labels before binding gRPC; add rollback/occupied-port coverage. Worker
rebasing only main2 commits onto accepted root, source-only while SD has lane.

SD worker rebased client+review onto160fcc6f3, tipf83c47fab, sole heavy lane
started tmux matching-shard-distributor-review at051406UTC. Frozen hashes/raw
exits initially /tmp/matching-shard-distributor-evidence/attempt-20261020T051406Z.
Must package evidence before merge. Resolver Go oracle is not executor parity.
History transport worker actual recovered HEAD58c5b9f5a includes GetMutableState
and task-start mappings; all5 callable adapters now source-only, production
retry/redirect/transport selection remains under implementation. No nested
workers active, no future service migration. Cache pending_init was interrupted
and resumed; no source lost. Avoid duplicating captured probe work.

## Production main validation and real restart metadata failure

Standalone SD client accepted root5c15c39e6 after7 Rust SD/12 config tests,
strict Clippy, existing pinned Go resolver suite; executor/spectator live Go
interop and registry/main wiring remain unproven. Evidence packaged with scope.

Main worker source1a8591566 fixes parent-found embedded runtime take/drop bug,
prebinds grpc, handles bootstrap signals, adds embedded durable native probe
and occupiedgrpc check. Build/no-run/runtime/lifecycle/provider pass; initial
Clippy last() lint fixed equivalently in a7f2603a2; corrected strict lint passes.
Focused embedded durability probe passes, production focused probe fails empty
polls because isolated harness only advertises Matching membership, no History.
Do not add production loopback fallback; fixture needs History Ringpop peer.

Actual mixed Go Frontend/History/Worker (no Go Matching) reached Rust through
real discovery. First attempt missing node dependencies fixed with explicit
NODE_PATH to existing root locked cadence-web node_modules. Next attempt got
through real production startup/task writes and SIGKILL, then Rust restart
failed valid Go metadata decode unknown ttype89. Root found Go codec version0
preamble0x59=89 (common/codec/interface.go/version0_thriftrw.go); nested domain
blob decoder currently reads raw struct. Worker owns sole lane diagnosing exact
nested-vs-outer serialization boundary, actual Go bytes, version validation and
regression fixtures; no dropping invalid cache rows/fallback allowed. Root main
still unmerged until recovered mixed evidence. Raw failures retained in cache
memory/evidence/matching-main-gate-20261020-a7f2603a2.

User confirmed root uncommitted matching_registry.rs and untracked
matching_shard_processor.rs are THEIR edits. Preserve alongsideAGENTS; never
restore/overwrite/blind cherry-pick. SWIM isolated candidate96b297447+f4a3e54b5
is source-only callback implementation/tests; now comparing read-only user
snapshot and preparing additive tests preserving their behavior. QPS gap
hypothesis disproved: existing grpc_matching report_arrival records nonforwarded
loads. No worker modifies root. History source candidate685cca4c8 includes5
transport adapters/integration, coherent TChannel named endpoints, Args/Result
wrappers, prepared Go fixture generator/consumer and native fake-peer tests.
Actual Go fixture and builds pending main lane; no parity claim.

## Continuation checkpoint, 2026-10-20

Root94ff64a6d retains exactly the user's AGENTS.md, matching_registry.rs and
untracked matching_shard_processor.rs edits; explicit user instruction is to
preserve them. No root implementation edits authorized to workers.

Main candidate eb06a3e99 fixes nested Go domain codec prefix after optional
Snappy; actual pinned Go serializer fixtures and12 metadata tests pass. Mixed
existing SDK workflows completed after SIGKILL/restart and graceful restart.
Full mixed gate still fails isolated poller registration: production needs
Go-shaped dynamic configuration, not a test admin endpoint. Runtime worker
source-only implementation now includes modern/legacy file selection, ordered
values/constraints, startup load and owned reload lifecycle. Path/poll semantics
and tests remain under review; no build lane yet. Focused configured native
harness still needs real discoverable History peer. Main remains unmerged.

History worker owns sole heavy lane. Pinned Go1.25.14 oracle exit0,25 entries,
observed/expected SHA256 de22e106 prefix equal. First Rust no-run failed15
candidate compatibility errors; fixing wrappers, inference, imports and local
metadata decode. Subsequent gates must include real generated Go YARPC History
peer for all five operations, including caller transport headers; fake peer
and wire fixtures alone cannot establish interoperability.

SD callback tests align to user snapshot in isolated commit50ce04f1b; not yet
built or merged. SD routing source in progress in separate worker, no main
edits: async spectator plus existing Ring snapshot fallback, dynamic policy
closure, named endpoint results. Source-only while History builds. Shared
CARGO_BUILD_JOBS=1 target remains t3-6f14baa6/rust/target; all shell work must
set explicit workdir because resumed workers can inherit root cwd.

### Review continuation

History candidate4d1b8bc6b passed no-run attempt2,25-fixture consumer
attempt3, and attempt4 focused fake-peer all5, TChannel redirect/deadline,
15 History regression tests and4 TChannel adversarial tests. Real pinned
YARPC source review found candidate uses History::Method instead of generated
HistoryService::Method and lacks cn caller transport header, placing reserved
caller/service in app headers. Real generated Go peer validation/fix is next;
do not merge from fake-peer results alone. History retains build lane.

Runtime source checkpoint2de12942c adds filebased dynamic config and exact
defaultStore lookup. Unbuilt. Parent review requests initial-load completion
time passed into watcher (avoid losing bootstrap-time updates), real Go null
root/property and malformed constraints oracle, and typed diagnostic parity.
Watcher lifecycle review fixed mutable receiver, Drop abort and cleanup error
aggregation. Main/mixed harness still pending.

SD router3ece21e23 plus18c9edce9 corrects partial move and Go unsigned port
parsing (plus signs rejected). c67b43457 adds isolated advisory cached-owner
probe. None built/merged. Full integration audit found registry get checks
Ring both before reuse and publication, maintenance evicts by Ring, and remote
forward/query/partition routes use Ring directly. Must integrate shared async
routing and local executor assignment policy coherently across these surfaces;
preflight alone is insufficient. Runtime and swim coordinating async assignment
refresh plus sync processor snapshot validation for publication and unload.
Root user source hashes remain d5d9f593 (registry),d7ae805e (processor); isolated
worker baselines include test seams and differ, so extract reviewed additive
diffs, never blindly import user snapshot commits. Disk39GB free.

### Native peer and SD integration continuation

History actual generated Go YARPC peer passed all5 nonempty request/response
checks, caller/service/procedure/encoding metadata, typed errors/redirect in
attempt6 after fixing empty redirect test fixture. Corrected procedure
HistoryService::Method, cn transport caller cadence-matching, app headers empty.
Current source uncommitted; unused helper removed, exact all-target strict
Clippy -D warnings and final peer rerun pending. Parent rejected blanket
-A dead_code lint as strict evidence. Keep attempt failure logs/provenance.

Runtime checkpoints348831585 (dynamic loader/harness fixes) andd801180a4
(unrun Go filebased oracle), routing cherry-picks through4ae464b80. Initial
load completion timestamp now passed to watcher, null/empty semantics and
constraints conversion reviewed. Diagnostics sample first/every1000 with
known generic/process-global limitation vs Go Collection context. Main SD
executor/factory/client construction still being implemented source-only.

Swim coherent routing/ownership641188049 includes async handler preflight,
sync local processor rechecks before cached reuse/publication, SD-aware
eviction and remote forwarding/query/parent poll/partition routing. Parent
reviewed lock order: executor releases processor mutex before Stop callbacks,
no reverse held registry lock. Rejected post-open owners use existing finish
cleanup. Unbuilt. ff6e10f8b adds actual Go handler/generated gRPC fixture plus
ignored executor/spectator callback integration gate, unbuilt/unrun.

Refreshed backend audit confirms configured task+domain stores remain SQLite
only; MySQL/Postgres/Cassandra and durable semaphore state/telemetry remain
Matching gaps. Audit based on swim branch must account for newer runtime
candidate discovery already implemented elsewhere. No History service
migration started. Root remains exactly user AGENTS/registry/processor edits.
Runtime resumed in root read-only once; corrected before edits. Explicit
absolute paths/workdir remain mandatory.

### Accepted History transport and next frozen runtime

Accepted/pushed full History transport source+evidence to rootfdb3b971a.
All62 changed files byte-equal validated worker10ff059f9; root user registry
and processor hashes unchanged. Source55e641fb1, evidence9b84238d4/10ff059f9.
Actual Go peer final test and exact all-target strict Clippy pass; evidence
at memory/evidence/matching-history-tchannel-20261020-attempt6/README.md.

Runtime owns sole build lane. Initial candidate omitted accepted SD config and
user callback dependencies; fixed only in isolated worktree. A duplicate local
cache1.6GB was accidentally created; all further builds require shared target.
First logged compile failed String-to-anyhow conversion. Runtime merged full
accepted rootfdb3b971a, coherent checkpoint7070bfaf3, post-merge no-run passed
exit0 in tmux sd-dns-norun (6m37s). Evidence matching-dns-build-20261020.
Focused config pass,10 SD tests pass; routing9/10 initially with incorrect
expected spectator calls4 vs actual3; parent reviewed and accepted test-only
correction, rerun pending. Processor filter matched0; extract prior isolated
50ce04f1b test seams/cases, not evidence of coverage. Dynamic Go oracle and
actual SD Go interop/mixed production SDK gate still pending. Production static
TChannel selection still needs wiring after accepted transport merge.

Runtime source75d334913 separately imports user processor snapshot;2ac06b995
main/executor assembly;b68e183ff peer TTL routing cache;bef46add3 cleanup fixes.
Explicit full config merge required, not selected struct copies. Root user files
remain preserved. Do not release lane before planned focused/mixed validation.

Swim new worktree matching-task-store-abstraction, branch
codex/matching-task-store-abstraction:03524be3f backend-neutral sync trait,
32a240b7f MySQL task adapter source,1c234e2d MySQL domain reader source.
No compile/tests/lock resolution. mysql26.0.1 minimal-rust/rustls-ring proposed;
Cargo.lock deliberately unchanged. Per-call timeout/config scope still open.
Real MySQL Go/Rust interop harness source underway; Docker available/no MySQL
container running. Avoid stale original worktree paths for next agent task.

History worker now matching-telemetry isolated worktree:9a137f96a sink+outer
History instrumentation,a64c4bb0b cancellation guard,d4d3715db Prometheus sink.
Source-only unbuilt. Real Go Tally reporter oracle and summary timer support
underway; actual Go default is summary (config comment wrong). StatsD/M3 and
production metrics injection/instrumentation remain required. All new slices
serve Matching only; no next-service migration.

### Resumed checkpoint 2026-10-20

Root user AGENTS.md, matching_registry.rs and untracked matching_shard_processor.rs remain preserved; do not import worker whole-file replacements over them.
Runtime still owns sole serialized build lane/shared target. Coherent routing/config/SD/processor/ownership/shutdown/callback tests passed, binary check passed, pinned Go dynamic configuration oracle passed. Static History transport selector and production callback parity fixes are in isolated runtime candidate. Actual Go SD gate exposed required YARPC metadata (service shard-manager, caller cadence-matching, encoding proto), then missing heartbeat TTL. Adding tonic grpc-timeout to heartbeat/GetShardOwner advanced gate to reassignment; Ready versus DONE assertion is being checked against pinned Go enum. Gate has NOT passed yet. Final strict lint/build, mixed SDK isolation/restart and both History transports remain pending.
MySQL source/harness remains uncompiled. Rejected KILL QUERY cancellation draft because of connection ID reuse/control connection/deadline hazards. Worker replacing domain cancellation with dedicated nonpooled mysql_async connection on owned runtime, future-owned permit and abort on deadline; real cancellation/drop/maxConns tests required. No invented fixed task DB timeout: Go task calls use context.Background; domain cache refresh has outer and child budgets.
Telemetry exporters and Go oracles remain source-only/unrun. Inbound shared-trait instrumentation added for 13 methods with actual request dimensions, typed status counters and cancellation latency; task-list kind/context dimensions and actual gRPC/Thrift once-only tests still assigned. Production mounting/full metric parity remains pending.
Matching remains incomplete. No History service migration started. Workers must explicitly set isolated workdir on every call because resumed calls otherwise default to root. Artifact directories may contain binaries/databases/private TLS keys: stage selected evidence only.

### Runtime wire gate passed; production mixed rerun underway

Runtime isolated source frozen at 7030f2ef6; actual pinned Go shard-manager gate passed 1/1, bounded rerun passed, focused unit tests7/7 and exact strict Clippy passed after equivalent toolchain lint cleanup. Production binary build passed. Actual wire corrections: rpc-service=shard-manager, rpc-caller=cadence-matching, rpc-encoding=proto, transmitted grpc-timeout on Heartbeat/GetShardOwner. Go JSON enum statuses are ShardStatusREADY and ExecutorStatusDRAINING. Initial false test expectations retained as failed evidence. Fixture reads/cleanup now bounded.
Mixed harness correction b900ee9c8 uses absolute dynamic-config filepath. Prior relative-path runs fell back to noop config and cannot prove isolation or selected History transport. Corrected gRPC mixed run reports preliminary-passed: query, isolation, partition config, persisted backlog SIGKILL/restart and graceful restart with same Go services process. Evidence still in isolated runtime memory/evidence/matching-dns-interop-20261020/mixed-grpc-fixed; TChannel independent rerun pending. Source/evidence not yet accepted into root. Shared NODE_PATH is required for grpc-js. Never stage full mixed artifacts (DBs/binaries/keys).
MySQL source checkpoints c0f9bb448/a52080cb3/a134b49e4 remain uncompiled. Vendored mysql_async0.37.1 minimal deadline patch preserves Go dial-only timeout, per-I/O timeouts, inherited greeting/SSLRequest absolute deadlines during TLS handshake; licenses/checksum recorded. Validation runner prepared; lock unchanged. RPCs currently use in-memory AppState domain snapshot; background cache performs persistence refresh, and adding synchronous cache misses to handlers would require spawn_blocking. Worker now audits/implements PostgreSQL Matching adapter source independently while awaiting lane.
Telemetry checkpoints87f1e34e1/8caf0088c/19b3634f7 source-only: inbound13 RPCs, typed counters, dimensions, caller/transport via scoped contexts (grpc/http/tchannel), cache-only metric domain resolver. No persistence I/O introduced by metric resolution. Remaining queue/engine/registry/scaler metric families inventoried; bounded AddTask/manager lifecycle work assigned. All exporters/Go comparisons still await executable validation. Root user hashes remain unchanged.

### Accepted bounded MySQL adapter validation; next wire gates

Runtime evidence package pushed456e6e7dc/4e1fedba4 on t3/matching-runtime-owner. Final isolated-config gRPC AND TChannel production mixed runs passed SDK/query/isolation/native partition/SIGKILL+SIGTERM recovery. Rust binary7030f2ef6 SHA19a97efebf19183c589e96a56a61119425e7ef0752fdcadf089e4a0fdf31956a, harness9e4559a92. Earlier shared flag inadvertently switched Go Frontend→Matching to Thrift, whose Go DescribeTaskList schema lacks partition_config; final harness splits Rust and Go dynamic config. Evidence transport-matrix.md accurately limits acceptance; source not integrated into root yet.
Runtime new unvalidated source a1f86f39d adds YARPC metadata on5 outbound Matching gRPC paths, actual Go peer oracle invoking production forwarding, and combined configured-SD production harness. Component SD test plus default-onboarding mixed runs alone do not prove configured namespace production behavior. Next lane assigned runtime for these real gates, then strict lint/build. Two-host failover/production idle unload harness also source work.
MySQL adapter validation PASSED frozen6d0f77b05; evidencece49d28c0 in matching-task-store-abstraction. Exact workspace Clippy -D warnings, unit11/11, vendor TLS deadline tests2/2, live timeout7/7, TLS1/1, real Go seed→Rust adapter interop1/1→Go verify on MySQL8.0.36. Disposable container removed. Fixed clock conversions, trait Arc coercion, negative-zero duration, authenticated SELECT1 readiness, real Go plugin registration, connection-slot completion ordering. Runtime ownership fixed using dedicated owner thread so final pool drop on Tokio worker cannot self-join; regressions verify shutdown/weak reference cleanup/maxConns1 follow-up. Source/evidence still isolated; adapter success is not production MySQL service parity.
Swim now source-only production MySQL runtime integration, merging validated runtime4e1fedba4 into its branch before configured factory/dyn store/cache wiring. PostgreSQL adapter2a88afd02 compiles/lints but TLS/domain/runtime/real Go interoperability remains incomplete. No Cassandra completion claimed.
Telemetry source-only checkpoints91f57f20f/c9dca697f/02ad1c472 preserve transient forwarded task/poll carriers, poll success/sync/timeout/direction/isolation and started-error counters, query path metrics and Go oracle source. No execution yet. Production exporter config/builder/mount now assigned; at most1 reporter per Go (multiple fatal). Full matcher/scaler/queue families remain open. Runtime owns sole build lane; telemetry and swim must not build.
Root NEW matching_runtime.rs user overlay preserved, hash05e1e8d4b23a988fb7e8af0947e883295879e736378f15230743c9b022cd7bd1 (duplicate with_domain_cache in in-memory constructor, once durable). All workers deny modifying it; treat as user work. Original AGENTS/registry/processor hashes remain unchanged. Defer root integration until additive conflict plan and telemetry dependency validate; never blind-copy overlap files. Resumed workers continue defaulting ROOT on some read calls: explicit isolated workdir on EVERY call and absolute patches required.

### Resumed orchestration checkpoint: forwarding wire oracle

User explicitly confirmed preserving all main-checkout edits. AGENTS.md, matching_registry.rs, matching_runtime.rs and untracked matching_shard_processor.rs remain untouched; no root implementation integration.
Runtime forwarding oracle rerun2 reports exit0, all3 production-path filtered tests passed. Go YARPC consumes reserved rpc headers; oracle now checks call.Service(), call.Caller(), call.Encoding() and actual deadline context, retaining Rust wire metadata. Evidence isolated artifacts/matching-production-runs/forward-oracle-rerun-2/run.log. Configured-SD production attempt failed only on missing isolated Node modules; rerun1 uses root cadence-web NODE_PATH and is active. Acceptance pending exact production binary source/hash verification, configured-SD result and two-host failure/idle eviction gate. Runtime retains sole heavy build lane.
MySQL configured production wiring source55c4ae9ad follows validated runtime merge8d8c47854, unvalidated/no builds; worktree clean. Actual configured service MySQL mixed SDK/restart/GC remains required. PostgreSQL source-only follow-up assigned while waiting lane.
Telemetry source734a37d6e mounts services.matching.metrics and Matching pprof config, shared Matching/registry/History sink, graceful reporter flush. Source-only formatting passed; Cargo/Go validation pending. Correct Go selection semantics: empty StatsD alone creates NoopScope allowing Prometheus; prior M3 plus empty StatsD rejects. Addressless Prometheus uses default mux served by configured PProf, never main RPC HTTP. Dedicated bind errors warn. Full pprof handlers unsupported and recorded. Further Matching metric families source-only assigned.
Matching remains incomplete. No subsequent service migration authorized until complete Matching parity and reproducible mixed-service evidence.

### Exact production binary and configured-SD gate

Runtime frozen27268085f built successfully with identical before/after source hashes; binary SHA679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa. Evidence isolated memory/evidence/matching-dns-build-20261020-2726808. Configured-SD production mixed gate exit0/probeExitCode0: Go Frontend/History/Worker, no Go Matching, onboarding100%, SDK/query/isolation/native partitions, persisted row SIGKILL and SIGTERM recovery, single completion and same Go pid. Bounded evidence only, not service acceptance. Prior readiness failure was harness enum acceptance (ShardStatusREADY); retained. Go forwarding peer oracle already3/3 passed.
Two-host parent review fixed survivor selection (secondary may be killed), direct secondary readiness and subprocess status checks. Wrapper d940ef5 adds missing --rust-matching; no server source change. Subsequent two-host probe failed public call with ABORTED task-list ownership on isolated list. Worker investigating real routing/ownership compatibility versus harness assumptions; do not weaken SDK assertion. No failover success claimed.
Telemetry source e7e233e25/925d3946c matches Go poller-history unique identity/TTL count and callback emit points;90d7d1460 preserves matcher-entry timestamp over forwarding retries for local/forward timing pairs. Unbuilt. TaskReader metric/context API plumbing authorized current-service only, exact Go backlog/count/lag semantics required; coordinate trait signature with persistence worker.
PG separate worktree matching-postgres-parity from frozen MySQL55c4ae9ad; audit607d243d9 and implementation underway source-only. Incidental rustfmt changes in unrelated domain/execution/history/semaphore files restored. Configured MySQL55c4ae9ad remains frozen awaiting lane. Root four user files hashes reverified unchanged.

### MySQL configured build and worker fallback

MySQL factory compile found concrete SqliteTaskStore partition refresh dependency; fixed df3466479. Frozen candidate865d82c0b+df3466479: durable queue19/19, server factory5/5, strict Clippy status0, production build status0. Binary SHA35e25467dfb6961d41d30565260c6e4afd9ed80b54857a403445cac57131817d at matching-task-store-abstraction/artifacts/frozen-production/cadence-server-task-store-abstraction (untracked, never stage). Logs /tmp/cadence-mysql-{matching-queue-focused,server-focused,clippy,production-build}.log need selected evidence preservation. Actual configured MySQL mixed gate not yet run. GNU ld test link took14min with tmpfs pressure; stale26 go-build directories cleaned after checking no refs and preserved runtime binary moved disk-backed, /tmp1.2GB now. Runtime frozen679e binary now matching-dns-provider/artifacts/frozen-production/cadence-server-679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa, hash unchanged. Disk~23GB free.
Swim provider usage limit interrupted immediately before MySQL gate. T3 fallback delegated OpenCode opencode/beam-free medium to continue same existing MySQL validation worktree (shared child test task, explicit cwd every call), owns sole build/mixed lane. TaskId node:delegated-task:command%3Amcp%3A26e1065c-30ed-4750-99fc-af78d84d24d2%3Adelegate-task%3Amatching-mysql-validation-fallback-20261020-1. Do not duplicate while active. No Go service evidence claimed yet.
Runtime evidence e3f9aa3d7 pushed selected3/3 forwarding + configured-SD exit0 and source/hash files. Two-host harness3e9ec5c scopes idle override to dedicated domain/list, repolls permitted empty decisions, persists stage checkpoints and adds pinned Go typed ownership error oracle; unrun while MySQL lane. Current parity auditcc90ed1b1. Runtime separate matching-semaphore-durable worktree has55c4ae9ad generic backend merged, SQL semaphore paritya0675229b source-only, async semaphore boundary/Cassandra scylla source underway. Go distributed semaphores ONLY Cassandra; SQL must return exact unsupported error, no private SQL tables. Cassandra existingv0.50/51 schema/LWT/sentinel contract required. No runnable Cassandra support claimed.
PG source-only isolated matching-postgres-parity0de46351d/15bda07c7 adds TLS/factory/domain cache/harness; no build/lock resolution, sslmode allow remains unsupported gap. Telemetry source-only through135003e3b includes task count/lag/expiry/forwarding/poller history and match timings/attempt events; remaining lease/writer/scaler/isolation gaps under implementation. All require executable oracle/exporter validation. Main checkout four user files remain preserved/hash-identical; no root code integration. Matching incomplete; no History migration.

### Continuation: evidence repair, two-host fixture corrections, telemetry candidate

Previous goal turn made progress (source/build/test/gate evidence); goal remains full migration, Matching incomplete. MySQL beam fallback actual production mixed-run2 EXIT0/probeExitCode0 (parent inspected raw results, mysql defaultStore, SDK/query/isolation/native partitions/SIGKILL+SIGTERM one row/one completion same Go pid), but worker deleted passing raw artifacts before committing only12line note4bc9873b9. Do not treat note alone as reproducible full audit. New OpenCode minimax-m3.1-flash-free medium task prepared9465af285 source-only GC/domain/evidence-retention harness and pushed. Offline decoder/gate checks pass. Followup task node:delegated-task:command%3Amcp%3A26e1065c-30ed-4750-99fc-af78d84d24d2%3Athread-send%3Amysql-gc-run-grant-20261020-3 NOW owns sole validation lane, no Cargo builds, actual MySQL gate with frozen35e254 binary + disk-backed original Go0ed004a0...9bb5. Must preserve selected complete logs/results/hashes/DB observations before cleanup; bounded readiness/queries, no private schema. Parent requested Go-derived ack semantics and stable range snapshots rather than unjustified +/-1. Root user dirty files remain user edits, not worker edits.
Root ps found TWO old runtime Rust Matching orphans (~4h) contradicting cleanup reports: runtime verified exact PIDs2500371/2501471 and SIGTERM reaped both. Harness88a986144 independently reaps both children even when primary shutdown fails. Gate frozen679e baseline27268085f still used. New failed gates saved useful typed Go oracle + idle/crash recovery evidence: range1->2, ack0->1, queue0, one completion. Repeated isolated-list ABORTED was DIRECT Matching.AddActivityTask hardwired primary, NOT Go Frontend public routing; corrected5e7262966 selects exact owner agreed both views. Followup range expectation1 invalid after handoff: e43f438e7 checks Go100k aligned block versus persisted range, singlehost retains1..100000. da6e3d2c0 fixes bare-port direct-client address to host:port. Rerun4 passed these checks but generic FE DescribeTaskList omitted recent service-probe poller; could be manager turnover/ownership rather than retention bug, still investigating. Rerun4 terminal, explicit audit no service/harness processes. Diagnostic source captures both owners/direct pollers/public block on failure; no new mixed run while MySQL lane.
Telemetry source through932ffe0ee (partition/standby/unknown/query metrics) merged MySQL4bc9873b9 into candidate233698d36; pinned/pushed matching-telemetry-mysql-candidatec4326a006, correct Rust2021 formatting. Main/grpc conflict audit recorded preserving generic configured store, metadata cache, shared metrics/History, named-port membership/listeners/SD lifecycle. No builds/tests yet; next Cargo lane after short MySQL gate. Explicit CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-6f14baa6/rust/target jobs1 mandatory. Semaphore context timeout metric still lacks current async outcome (runtime separate unvalidated implementation).
Cassandra source separate matching-semaphore-durable:1a0deb8a9 async semaphore persistence/session, c5b5578e9 CQL sentinel/metadata IF NOT EXISTS/token+owner lookup/logged LWT grant/release and free slots from metadata budget (Go does not seed token rows). Domain names resolved to UUID before persistence. Still unbuilt/no lock resolution/no task factory runtime support. Queue sync DB calls actually run on Tokio reader/writer paths; runtime assigned explicit spawn_blocking boundaries before safe dedicated-runtime Cassandra task adapter, preserving task background-context semantics. No full Cassandra support claimed. No root code integration/next-service migration.

### Post-compaction continuation, 2026-10-20 18:43 UTC

Preserved all four root user paths without implementation edits. Read both terminal MySQL notifications: prior production run passed but GC/evidence remained incomplete; evidence repair initial task completed source only, while followup mysql-gc-run-grant-20261020-3 remains active. First retained gate at 183524Z failed because no persisted IDs were observed after delivery. Worker corrected harness to observe known persisted backlog before delivery, checkpoint 8c253f417 following stable range/Go ack bounds a41a6bb4c. Actual MySQL rerun 184311Z is now active with original Go binary and frozen Rust35e254; sole build/mixed lane remains assigned to it. No passing GC claim yet.
Runtime two-host diagnostic 9a2f08487 captures both ownership views/direct/public pollers for missing service-probe history; full gate still failing, rerun deferred. Manager-local poll history can disappear on handoff; do not weaken assertions without diagnosis. Earlier typed ownership, idle unload, crash recovery and range/ack stages passed; not full completion.
Cassandra source checkpoints a9e406a8b/d1bbb2be9 move blocking persistence operations off RPC timers. Latest pushed 1c88cd556 on matching-semaphore-durable owns dedicated driver Arc<Runtime>, dispatches drop to plain shutdown thread, restricts session escape. Task/domain adapters/factory still in progress, unbuilt. PostgreSQL worker resumed source-only TLS allow/connect precedence and real pending-task Go handoff work. Telemetry candidate c4326a006 remains next validation lane; actual exporter oracles required in addition to focused tests. Matching remains incomplete; no migration work on later services.

### 2026-10-20 continuation: retained MySQL failures and telemetry validation

Root user four file hashes reconfirmed identical. PG source532301c3 pushed: allow-mode retains CA/client cert/key, plaintext-first+restricted SSL-off fallback/attribute precedence tests, actual pending-task Go SDK handoff harness; unbuilt/unrun. Cassandra source55413dbf4 pushed adds task/domain UDT adapters+configured factory and range-fenced logged batches; no build, further Go CQL audit/harness underway.
MySQL harness7d7e7f38c fixed restartWatermark scope and added static no-undef checks. Run184551Z failed metadata checks because harness mistakenly used domain shard54321 for task_lists instead of computed task-list shard0 (single DB). Live raw observations confirm55rows shard0,22domain rows; strict observed durable task IDs covered by persisted acks, control row retained. d2d3a1800 preserves failed runs183524/184311/184551 including raw statements/results/exits/config/hash evidence, fixes query and fast-fails missing metadata. No fresh corrected full gate yet. Proposed ackMayLag bypass was rejected/removed; unobserved activity queues explicitly carry no durable GC evidence and Go initial lower bound0, observed queues strict.
IMPORTANT worker ack divergence claim RETRACTED with exact source auditb5e6ba875: Go taskListDB has no CompleteTask API; completion updates in-memory ack then GC, timers/teardown persist, same Rust semantics. No product change warranted. Corrected memory notes pushed; final corrected MySQL gate queued, container idle.
Telemetry d645c1630 adds direct Rust/Go StatsD and M3 output comparators alongside Prometheus. Sole Cargo/Go lane granted, serialized tmux script started. First common selection test1/1 passed, next server compile failed23 merged source errors; worker fixing before continuing focused nonzero tests/exporter oracles/strict Clippy/build. No configured mixed exporter gate passed yet. All implementation stays isolated; Matching incomplete.

### Telemetry executable verification continuation

Telemetry candidate compile fixes63a2525ba/0040afaaf pushed; actual focused common1/1, limiter1/1, metrics29/29, standby1/1 passed. Limiter test timeline corrected against Go stale-report TTL behavior. Metrics fixes preserve labels on second timer/histogram event, Go tally1-based M3 bucket IDs, Go sanitizer. Go AddTask oracle96c01f7f1 corrected tally snapshot pointer/value and metric-family counting. Go poll/AddTask/config oracles passed. Worker initially reported whole remaining chain exit0, but parent independently inspected captured final.exit1: Go Prometheus scope.Close compile failure. FULL PASS CLAIM INVALID; fail propagation corrected, helper6f4c84932/c71bde3fc fixed, normalization057893a11 only label ordering. Parent inspected real final2 log: Prometheus4scenarios + StatsD2scenarios pass. M3 still fails decoder. Current encoder source changed binary→Compact; independent swim source/raw packet audit found old /tmp/m3-rust-packets.hex strict-binary80010004 while current source emits Compact8281, Go raw UDP Compact no extra prefix. Worker verifying fresh source/executable/hash/capture before acceptance. No strict lint/build/full mixed telemetry gate accepted yet; sole lane remains telemetry. Failed logs retained pending selected evidence commit.
Cassandra checkpoints615cdf1f0/7303a0f66 source-only: Go config defaults/timeouts, ALL consistency delete fallback, native OS trust roots, Go authenticator allowlist; lock unresolved new deps while lane occupied. Narrow Scylla vendor SNI override and public-trait roundrobin source underway, hostpool/default token-aware gaps must still be implemented. Actual adapter fixes include Go range DELETE, opaque paging state, checked timestamp/limit conversions, dynamic CAS decode, now_nanos trait propagation all backends, executable Go/Rust restart probe; unbuilt. PG integration plan28c06249 records overlaps/root user injected-clock processor preservation. No main implementation integration; four user paths preserved.

### 2026-10-21 resumed after interruption

Fresh MySQL corrected production gate194644 EXIT0/probe0 verified by parent, no Go Matching, original Go FE/History/Worker. Strict observed durable GC IDs/acks pass; unacked control1 survives, schema/domain pass, SDK SIGKILL+SIGTERM one completion each. Complete25file evidence pushed efee2576e; container/processes cleaned. Telemetry fbb3b86ef has actual Compact M3 Go/Rust semantic evidence; dirty7files include subsequent lint/shutdown fixes and --matching-exporter runner. Prior worker hit usage limit before final lint/build/freeze/mixed exporter; do not claim completed. Cassandra0b9fd4bb4 clean includes source hostpool3bbf plus validation script, but runbook incorrectly or inconsistently says configured factory absent/SQLite mixed; new worker must inspect actual factory and implement real Cassandra gate.
On resume no active builds/baseline processes. Root four known user path hashes unchanged; new dirty lib.rs one-line delta also preserved as user WIP. No root implementation integration. T3 Luna medium standard workers started: telemetry-resume-20261021-1 owns sole Cargo/Go/mixed lane in matching-telemetry; cassandra-wiring-resume-20261021-1 source-only in matching-semaphore-durable. Task prefix node:delegated-task:command%3Amcp%3A1dcae31e-33fb-4591-b39f-e5e6c53b6c24%3Adelegate-task%3A. Returned options verified reasoningEffort medium/serviceTier default. Matching incomplete.

### 2026-10-21 source findings and implementation continuation

Cassandra5521d0809 pushed: actual task/domain factory already existed; missing main.rs configured semaphore-store initialization now added. Old runbook factory-absent claim corrected. Followup task cassandra-real-harness-20261021-2 must IMPLEMENT --cassandra mixed original Go/Rust harness (not merely document SQLite limitation), source-only while telemetry lane.
Twohost auditdb148fd8a confirms Rust remote partition refresh absent (Go notifies old/new non-root read partitions); domain callback scoped to newly notified global non-active-active domains, not generic config unload. Worker incorrectly avoided isolated registry/runtime due root protection; clarified protection only ROOT user files, followup twohost-implement-confirmed-gaps-20261021-2 now implements exact remote refresh/callback + poller harness in isolated runtime worktree. No gate acceptance yet.
Telemetry strict Clippy process3019823/3019842 was verified live compiling cold dependencies. Parent reviewed shutdown aggregate: it already captures shutdown error and bails, so adding removed shutdown? would duplicate move; worker agrees preserve existing path. Telemetry source review also found uninitialized Go idls submodule, handling prerequisites. All three tasks active, only telemetry can build/run. Root user edits remain untouched.

### 2026-10-21 merged telemetry and partition review

Telemetry20803f369 commits validated strict workspace/all-target Clippy evidence and final telemetry cleanup/exporter harness. Mergefaed87d28 combines MySQL efee2576e strict GC/evidence and exporter capture. Parent caught capture-only false acceptance (swallowed scrape error/empty UDP); worker now implements decoded positive SDK metrics+same-sample actual domain/caller/transport/service/operation assertions and evidence collection, no arbitrary first M3 sample or cross-sample label union. Production cargo build PID3033166 verified live from final unchanged Rust inputs, jobs1/shared target; binary must be disk-frozen/source+SHA before further builds. No configured runtime exporter gate passed yet.
Runtime source d30f2ef85 implements remote old/new nonroot READ refresh and scoped failover callback. Corrected old persisted-list claim: Go engine.go1206-1213 enumerates loaded ManagersByDomainID then maps GetRoot, so root mapping is pinned Go behavior. Independent review partition-failover-review-20261021-1 reported sequential/no-context peer refresh defect (valid; implementation followup partition-review-fixes-20261021-3 active) and nonroot-release claim (disputed by explicit Go GetRoot; verify before changing). Still source-only, no gate acceptance.
Cassandra real harness task remains active; parent caught WIP null observation fallback breaking SQLite and restart/graceful paths retaining SQLite dependency. Worker directed backend observation abstraction for both SIGKILL+SIGTERM and preserving existing branches. No Cassandra build or real mixed result. Root five user dirty paths preserved, no implementation integrated.

### 2026-10-21 post-compaction checkpoint

Root five user dirty paths preserved; no implementation integrated. Telemetry production build passed exit0 (7m31s), binary SHA fa35738b543ff1d7759995442ce4e11c55693e522540c006b039c90edcf031b5; evidence attempt-20261021-production-build. Telemetry task remains running and retains sole Cargo/Go/mixed lane, steered to freeze disk binary and complete strict same-sample exporter SDK/shutdown gates. No configured exporter runtime pass claimed.
Runtime e4042831d pushed: concurrent JoinSet refresh, remaining caller deadline, cancellation aborts; independent partition-failover-review-20261021-2 completed with no actionable source finding. Synthetic timing tests do not verify RPC path; compilation/twohost gate pending.
Cassandra aeba2c46f committed/pushed: true Cassandra mixed harness uses CQL selected observer for crash/restart/graceful drain, Go schema/configs and Cassandra-only continuation paging; source/offline checks passed, no Cargo/Go/container runtime run. Independent source review cassandra-harness-review-20261021-1 delegated Luna medium/standard. Cassandra runtime remains queued after telemetry. Matching incomplete; later services untouched.

### 2026-10-21 configured exporter failure and review fixes

Telemetry ab4aa2d13 pushed with frozen production build and strict exporter/shutdown harness. Actual Prometheus mixed MySQL SDK/crash/graceful probe exits0, but full exporter gate exits1: missing cadence_client_requests; no exporter pass. Raw repeated233MB scrape compressed5.8MB with raw hash; active worker diagnoses Go registry/History instrumentation, also actual query transport grpc contradicts harness assumption tchannel. Preserve failed evidence and fix exact behavior, do not waive assertion.
Cassandra review P1 wrong semaphore row type fixed/pushed0c42f39c4: exact owner type2/token sentinel-1/held token and owned-container cleanup. Round2 confirms fix; its missing-wiring claim is contradicted by matching-service-compat.cjs312/531 explicit verify-semaphore invocations and baseline production probe selection. No runtime validation yet. Runtime real RPC regression00adafc26 had copied-i32 dereference defect caught by parent; c0248acec fixes and asserts fast peer refresh before slow deadline. Both source only; no root user edits touched. Matching scope audit delegated matching-final-scope-audit-20261021-1, read-only outside known queued work.

### 2026-10-21 Prometheus production pass and remaining implementation

Confirmed final Prometheus mixed test actual run/probe/launcher exits all0 at memory/evidence/matching-telemetry/attempt-20261021-exporters/prometheus-final (telemetry worktree). Frozen source1c7f6b6b9, binary a863a56e2864a1fa5dfea677f04915ffff559240130980ee54d40e98a741049f. Missing History metrics sink was real startup bug fixed1c7f6b6b9; strict all-target workspace Clippy/build0 evidence rebuild-client-metrics committedc7266efb7. Intermediate retry correctly failed caller hyphen/underscore expectation; sanitizer correction55b2240ae uses pinned Go behavior for every label. Final positive SDK query SAME sample exact domain/caller/service/operation/grpc, required add/poll/client metrics. SDK SIGKILL+SIGTERM strict MySQL probe0. StatsD/M3 still pending; bounded exporter pass is not Matching signoff.
Live drain fix888027bde pushed Cassandra branch: provider read after eviction, test initial5ms changes137ms uses injected wait; source formatting/diff only. History outbound TChannel audit corrected: existing live Ringpop client path already present, false rejection claim stale. Selector regression + queued fresh transport evidence5ffa27271 pushed. Config property audit359c5bb09 found actual RPC host/domain user/worker quota and persistence host/cluster QPS gaps; implementation tasks matching-rpc-quota-implementation-20261021-8 (runtime child) and matching-persistence-qps-implementation-20261021-5 (Cassandra child) active SOURCE ONLY. Telemetry retains sole build/Go/cluster lane. Throttled logger limits remain followup gap.
Read-only integration assessment completed matching-integration-refresh-20261021-1: shared ancestor4e1fedba, broad main/lib/config/runtime/store conflicts; compose stores+metrics+ownership lifecycle, preserve explicit now_nanos and root injected clock/tests. Proposed telemetry→PG→Cassandra→runtime assembly, but exact semantic merge required. Root USER five paths remain unchanged, no root integration.

### 2026-10-21 M3 success and provider fallback

StatsD final safe-packet run/probe/launcher0 committed9de2c8b18 (205 packets max1430B,70 postSIGTERM packets all positive required metrics and qualified query). First StatsD failed because harness flushBytes10MiB exceeded UDP limit; actual fatal EMGSIZE, not merely membership warning;9aaeac777 preserves failure and1432B config fix. M3 completed despite worker interruption: /tmp/matching-telemetry-m3 results run/probe0, evidence attempt-20261021-exporters/m3 launcher0,22 Compact batches/1471metrics, exact positive SDK query domain/caller/service/op/grpc sample,17postSIGTERM packets. Evidence finalization remains needed. All three bounded exporter gates pass on source1c7f6b6b9 binarya863a56e; not full Matching parity.
Luna RPC worker failed429 retry exhausted, persistence worker usage-limit; telemetry interrupted. Confirmed all terminal before resuming, no python runner remains. Authorized free fallback opencode/beam-free variant medium verified and tasks active: telemetry-finalize-fallback-20261021-1 (matching-telemetry, sole build/mixed lane, finalize evidence then prepare next runtime gate), rpc-quota-fallback-20261021-1 (matching-dns-provider preserved dirty quota WIP), persistence-qps-fallback-20261021-1 (matching-semaphore-durable preserved dirty wrappers/limiter WIP). Same task prefix node:delegated-task:command%3Amcp%3A1dcae31e-33fb-4591-b39f-e5e6c53b6c24%3Adelegate-task%3A. Root USER five paths untouched. Runtime dirty tree MUST NOT build until quota worker commits. All other workers source only, no concurrent Cargo/Go/cluster.

### 2026-10-21 resumed quota review checkpoint

M3 evidence finalized/pushed864dcdbe9. PostgreSQL validation fallback task postgres-validation-fallback-20261021-1 remains running and owns sole Cargo/Go/container/mixed lane in matching-postgres-parity; requested concrete intermediate exits/process/log update. No PG pass claimed. RPC quota source committed/pushed7c0ba34dc on t3/matching-runtime-owner; persistence host/cluster QPS source committed/pushed58fb1a17b on t3/matching-semaphore-durable. Neither compiled/tested. Independent SOURCE ONLY review tasks matching-rpc-quota-review-20261021-1 and matching-persistence-quota-review-20261021-1 started with opencode/beam-free variant medium verified, original Go comparison required. Root user five paths preserved; implementation integration remains pending. Matching incomplete, later services untouched.

### 2026-10-21 quota review findings and PG compile recovery

RPC empty/cache-missed domain must still consume domain fallback bucket (Go passes possibly empty domain); fix committed/pushed2fb61b211. Worker initially deferred throttled logger despite source-only authorization; reissued explicit implementation task logger-implementation-not-defer-20261021-3, now active in runtime worktree. Parent directly traced Go common/clock/ratelimiter.go325-350: SetLimitAndBurst deliberately does not advance time; RPC DynamicBucket.refresh incorrectly calls tokens(now) first. Directed correction/regression rpc-refresh-time-correction-20261021-4. Persistence second review retracts initial-zero bug: Go persistence/client/factory.go buildRatelimiters only installs limiter if initial maxQPS()>0. Its claimed ttl4s nuance is false: Go ttl-1 subtracts1nanosecond; integer-ns >=5s matches. No persistence fix based on those invalid claims. PG actual compilation found unavailable driver SslMode VerifyCa/VerifyFull variants, worker repairing distinct TLS verification policy; live Cargo3171042/rustc3171338 observed. No PG gate pass. Root user five files remain preserved; all implementation isolated.

### 2026-10-21 combined source candidate started

New Luna medium STANDARD tasks verified running: matching-logger-actual-implementation-20261021-4 in matching-dns-provider (actual shared throttled logger + longpoll deadline validation + RPC no-clock-advance correction; prior Beam only documented dependencyc253324be and did not implement); matching-semantic-integration-20261021-1 isolated branch t3/matching-source-candidate worktree /home/ubuntu/.t3/worktrees/cadence-rust/t3-db1d9740. Integration inputs committed telemetry864dcdbe9, PG28c062496, Cassandra58fb1a17b, runtimec253324be; pending PG TLS/logger changes require later explicit hashes. No root overlay included, user five paths preserved. Both source-only while PG sole lane.
PG persistence build succeeded and tests223passed10ignored in child output; discarded full logs/pipe-tail means no complete reproducible gate evidence yet. Worker currently release server build live Cargo3176202, not tmux; mandatory all-next-run tmux+full logs+actual exit reiterated. Current TLS diff still raw hostname62 verify_callback; directed MakeTlsConnector::set_callback with ConnectConfiguration.set_verify_hostname plus pinned lib/pq require-rootcert policy and wrongCA/wrongHost actual probes. No PG runtime/full workspace lint acceptance. Persistence wrapper review3 inspected semaphore/member status paths and found no proven defects, not exhaustive manager/error/metrics signoff. Matching remains incomplete.

### 2026-10-21 actual logger source and PG driver confirmed

Luna logger worker completed source95c6ad9c2 pushed runtime branch: both polls missing/unparseable grpc-timeout error + shared matching.throttledLogRPS logger default20, dynamic quota refresh no longer advances time. Source regressions added, no builds/tests. Fresh independent Luna review matching-log-deadline-review-20261021-1 checks native TChannel TTL bridge and tonic deadline handling; integration worker instructed incorporate95c6ad9c2. Integration still source-only in t3-db1d9740.
Old Beam PG worker terminal, clean eeaaf4bb0 pushed and no heavy processes confirmed; lane reassigned new Luna matching-pg-evidenced-validation-20261021-2 (task prefix same delegate-task). New worker actual go list -m confirms resolved github.com/lib/pq v1.10.2, matching local ssl.go: allow/prefer unsupported, require+existing rootcert verifies CA. Prior Rust PG allow/prefer emulation claims were not Go parity; corrections/oracles and full mixed gate pending. Only PG owns Cargo/Go/container/mixed lane. Root user overlay untouched. Matching incomplete.

### 2026-10-21 corrected logger refresh source checkpoint

Runtime04e052e60 pushed: logger now uses common RateLimiter, initial rate/default20,5s dynamic refresh no time advance, zero/default/TTL source regressions; poll worker quota precedes missing-deadline validation with precedence regression. Integration informed exact new head; independent fresh review matching-log-review-round2-20261021-2 running Luna medium standard. Prior review claims tonic strips timeout /Request::deadline method unproven and contradicted pinned tonic0.12.3 source (grpc_timeout.rs reads req.headers unchanged; request.rs no deadline API); native thrift_matching.rs300 request.set_timeout bridge exists (review used yarpc HTTP). No source changes based on those false claims.
PG original Go resolved driver1.10.2 confirmed by actual go list. New Luna removed allow/prefer emulation, restores require+CA semantics, exact case handling; compile import NoTls restored after parent catch. Proper logged tmux cadence-rust:pg-parity now live Cargo3185227, no result yet. Sole heavy lane remains PG. Integration still merging Cassandra/runtime conflicts in t3-db1d9740, no heavy commands allowed. Root user five edits preserved, no integration in root; all Matching completion gates remain required.

### 2026-10-21 PG real Go TLS oracle and logger review retraction

Parent inspected /home/ubuntu/.cache/cadence-rust-evidence/matching-postgres-parity-20261021/focused-test.log:5actual matching_postgres config/schema tests passed ACTUAL_EXIT_CODE0. Initial run mistakenly used private matching-postgres-parity/rust/target2.4GB; instructed preserve valid result, exact shared target for every next Cargo, cleanup private build only after safe. New Rust TLS oracle now actual shared target live3196103/rustc3196453.
Real Go lib/pq1.10.2 TLS oracle aggregateACTUAL_EXIT_CODE0 and10cases passed independently inspected results.json: requireCA, verifyCA/full valid, requiremissingCA, verifyCA wronghost successes; verifyfull wronghost, requirewrongCA, verifyCAwrongCA, allow/prefer expected failures. Initial invalid wrongCA fixture used trusted server leaf; prior aggregate1 preserved by worker instructions, no false pass. Rust oracle/mixed SDK/full lint/build still pending.
Logger reviewer claimed emission assertion5s fails, RETRACTED after full sequence: previous5s-1ns event advances latest, refresh5s gives1ns new rate credit, not full bucket. Source806067b1 adds rate/burst/refresh-time assertions retaining correct emission counts; pushed, integration informed. Fresh Cassandra expiry audit task cassandra-expiry-clock-audit-20261021-1 Luna medium standard SOURCE ONLY in matching-semaphore-durable; PG sole heavy lane. Integration worktree still nine conflicts HEAD1082ce04d, asked concrete progress while continuing. No root source integration, user overlay preserved.

### 2026-10-21 source integration and paired TLS progress

Parent independently inspected Rust TLS shared-target runner:8live connection cases, each postgres_tls_live_oracle1testpass actualexit0, aggregateORACLE_RESULTPASS. Full Go10case aggregate0 already verified. Rust require-missingCA initially unset rather than nonexistent path; owner correcting coverage before final evidence. Full backend schema/SDK/recovery/lint/build pending; PG retains sole heavy lane, latest tmux focused-pg live Cargo3199258.
Cassandra source audit found actual adapter discrepancy (not generic clock issue): Go Cassandra plugin reconstructs TTL expiry but nosqlTaskStore.toTaskInfo405 omits it (parent directly checked). Rust now decoded expiry0 and native rowTTL preserved, test/source4328b58d4 pushed. Integration includes Cassandra+QPS ae59fe895 and expiry6f68fb2ca; now merging runtime, only grpc_matching.rs/matching_runtime.rs/shared compat harness unmerged. Exact latest runtime806067b1 should be included. User main overlay untouched, no full Matching signoff.

### 2026-10-21 PostgreSQL stable source and workspace failure

PG corrected driver TLS policy/source live oracle tests committed+pushed58d1d1e32; integration informed to compose without losing explicit now_nanos. Latest shared focused log5pass1ignored(actual live separately verified),exit0. Full locked workspace/all-target Clippy FAILED101: first E0308 concrete SQLite loader vs trait store, next E0277 &Arc<dyn MatchingTaskStore> not trait. Worker repairing abstraction callsites (matching_partition.rs3line diff observed), not accepted full lint/build.
Independent Luna review matching-pg-tls-source-review-20261021-3 completed concrete additional sslinline=true gap: Go plugin permits driver connectAttribute, lib/pq1.10.2 inline PEM supported, Rust rejects. PG owner assigned actual inline connector semantics+paired live oracle beforefreeze; production no secrets in logs/commits. Further source hash pending. Reviewer said implicit home cert lookup unreachable TLS plugin emptyattrs; owner must verify actual configured disabled/override paths before claiming. Integration still runtime merge3unmerged paths HEAD6f68fb2ca; PG sole heavy lane, no duplicate builds. Root user overlay hashes unchanged on latest check. All Matching backend/mixed/recovery/signoff requirements pending beyond bounded TLS.
