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
