# Worker migration progress — 2026-10-31

Branch: `worker-core`, based on `origin/master` at
`94e79ff9835ec0f0f32b2fa2728510a81892f312`.

## Current checkpoint

Continuation at `7845b4939752c16f9b4642b4a598a725a39fbac2` found the earlier
package run terminal: run
https://github.com/auenkr/cadence-rust/actions/runs/41206189276 failed its
test step. Clippy completed; 12 of 13 unit tests passed. The failing scanner
contract test expected `cadence-sys-tl-scanner-tasklist`, while the Go source
`go/service/worker/scanner/workflow.go` defines
`cadence-sys-tl-scanner-tasklist-0`.

The continuation fixes that stale expectation and expands Rust scanner contract
coverage to Go's task-list/history scanner workflow IDs and activity types,
periodic start policies, timeouts, retry constants and workflow ID reuse
behavior. It records all six execution/timer scanner and fixer workflow IDs,
task lists and start policies. Source inspection confirms Go uses 12-hour cron
schedules for task-list/history scanning and five-minute schedules for
execution/timer scanner/fixer workflows. This remains declarative parity
coverage only; no scanner activities or deterministic workflow runtime have
been implemented.

The public gRPC client now also exposes activity completion/failure/heartbeat
and decision completion RPCs alongside polling. These retain the task token,
identity, payload/failure envelope and binary checksum fields; activity
heartbeats return the server cancellation flag. This is transport support only
and cannot safely enable pollers until replay, decision generation and
registered activity execution exist.

Parent-close policy now has an internal History API adapter for its cancel and
terminate operations. It sends the resolved domain UUID, first execution run
ID, Go identity/reason, and the child-only parent execution guard on the
internal History envelope. Error classification, domain-cache lookup,
cross-cluster forwarding and processor lifecycle are still outstanding; this
adapter is not yet wired into an executable processor.

The processor contract now also includes Go's remote workflow ID format,
signal timeout, 20-year execution timeout, 60-second workflow task timeout and
`cadence-worker` remote identity. Domain-resolution planning matches Go's
lookup-before-Abandon order and treats missing domains as successful skips;
other lookup errors stop the batch. Remote client discovery/signal-with-start
and History status classification remain open.

The tonic client now owns both WorkerAPI and Frontend WorkflowAPI clients over
the configured channel and exposes SignalWithStart. Parent-close adds a Go
compatible remote signal request builder with the system domain, random shard
workflow ID supplied by caller, signal name, workflow-ID reuse policy,
timeouts, identity, caller-provided request ID, and compact JSON payload. A
request-envelope fixture checks those values. Remote cluster routing and
generation of the Go-equivalent random request ID/shard remain unimplemented.

Batcher now has a FIFO child-execution traversal helper mirroring Go's
`processTask`: it invokes an injectable rate-limit/heartbeat boundary before
each operation, applies before Describe, skips entity-not-exists from either
RPC, and appends pending children in response order. A Go-derived test covers
the breadth-first ordering and missing-entity behavior. API-specific batch
operations, query paging, retries, heartbeat recovery, and V2 tuning are still
unported.

The scanner now has a nested task-list scavenger decision module. It mirrors
Go's sorted-batch behavior, expired-task cutoff, fairness-cap overshoot at a
whole-batch boundary, 48-hour idle deletion, and scanner task-list prefix
protection. Go-derived tests cover cutoff, task ordering, fairness and the
exact grace-period boundary. Persistence retries, executor pool lifecycle,
orphan-task cleanup and task-list execution are still unported.

Parent-close policy now also has an ordered History outcome processor matching
Go `ProcessorActivity`: EntityNotExists, WorkflowExecutionAlreadyCompleted,
and CancellationAlreadyRequested are treated as success; DomainNotActive
resolves the domain's active cluster and queues the original request detail for
remote forwarding; other History and domain-cache errors remain retryable
activity failures. Go-mirroring tests cover the terminal race, remote routing,
child-only guard, and unclassified failure behavior. The helper is still a
testable orchestration boundary: no production domain-cache/remote-client
provider is wired into a poller, and the workflow replay runtime is absent.

Worker registration descriptors now include Go's task-list and history
scavengers plus the concrete/current/timer scanner and fixer workflow types,
task lists and shardscanner activity names. `WorkerService` exposes these
descriptors along with parent-close and batcher registrations. This records
registration contracts only; no corresponding pollers, replay handlers or
scanner persistence activities are executable yet.

This is an initial crate bootstrap and partial contract/logic port, not Worker
parity. Cargo was not run locally. `git diff --check` passed before each push.

Implemented in this slice:

- Replaced the placeholder Worker crate with modules matching the requested Go
  subsystem boundaries: archiver, asyncworkflow, batcher, diagnostics,
  domaindeprecation, esanalyzer, failovermanager, indexer, parentclosepolicy,
  replicator, scanner, scheduler, workercommon, service, runtime and client.
- Added a Worker-specific configuration facade and the shared CLI/config entry
  point. `version` and `check-config` work against the shared Go-shaped config.
- Added access to the generated public Cadence `WorkerAPI` gRPC client with
  plaintext and TLS endpoint constructors.
- Added explicit lifecycle and workflow-registration primitives.
- Recorded initial Go workflow/task-list/activity/signal/query identifiers.
- Added parent-close-policy request JSON structs with Go's exported-field names
  and field order, plus ordered operation planning for Abandon, Cancel and
  Terminate (legacy domain fallback, child-only semantics, first run ID,
  identity and terminate reason).
- Added batcher parameter structs with Go JSON names, base64 heartbeat page
  tokens, exact required-parameter checks, supported type checks, and defaults
  for RPS, concurrency, page size, retries and termination children.
- Added a Go-source test fixture for literal workflow and task-list contracts,
  and a beginning dynamic-config key/default audit.
- Rechecked worker dynamic-config defaults against Go and corrected enabled
  defaults, parent-close workflow count, archiver settings, scanner QPS/batch
  and max retention values.
- Worker startup intentionally errors before polling. The Cadence SDK's
  deterministic history replay, workflow decision runtime, activity execution,
  heartbeat/retry behavior and service dependencies are not ported yet. This
  prevents the new process from accepting tasks it cannot safely execute.

## Remaining work

1. Replace the facade config with Go `worker.NewConfig` dynamic-property
   bindings and validate defaults against `dynamicproperties`.
2. Complete the Cadence client: TLS/auth, discovery, retry/metrics, Frontend
   gRPC and TChannel/Thrift methods needed by internal workers.
3. Implement deterministic replay and decision/activity pollers with exact
   Go SDK history, payload, query, signal, heartbeat, retry, cancellation and
   failure behavior. Add Go-generated task/history fixtures.
4. Port each module's algorithms and dependencies in priority order:
   parentclosepolicy, batcher, scanner/executions, archiver, failovermanager,
   domaindeprecation, scheduler, esanalyzer, indexer, replicator, diagnostics,
   asyncworkflow.
5. Add mixed Go/Rust integration gates for SDK workflows and service calls,
   including failure, restart and recovery. Worker is not complete until those
   gates and the per-module Go parity audit pass.

## Validation evidence

- Local: `git diff --check` passed; no Cargo command was run.
- Package CI at `f0a142870c9d9bec76f939840583d513ef848780` failed Clippy only
  on `result_large_err` for the two tonic WorkerAPI poll methods. Added the
  local generated-RPC-style allowance in `af2b555e9`.
- Latest package CI at source head
  `af2b555e9131cbc2049dd111ecff799a7f97db8d`:
  https://github.com/auenkr/cadence-rust/actions/runs/41206189276
  was `in_progress` at last inspection. Check its final Clippy, formatting and
  test conclusion before relying on this checkpoint. Runs `41205510385`,
  `41205587991`, `41205654608`, `41205770812`, and `41205897788` were
  superseded/cancelled or failed as source commits advanced; `41205897788`
  failed only on the corrected lint.
- Follow-up run
  https://github.com/auenkr/cadence-rust/actions/runs/41206890855 failed at
  exact head `1d75ba41628ebbc0a64e995abcd74f70b92da13d`: Clippy found the
  expected large tonic `Status` error on the new History adapter and a
  test-only constant assertion. Both are fixed locally (method-scoped lint
  allowance and removal of the constant assertion). The run's compile stage
  confirmed the new client RPC type signatures.
- Package run
  https://github.com/auenkr/cadence-rust/actions/runs/41207447463 completed
  `success` at exact head `56425246b0398fb4383141c4b730c10a4d10bd54`. Worker
  Clippy, formatting and tests passed, including the scanner mismatch repair,
  retry schedule, parent-close domain lookup and History adapter, and batcher
  FIFO/missing-entity test. This run predates the Frontend WorkflowAPI
  SignalWithStart request-builder commit `cbb8988f1`; that addition still needs
  exact-head CI.
- Exact-head package CI for the Frontend WorkflowAPI SignalWithStart request
  builder is pending.
- Follow-up exact-head CI at `f03cae26b53d517a05150d26cf84aab66adfdbe7`:
  https://github.com/auenkr/cadence-rust/actions/runs/41207805929 completed
  `success`; Clippy, formatting and tests passed for the task-list scavenger
  change. This predates the parent-close History outcome processor in the
  current working tree; dispatch package CI after that change is pushed.
- Package CI at source head `5e6098131ac32220515af213780c1b1fb8fb65c` is
  running as `41208131617` and validates the parent-close History outcome
  processor. The scanner registration descriptors were added afterward, so
  they require a later exact-head run.

Commits pushed to `origin/worker-core` so far: `3d87ba4cf`, `0ecfb3889`,
`627fe3ce7`, `a1c371111`, `f0a142870`.

