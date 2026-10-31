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
- Continuation package CI after the scanner/client/parent-close changes is
  pending; record its exact head, URL and conclusion here before relying on it.

Commits pushed to `origin/worker-core` so far: `3d87ba4cf`, `0ecfb3889`,
`627fe3ce7`, `a1c371111`, `f0a142870`.

