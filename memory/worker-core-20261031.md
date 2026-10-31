# Worker migration progress — 2026-10-31

Branch: `worker-core`, based on `origin/master` at
`94e79ff9835ec0f0f32b2fa2728510a81892f312`.

## Current checkpoint

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
- Latest package CI at source head
  `f0a142870c9d9bec76f939840583d513ef848780`:
  https://github.com/auenkr/cadence-rust/actions/runs/41205897788
  was `in_progress` at last inspection. It must be checked to completion; this
  run includes crate Clippy, formatting and tests after the payload/config
  additions. Runs `41205510385`, `41205587991`, `41205654608`, and
  `41205770812` were superseded/cancelled as source commits advanced.

Commits pushed to `origin/worker-core` so far: `3d87ba4cf`, `0ecfb3889`,
`627fe3ce7`, `a1c371111`, `f0a142870`.

