# Matching dependency parity review — 2026-10-25

Independent source review only; no build, test, or runtime gate was run. This
is not a service completion sign-off. Requested candidate was
`27d3ff5f60f0604553824420da0cf61026eda468`; this checkout is
`af0609dbe676fe09dae62be3735ed73fd12745c0`. Production-source claims below
apply to the checkout inspected. The existing audit notes describe prior CI
evidence, but this review does not independently establish mixed-cluster
behavior.

## Reviewed coverage

- Go `go/service/matching/config/config.go:NewConfig` contains Matching's
  typed dynamic-config getters and filters. The existing key matrix in
  `memory/matching-dyncfg-key-matrix-20261023.md` records 59 keys, defaults,
  filters, and Rust readers, with no recorded mismatch. This review did not
  find a new key/default/filter discrepancy; the matrix is static audit
  evidence, not proof that each production call site uses a live value.
- Rust production assembly in `rust/crates/cadence-matching/src/launcher.rs`
  wires layered config, file/config-store dynamic config, runtime OpenFeature
  provider, persistence selection/limiter, domain cache, shard membership and
  discovery, listeners, TLS, and shutdown ownership. Runtime reload/error
  retention and OpenFeature/Unleash details have dedicated notes listed in
  the request and `memory/`.
- Go Matching startup is assembled in `go/service/matching/service.go`; it
  creates the engine from persistence, History, raw Matching client, config,
  domain cache, membership resolver, isolation state, metrics, shard
  distributor and drain/onboarding dependencies, registers both Thrift and
  gRPC, then starts resource and handler. Rust launcher has corresponding
  assembly paths, but source correspondence is not mixed-service proof.
- Metrics conditions/names and the representative oracle plus 74-rollup
  mapping are covered by the existing metrics audit notes. Nine catalog-only
  Go definitions are not active Matching emissions and are not a blocker on
  their own. The oracle evidence is representative, not exhaustive runtime
  evidence for every conditional metric path.

## Concrete unresolved blockers / evidence limits

1. `rust/crates/cadence-matching/src/matching_runtime.rs:1-7` explicitly says
   membership discovery/eviction remains a separate adapter and that this
   owner alone is not complete Go parity. The launcher separately installs
   membership and lifecycle hooks, so this comment is not proof of a deployed
   omission; however, the production composition must be checked as one
   running Rust Matching process. No mixed-cluster startup/failure/restart
   evidence was inspected here to close that concern.
2. Persistence/domain interactions are wired in launcher and runtime, but
   required acceptance includes SDK workflows against Go Frontend/History,
   persistence recovery, and no Go Matching instance. This source review
   cannot establish those interactions or recovery semantics. Parent's
   assigned mixed-gate evidence remains outstanding independently of source
   parity.
3. TLS/listener wiring appears in launcher (including distinct Matching RPC,
   History client, and TChannel paths); source inspection alone does not prove
   certificate reload/error semantics or service discovery across transports.
   Existing dedicated TLS/discovery test notes are not substituted for the
   final mixed-cluster gate.
4. The requested source pin differs from HEAD. Re-run any final source
   comparison on `27d3ff5...` (or establish that this HEAD contains that
   candidate's production tree) before attaching this review to that SHA.

No production behavior gap was established by this narrow review. The blockers
above are explicit runtime/evidence requirements, not claims that an adapter
or behavior is absent. Do not treat this note or prior unit/oracle evidence as
Matching completion.
