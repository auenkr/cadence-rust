# Matching final acceptance ledger — 2026-10-26

## Pin and disposition

Reconciled against current source pin `10692b2d5a5b912da051a80ca15c30f8236484dd` (master). The prior RPC matrix and gate plan remain useful inventories, but their older-source status statements are stale. The final combined-pin fresh CI belongs to worker `c25`; the serial mixed gates belong to the parent. Adaptive run `40373145351` targets the older `eecb6bef8b5b5c6c36e3e9c7336fa01f451a3f97` source. Do not treat that run as evidence for this pin, and do not poll or dispatch from this ledger update.

**Matching is not accepted yet.** The source audit found no new concrete production mismatch. Remaining work is exact-pin evidence, not speculative feature work.

## Requirement checklist and proof owner

| Requirement | Current source/evidence assessment | Exact final proof |
|---|---|---|
| All 13 gRPC RPCs, 10 legacy Thrift procedures | All implemented and source-traced in `matching-final-rpc-acceptance-matrix-20261025.md`. Focused unit/oracle tests cover the listed branches. Three RPCs (`UpdateTaskListPartitionConfig`, `RefreshTaskListPartitionConfig`, `AcquireSemaphore`) are gRPC-only in Go as well. Optional-field/error-precedence limits are test-scope limitations, with no sourced caller-visible mismatch identified. | Fresh combined-pin `rust-ci.yml` from `c25`: Clippy then workspace tests. Mixed SDK lanes below establish service interoperability, not every RPC error branch. No extra direct mixed call per unit branch is required absent a concrete mismatch.
| Persistence backends | MySQL, PostgreSQL, Cassandra and configured dynamic-config store have distinct production lanes. Older green artifacts are bound to older SHAs. | Parent’s serial exact-pin gates: `matching-production-validation.yml` lanes `postgres`, `mysql-probes`, `configstore`; `matching-cassandra-mixed-gate.yml` lane. Inspect gate/result logs and binary SHA.
| Transports | gRPC is the normal mixed path; Thrift Matching plus TChannel History is a separate required compatibility path. Production gRPC TLS has a dedicated mixed lane. `matching-http-tls-interop` is a History fixture, not Matching acceptance. | Exact-pin `matching-production-validation.yml` `thrift` lane; `matching-production-tls-mixed.yml` for production CA/hostname-verified gRPC TLS. HTTP/TLS fixture is supplemental only.
| Configuration, discovery, lifecycle | Source traces cover config readers/defaults, dynamic-config store, Ringpop/shard ownership and service lifecycle. No concrete missing deployment field has been identified; the ignored `NewConfig` aggregate test is a test-shape limitation. | `configstore`, `shard-distributor`, and production TLS lanes; exact-pin workspace CI for unit/source-level regressions. Confirm artifacts show Go Frontend/History/Worker, Rust Matching, and no Go Matching.
| SDK/service-to-service compatibility | Existing mixed probes exercise Go SDK workflow/activity/query paths and Go History task dispatch. Prior evidence is scenario-specific and older-pin. | Each final mixed production lane’s SDK probe; inspect completion assertions and topology. Adaptive lane additionally checks all 24 SDK load workflows and activity completion.
| Restart, graceful drain, crash recovery, ownership | These are required runtime properties; a successful build or simple RPC probe does not prove them. | Exact-pin Postgres, MySQL, Thrift, configstore, shard-distributor, two-node MySQL, Cassandra, active-offer, and production TLS lanes. Verify emitted restart/recovery scenario results, not only requested flags. Include binary SHA for every lane.
| SQL malformed RunID fatal exit and concurrent delayed append shutdown | Actual writer append plus runtime shutdown regression exists. Focused clean-target CI `40371677686` passed on `10c7e8995aefe717b3d4388a405b07fee3a5b7bd`, including `runtime_shutdown_waits_for_real_delayed_append_and_fatal_notification` and the process exit-2 regression. That source predates this final pin. | Fresh combined-pin workspace CI and exact-pin mixed lanes (especially failure/recovery). The focused run is supporting evidence only; do not claim it validates current pin.
| Adaptive scaler deployment wiring and routing | Current probe uses live persisted scaler config/version growth, Go-compatible static `ListTaskListPartitions`, SDK processing of 24 workflows, then direct polls of each currently reported live read queue and verifies returned workflow/run IDs and completed results. This is stronger than queue completion alone. Probe expressly cannot prove the physical internal partition after forwarding/owner retry. | Exact-pin `matching-production-validation.yml -f lane=adaptive` in the parent’s serial gate sequence. Run `40373145351` is on old `eecb6bef8...`; it is not final-pin proof. Do not change production listing semantics to use persisted adaptive config: Go lists its filtered static write-partition count.
| Scaler stop/unload behavior | Scaler algorithm/lifecycle and manual-update guard have unit coverage; the current adaptive workflow probe does not explicitly establish shutdown on unload/drain. No shutdown defect is demonstrated by source review. | Existing restart/drain lifecycle lanes provide service lifecycle evidence. Treat a separate scaler unload assertion as optional test enhancement unless a source-backed mismatch or explicit required behavior is found; do not block acceptance speculatively.

## Final exact-pin gate set

Worker `c25` owns the fresh, uncached CI run. Parent owns serial mixed validation; this note starts or polls nothing. On one immutable `EXPECTED_SHA`, retain run URL, head SHA, conclusion, scenario/result files, each production Matching binary SHA256, and verify Go Matching is absent:

1. `rust-ci.yml` with `clean_target=true` (fresh runner target; Clippy then workspace tests).
2. `matching-production-validation.yml`: `postgres`, `mysql-probes`, `thrift`, `configstore`, `shard-distributor`.
3. `matching-mixed-gate.yml` (two-node MySQL).
4. `matching-cassandra-mixed-gate.yml` (confirm actual recovery scenario in artifact).
5. `matching-production-validation.yml`: `adaptive` (corrected probe, exact pin).
6. `matching-active-offer-gate.yml` (active offer, task insertion, restart/drain and crash/owner recovery).
7. `matching-production-tls-mixed.yml` (production TLS mixed topology).

`matching-http-tls-interop.yml` may be retained as supplemental transport fixture evidence; it does not replace a gate above. Preserve current eligibility labels in generated artifacts. Do not infer physical serving-partition identity from requested queue or addressed endpoint.

## Acceptance decision

- **Production mismatch found in this reconciliation:** none beyond no newly sourced discrepancy; the previously identified fatal SQL lifecycle issue has an implementation and focused passing regression, pending exact-pin validation.
- **Explicit requirement still awaiting proof:** all above exact-pin fresh/mixed lanes, including the corrected adaptive production probe.
- **Optional coverage only:** exhaustive pairing of every RPC validation/error-precedence branch with a Go oracle; direct physical internal-partition identity instrumentation; dedicated scaler unload probe. Add these only if source evidence identifies an observable mismatch or a required contract currently unproved by the listed lifecycle lanes.
- **Next service:** do not advance to History until final artifacts are reviewed and this checklist is closed.

## Later contract recheck — acceptance still open

The older unresolved panic-string note exposed a concrete current wire-text gap:
Go `%#v` renders NUL as `\x00`, while Rust debug string formatting renders
`\0`. A Luna worker owns a Go-compatible quoting implementation and focused
Actions validation. Do not interpret the earlier no-new-mismatch snapshot as
closing this subsequently confirmed issue. Cassandra UUID-spelling effects are
also under independent review: a metrics label discrepancy is not automatically
resolved merely because it does not alter task delivery.

The corrected adaptive fixture candidate is immutable tag `t3/final-acceptance-v2`
at `eba0b437df9096d44b82be91af542d164c5c5123`. Fresh workspace run
40375453720 and adaptive mixed run 40375459588 target that source. Any subsequent
production contract fix requires a new reviewed pin and appropriately scoped
validation; these runs cannot prove changes they do not contain.

Independent Cassandra review subsequently confirmed a metric and quota-context label gap for gocql-valid UUID spellings. Backend-specific cache-key normalization is required; the prior telemetry-only closure is superseded. A Luna worker owns the fix, preserving SQL UUID panic behavior and strict real validation paths.
