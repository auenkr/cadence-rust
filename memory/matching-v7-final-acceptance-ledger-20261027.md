# Matching v7 acceptance ledger

Matching remains incomplete. This ledger supersedes the current-status claims
in the v5/v6 ledgers; their results remain historical evidence.

Final ref: `matching-acceptance-20261027-v7`.
Final SHA: `f5963951befb12dc64b72bb366c44a464939b87a`.
User now explicitly requires raw-byte Thrift string parity. Source work is
reopened; see matching-raw-byte-string-progress-20261027.md. V7 is baseline
evidence, and remaining v7 dispatches are held pending a corrected candidate.
Never credit an old-pin result to this candidate. Any source correction after
the pin requires a new immutable candidate and same-pin gate reruns.

## Combined source prerequisite

Cached package CI [40534430151](https://github.com/auenkr/cadence-rust/actions/runs/40534430151)
passed at `8ed5ff127ae36a47c33f3b31577444f94caf7a64`. It includes 756 unit
tests and all package integration suites, including eight concurrency/fencing
tests, actual concurrent decision/activity isolation delivery and active offer.
Compact proof: memory/evidence/matching-focused-40534430151/.

Later source 94276d20f adds the refresh persistence-error/retry cancellation fix,
zero-rate recovery assertion and actual registered domain-failover callback
test. Diagnostic CI40535623193 failed missing test imports before execution.
97eb8560e corrects imports, loads the fixture manager through the public
registry API, and applies runner formatting. Replacement diagnostic CI:
[40536574918](https://github.com/auenkr/cadence-rust/actions/runs/40536574918),
head `97eb8560e` (pending). Formatting enabled; apply its patch before the final
pin. Superseded unformatted diagnostic40536460336 is not acceptance evidence.

Current diagnostic status (supersedes the pending status above):
- 40536574918 failed compiling the test's equality assertion; aab625202
  compares the stored partition configuration fields individually.
- [40537474183](https://github.com/auenkr/cadence-rust/actions/runs/40537474183)
  at aab625202 passed Clippy, then failed the reader zero-dispatch recovery
  regression (757 passed, 1 failed, 2 ignored in the lib suite). Polling
  delivers an allocated task but does not complete it; the assertion wrongly
  expected the ack level to advance before completion.
- ed63d5f47 explicitly completes the delivered task, first checking that
  delivery alone preserves the unacknowledged state. Replacement focused CI
  [40538520049](https://github.com/auenkr/cadence-rust/actions/runs/40538520049)
  at `ed63d5f47287daf5126b5cd8e01deafc224f0076` is running with runner formatting
  enabled. This is diagnostic package validation, not fresh workspace evidence.
- c16d25e01 adds public certificate fingerprints and a verified live TLS
  handshake record; final runtime validation must inspect that record.

Focused CI40538520049 **passed** at ed63d5f47 (Clippy exit0, test exit0).
Compact proof: `memory/evidence/matching-focused-40538520049/`. Its runner
formatting patch is applied in f5963951b. Candidate v7 also includes b380cf224
(TLS sanitized evidence bundle and collector retention); that Python-only
collector change passed a synthetic retention/exclusion fixture. The package
pass is a prerequisite, not same-pin workspace acceptance.

Fresh uncached workspace CI is now dispatched at the v7 tag:
[40539581017](https://github.com/auenkr/cadence-rust/actions/runs/40539581017).
Run head and downloaded artifact commit match v7; Clippy/test exits are zero,
fresh target is /home/runner/work/_temp/cargo-target-final. All four live
metric comparisons match. Compact evidence:
`memory/evidence/matching-v7-workspace-40539581017/`. PostgreSQL is dispatched;
all subsequent mixed lanes remain undispatched. Use the explicit two-node Prometheus/cold-domain arguments in
`matching-v7-two-node-dispatch-review-20261027.md` to preserve the v5 checks.

## Required runtime evidence

Dispatch commands and artifact requirements are in
matching-acceptance-evidence-readiness-20261027.md. Run mixed lanes serially
after fresh workspace validation. For each require run head and artifact SHA
equal FINAL_SHA, Rust binary SHA256, Go Frontend/History/Worker with Rust
Matching and no Go Matching, actual SDK/probe/restart/recovery outcomes,
matching exit files and cleanup proof. Job conclusion alone is insufficient.

| Requirement | Run | Reviewed result |
| --- | --- | --- |
| Fresh uncached workspace Clippy followed by tests | [40539581017](https://github.com/auenkr/cadence-rust/actions/runs/40539581017), v7 tag | PASS: reviewed fresh target, exact commit, Clippy0/test0; 52 suite summaries retained |
| PostgreSQL | [40541390213](https://github.com/auenkr/cadence-rust/actions/runs/40541390213), v7 tag | PASS: reviewed exact source/binary identity, SDK, GC/domain, standby query, partitions, durable restart/drain, crash/idle recovery and container removal; compact evidence matching-v7-postgres-40541390213 |
| MySQL probes | [40542820380](https://github.com/auenkr/cadence-rust/actions/runs/40542820380), v7 tag | PASS: reviewed exact source/binary, Go schema (no missing/extra tables), GC/domain, SDK, forwarding/query/partitions, restart/drain/crash/idle and cleanup; evidence matching-v7-mysql-probes-40542820380 |
| Thrift | [40543897866](https://github.com/auenkr/cadence-rust/actions/runs/40543897866), v7 tag | PASS: reviewed actual Thrift/TChannel topology, SDK/query/isolation/config hints, restart/drain/crash/idle, exits and cleanup; evidence matching-v7-thrift-40543897866; malformed UTF-8 limitation remains open |
| Configstore | [40544961371](https://github.com/auenkr/cadence-rust/actions/runs/40544961371), v7 tag | PASS: reviewed seeded2000ms/observed2009ms poll, SDK/restart/drain/crash/idle and cleanup; evidence matching-v7-configstore-40544961371 |
| Shard distributor | Not dispatched | Pending |
| Two-node MySQL | Not dispatched | Pending |
| Cassandra | Not dispatched | Pending |
| Adaptive two-node | Not dispatched | Pending |
| Active offer | Not dispatched | Pending |
| Production TLS | Not dispatched | Pending |
| Supplemental Go History peer and PostgreSQL TLS oracles | Not dispatched | Pending |

## Source audit closure record

- Unresolved malformed-Thrift boundary: the decoder review records that Rust
  rejects invalid UTF-8 in string fields while Go's FromWire accepts the raw
  bytes. See matching-thrift-decoder-review-20261027.md, "Invalid UTF-8
  limitation", and matching-thrift-full-decode-parity-20261027.md. No user
  acceptance of this divergence is recorded. Do not treat successful valid
  string SDK/mixed gates as proof of parity for those inputs or silently mark
  the source audit fully closed. Lossy conversion would alias identifiers and
  is not a parity fix; downstream Go behavior and a lossless approach still
  need resolution before an unrestricted parity-completion claim.
- RPC inventory: matching-v6-rpc-audit-20261027.md closes method surface.
  U1/U4 source fixes and review corrections are recorded in
  matching-thrift-full-decode-parity-20261027.md and later combined checkpoints;
  the passing focused run includes their normalization/nil/quota regressions.
- Reachable query errors, typed wire categories and duplicate map keys:
  matching-final-query-wire-review-20261027.md. No confirmed mismatch in scope.
  Arbitrary mock errors are not additional reachable production categories.
- Configuration: matching-current-config-reconciliation-20261027.md, with
  fixed range100000/GC1s/hostname/RPC wiring resolved by
  matching-fixed-config-gc-final-review-20261027.md.
- Lifecycle/persistence: the earlier audit and its deviation-resolution notes,
  transient writer retry fix, reader host-fatal wiring and regression review.
  Actual restart/crash/idle-recovery gates remain mandatory.
- Ignored tests: matching-current-ignore-reconciliation-20261027.md corrects
  stale mock-seam claims. The limiter table has all12 Go rows in active Rust
  tests. Callback integration and refresh error propagation now have new
  regressions awaiting combined CI. Dev-only nondurable Describe remains
  outside the configured Go runtime; see matching-ignored-rpc-audit-20261025.md.
- Refresh error/context parity: matching-refresh-retry-error-fix-20261027.md
  and matching-refresh-final-review-20261027.md. Tests await the latest CI.
- Zero-rate durable recovery: matching-zero-dispatch-recovery-20261027.md.
  Reader-boundary coverage complements public concurrent delivery tests; do
  not claim the exact concurrent zero-RPS Go test was duplicated.
- Dependency ignored tests have a dispatch-only dedicated workflow; neither is
  credited until its exact-pin artifacts are reviewed.

These are bounded source/test reviews. Service acceptance still requires the
full same-pin workspace and runtime evidence above; no service completion is
claimed by these notes.
