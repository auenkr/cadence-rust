# Matching v7 acceptance ledger

Matching remains incomplete. This ledger supersedes the current-status claims
in the v5/v6 ledgers; their results remain historical evidence.

Final ref: **not cut**. Final SHA: **pending combined source validation**.
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

## Required runtime evidence

Dispatch commands and artifact requirements are in
matching-acceptance-evidence-readiness-20261027.md. Run mixed lanes serially
after fresh workspace validation. For each require run head and artifact SHA
equal FINAL_SHA, Rust binary SHA256, Go Frontend/History/Worker with Rust
Matching and no Go Matching, actual SDK/probe/restart/recovery outcomes,
matching exit files and cleanup proof. Job conclusion alone is insufficient.

| Requirement | Run | Reviewed result |
| --- | --- | --- |
| Fresh uncached workspace Clippy followed by tests | Not dispatched | Pending |
| PostgreSQL | Not dispatched | Pending |
| MySQL probes | Not dispatched | Pending |
| Thrift | Not dispatched | Pending |
| Configstore | Not dispatched | Pending |
| Shard distributor | Not dispatched | Pending |
| Two-node MySQL | Not dispatched | Pending |
| Cassandra | Not dispatched | Pending |
| Adaptive two-node | Not dispatched | Pending |
| Active offer | Not dispatched | Pending |
| Production TLS | Not dispatched | Pending |
| Supplemental Go History peer and PostgreSQL TLS oracles | Not dispatched | Pending |

## Source audit closure record

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
