# Matching candidate v6 final acceptance ledger

Final ref: `matching-acceptance-20261027-v6`
Final SHA: `3055d0c6c0c8932c14e12714b498b82569cfe0b5`

Matching remains incomplete. V6 removes four redundant test struct updates that
failed v5 Clippy. All v5 results are historical and do not count for v6.
Keep this source pin immutable; any required source change needs a new candidate
and reruns. Subsequent master commits may contain documentation only.

| Requirement | Run / task | Reviewed result |
| --- | --- | --- |
| Fresh uncached workspace Clippy then tests | [40520934239](https://github.com/auenkr/cadence-rust/actions/runs/40520934239), head matches v6 | FAILED: Clippy exit 0, tests exit 101; cold-domain listing fixture does not advance its manual clock |
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
| RPC native/Thrift audit | memory/matching-v6-rpc-audit-20261027.md | Method inventory closed; four Thrift questions under deeper review |
| Ignored-test/exclusion audit | memory/matching-v6-ignored-tests-audit-20261027.md | Static report complete; two stale harnesses confirmed, other coverage claims need reconciliation |
| Lifecycle/persistence audit | memory/matching-v6-lifecycle-persistence-audit-20261027.md | Shutdown/drain/fencing reviewed; task reader/writer/GC review running |

Use memory/matching-acceptance-evidence-readiness-20261027.md for exact dispatch
commands and evidence requirements. Run mixed lanes serially after workspace
validation; retain exact source identity, binary SHA256, actual scenario/SDK
results, recovery outcomes, topology and cleanup. Do not infer acceptance from
job conclusion alone. No local Cargo/Go validation is authorized.

Automatic cached push CI 40520845748 was cancelled in favor of the explicit
fresh-target v6 workspace run. No mixed lane is running.

Worker orchestration: three Claude Sonnet 5.5 low-effort source auditors started
2026-10-27 approximately 14:25 UTC (19:55 IST), authorized for the next hour by
the user. Use Luna medium/default speed for later work after that window.
Prior v5 Luna RPC audit was interrupted and lifecycle audit failed; replacement
workers were given their thread pointers to recover partial findings. Parent
owns source changes and CI. Workers write separate memory reports only.

## Follow-up source work (14:30 UTC)

V6 is not the final acceptance candidate: two confirmed stale harness package
selectors require changes (`tests/matching-history-compat.cjs` and
`tests/run-shard-distributor-go-interop.sh`, cadence-server -> cadence-matching).
The current v6 CI remains useful diagnostic evidence but no mixed v6 lanes
will be dispatched. Cut v7 after the deeper workers finish and confirmed fixes
are reviewed, then repeat fresh workspace and all mixed lanes at v7.

Three Sonnet low-effort workers now own independent tasks:
- d1dd5841-ba59-44fd-b2b3-c32d926a4a8c: resolve Thrift U1-U4 against generated Go decoders and handlers; fix only confirmed mismatches.
- c1a099a7-2da7-41f9-b316-62978b1d553c: read-only task reader/writer/GC/completion source audit.
- ae49f94c-3d5a-4f71-b78c-0a097413a3e0: repair active-offer ignored test with reachable History fixture and reconcile Go/Rust test map.

These replace the three completed initial audits, whose scope was bounded;
none constitutes full service acceptance. The ignored-test report's claims of
missing harnesses or untested behavior require source reconciliation, not
blind promotion into new completion requirements.

## V6 final workspace result and additional fixes (14:38 UTC)

Run 40520934239 concluded failure at exact v6 SHA; downloaded rust-ci-logs under
`memory/evidence/matching-v6-workspace-40520934239/`. Artifact source SHA matches,
fresh target `/home/runner/work/_temp/cargo-target-final`, Clippy exit 0, Test
exit 101. Matching lib: 733 passed, one failed, two ignored. The failed test is
`name_based_listing_handlers_read_through_cold_domain_cache`: its manual clock
stayed at zero after the initial empty scan. Production cache correctly suppresses
another scan inside one second, matching Go domainCacheMinRefreshInterval.
Advance the fixture clock to one second, just like the adjacent metric test;
no runtime cache behavior change is needed.

Re-enabled the formerly ignored active-offer test with a reachable scripted
History peer in cdbc8cb07. CI on that intermediate SHA is diagnostic only.
The dependency oracle workflow adds direct ignored-test execution for generated
Go History TChannel calls and live PostgreSQL TLS; it is supplemental to the ten
mixed lanes. Parent review corrected IDL initialization, Docker password env,
certificate permission ordering, error-handling scope and cleanup result propagation.
It remains unexecuted until CI.

Deep source audit found B1: failed-delivery rewrite retries recognize only SQLite
and sticky-kind lease errors, missing typed backend transient failures. A worker
is repairing it with a backend-timeout regression. Separate workers resolve
Thrift nil/wrong-type wire decoding and the audit's crash-vs-unload deviations.
V7 is deferred until all confirmed fixes and tests are reviewed.
