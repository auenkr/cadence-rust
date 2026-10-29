# Matching native active audit test evidence — 2026-10-29

Reviewed the locally retained full Rust test log for Actions run
[40807471166](https://github.com/auenkr/cadence-rust/actions/runs/40807471166),
under frozen source candidate `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1`.
The exact log is
`/mnt/storage/service/ci-evidence/matching-native-workspace-40807471166/downloaded/rust-ci-logs/cadence-rust/cadence-rust/rust/test.log`.
Compact exact test lines, including source-log line numbers, are retained at
`memory/evidence/matching-native-workspace-40807471166/native-audit-test-lines.log`.

## Named assertions observed passing

The test log contains `... ok` lines for all of these requested native regressions:

- Declared Thrift exceptions, including raw field and wire ID preservation (`every_declared_matching_exception_preserves_raw_fields_and_wire_id`, line 1454), and the pinned all-method InternalServiceError wrapper matrix (line 1456).
- Actual served raw status over the transport test (`served_raw_status_normalizes_message_details_and_marker`, line 1716).
- Production failover callback attachment after catchup (line 1123).
- Public refresh RPC storage failure without cache/owner replacement (line 1276).
- Query forwarding failure propagation and permit release (line 1278).
- Typed handler metric classification across the fourteen known error categories (line 2194). This is the classification assertion; it is not exhaustive typed payload testing across every RPC.
- Raw source/cache and raw UUID behavior, including backend/cache-key lookup (781), accepted non-UTF8 SQL wrapper/source behavior (961), Go parser and quoting cases (776), Cassandra parser boundaries (772), raw forwarding fields/domains (799), and synchronous forwarding with cached raw source (803).
- Related activity source-cache/standby/persistence assertions (1265, 1275), plus rate-limit/panic metric ordering and request visibility (960, 962).

The logged test binary reports `972 passed; 0 failed; 2 ignored` (line 1500). This is evidence that this test binary completed successfully. It is not evidence that the complete workspace workflow passed; this retained log alone does not establish final workspace CI conclusion or mixed-cluster acceptance.

## Explicitly missing named evidence

The configured semaphore assembly note calls out the independent 30-second manager startup timeout as unasserted. No matching passing test in this log establishes that exact timeout. The passing `matching_semaphore_acquire_honors_deadline_while_starting` (line 1212) exercises a caller deadline, while `slow_start_keeps_running_after_caller_cancel_and_reuses_manager` (line 1264) exercises slow startup/cancellation/reuse. Neither proves the separate fixed startup timeout. Failed-start cleanup/rebuild is covered at line 1258.

## Acceptance boundary

This is exact test-execution evidence for the lines listed above, not a broader parity or service-completion claim. The source reconciliation notes still require final same-pin workspace conclusion and mixed Go/Rust SDK, service interaction, failure, restart, recovery, topology and backend evidence. The ten mixed lanes remain pending in the supplied reconciliation notes. No mixed acceptance, final Matching parity audit, or service completion is inferred from this unit/workspace test log.

Parent verification: all numbered retained lines were checked for exact equality against the downloaded run40807471166 test.log. The workflow conclusion and fresh-target Clippy/test exits are independently retained in workspace-result.txt and run.txt. This does not add mixed-lane credit.
