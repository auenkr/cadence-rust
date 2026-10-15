# Current resume checkpoint (2026-10-15)

Follow root AGENTS.md. The user explicitly requests continuing until the whole
Go implementation is migrated, with drop-in compatibility and compatibility
tests. The active goal is incomplete. Do not equate RPC adapter coverage with
behavioral compatibility. Keep working autonomously; use tmux cadence-rust for
long processes. Save progress, commit, and push after compaction.

## Latest verified active-active registration/failover checkpoint

Active-active helpers now initialize registration versions from original cluster
offsets, validate incoming update versions, compute server-side versions and
merge only strictly newer attributes while preserving untouched state. Domain
registration and FailoverDomain use them across transports. Explicit same-cluster
domain targets now match Go: AP version stays equal; AA domain fencing advances.
Destination errors follow scope/attribute ordering. Audit attribute comparison
uses version changes. Proto/Thrift mappers ignore deprecated region maps; Thrift
zero-valued attribute versions are explicitly emitted like Go.

62 shared fixtures call the actual Go helpers via overlay; a separate Go mapper/
codec fixture checks exact zero-version Thrift bytes. Full runner PASSED all 14
stages, 290 workspace tests and strict clippy, standalone/config checks, public/
internal gRPC and existing Go SDK TChannel. Evidence:
/tmp/cadence-active-active-compat-v2/results.json and outer exit file 0. User
development server unchanged; test servers cleaned up. Ledger: 15 partial and
2285 review-required Go files, none marked complete.

Next: read active-active-next.md and port UpdateDomain's configuration/failover
dispatch, field presence, version updates and atomic validation. It STILL replaces
attribute maps wholesale. Also pending: graceful failover (including AA rejection),
primary restrictions, dynamic migration override evaluation, workflow attribute
routing, passive ownership and replication. Empty-map wire presence needs broader
codec work. This checkpoint is not a complete domain service or replication port.

All 164 RPCs in all 13 services have concrete Rust adapters. Significant
single-host/no-op behavior remains behind that surface, especially replication,
queue/shard operations and failover. Read migration-inventory.md for the major
unported capabilities; its adapter counts are not completion evidence.

## Latest work

Scheduler behavior has been checked against actual Go scheduler functions via
Go overlays (no copied Go implementation and no source-tree changes). Commits
pushed this session:

- 30576fd08: inclusive start/end bounds, delayed ticks, bounded backfill.
- d3580c5a6: catch-up watermarks, skipped counters, batch overrides.
- 5e2b9d8f0: catch-up on snapshot startup recovery.
- 817dc47e8: paused/large backfill queues, pending-only descriptions.
- 9a490be26: exact Go UUID namespaces, buffered origins, system limits.
- 693964d8e: cooperative cancellation and durable latest-target tracking.
- 7c08f363b: monotonic fire accounting and full isolated compatibility runner.
- 3a2431677: embedded development defaults and copied-binary verification.
- 0cdb0bc60: Go history-version algorithms with 68 differential cases.
- Latest execution version checkpoint: durable version ranges/branch tokens,
  -24 local versions, reset ancestry, snapshot upgrades, mutable-state metadata;
  Go differential suite expanded to 82 cases including codec/SQL fork behavior.

The latest checkpoint integrates version histories into stored executions,
including buffered flushes and transient tasks, child/continue-as-new/reset,
and snapshot recovery. New local history versions are -24; legacy zero-version
Rust snapshots retain their original stream. GetMutableState and PollMutableState
now expose durable branch tokens/version histories. Branch tokens use Go's
0x59 + binary Thrift codec, with byte-exact and SQL-fork differential checks.
ReplicateEventsV2 still accepts without applying events; do not claim replication.

Commands:

- python3 tests/run-scheduler-compat.py: shared Go/Rust boundary, catch-up,
  backfill, identity/limits, overlap and accounting fixtures.
- python3 tests/run-compatibility.py: workspace tests + strict clippy,
  differential fixtures, server build, isolated server on free ports,
  public/internal gRPC checks and existing Go SDK over TChannel. It prints
  artifact paths, writes results.json, and stops its own server afterward.
- Latest complete runner passed in tmux cadence-rust:version-verified-run;
  artifacts /tmp/cadence-version-state-compat/results.json, outer log
  /tmp/cadence-version-state-compat-run.log, exit file
  /tmp/cadence-version-state-compat.exit (0). Its isolated server was cleaned up.
  It copied the executable to standalone/cadence-server and booted it from
  that fresh working directory, using embedded defaults and no Go checkout.

Latest full checkpoint: 284 workspace tests + strict workspace/all-targets
clippy passed. The full runner passed scheduler differential fixtures and all
three wire/SDK drivers against the latest binary. Coverage includes 21 behavioral
scheduler scenarios + 4 UUID cases + 7 limit cases, and 82 history-version/branch
cases against real Go algorithms, codec and mocked SQL history-store operations.

Standalone startup is independent of go/config/development.yaml; explicit
--config still works. Earlier raw-history V2 work added requested version-item
selection, exclusive bounds and structured cursors. Latest work replaces raw
JSON batches with Go version-0 ThriftRW history batches, sets the actual gRPC
encoding, and normalizes omitted bounds before validating pagination cursors.
A new Go serializer oracle provides byte fixtures and decodes actual network
responses. Full validation PASSED in tmux cadence-rust:blob-full-check: all ten stages,
including workspace tests, strict clippy, three Go differential suites, copied
standalone binary/config, public/internal gRPC and Go SDK TChannel. Evidence:
/tmp/cadence-history-blob-compat-v2/results.json and outer exit file (0).
The internal client decoded the network blob with Go persistence and checked
omitted-bound continuation plus changed-bound rejection. See replication-next.md
for remaining raw-history storage/precision and remote-application limitations.

ReplicateEventsV2 remains a validated no-op. Internal guard removal in prior
commits only routes metadata to the local engine; forwarded requests and
external-child/failover semantics are not established. Next major work is real
passive history/state application with branch conflict handling and failover
version gating, plus production backends and independent service lifecycles.

## Latest verified cluster-version checkpoint

Common cluster version arithmetic/ownership and migration-offset fallback now
match 69 original-Go differential fixtures. Configured increment and cluster
offsets feed frontend global registration/failover (including Thrift), replacing
placeholder +1 versions. Compatibility clients now require configured remote offset 2,
cluster0 offset 0 and increment 10; full runner supplies a standalone YAML and
separately boots embedded defaults before that. It checks 2 -> 10 -> 12 -> 20
failovers and history version 20 on a new active-cluster execution.

Latest full runner PASSED all 12 stages in tmux cadence-rust:cluster-full-check;
/tmp/cadence-cluster-metadata-compat-v4/results.json and outer exit file 0.
286 workspace tests, strict workspace/all-targets clippy, four Go differential
suites, copied-binary embedded-default startup and configured-server public/
internal gRPC plus Go SDK TChannel all passed. Test servers were cleaned up;
user development server was not restarted. The older web IDL cannot encode the
new destination-check override, so reverse-failover checks use original Go IDL.
Port ledger has 10 partial and 2290 review-required files; no source file is
marked fully ported.

Remaining: Go config validation/default layering, dynamic migration override
lookup, active-active attribute merge/version updates, in-flight execution
promotion, passive mutation gating, queues and replication. New-domain version
mapping alone is not production failover. This is a version-metadata checkpoint, not complete failover.

## Latest verified cluster configuration checkpoint

Go cluster-group defaults and validation now live in common::cluster::GroupMetadata.
The config loader retains canonical/deprecated keys separately and applies Go
fallback precedence before validating. Missing or invalid supplied metadata is
rejected; main no longer silently substitutes default cluster0 metadata. No-file
startup still uses embedded valid development defaults. 34 shared fixtures match
actual Go FillDefaults/Validate, including legacy-map defaults and exact errors.

Full runner PASSED all 13 stages: 288 workspace tests, strict clippy, Go
differential suites, copied-binary embedded startup and 33 CLI config checks
(including invalid startup), public/internal gRPC and existing Go SDK TChannel.
Evidence: /tmp/cadence-cluster-config-compat/results.json and outer exit file 0.
User development server was not restarted. Source ledger has 11 partial and
2289 review-required files; none marked complete.

The later active-active checkpoint above ports registration/failover attribute
initialization and merging. UpdateDomain remains the next domain-handler gap;
read active-active-next.md for exact source anchors and validation boundaries.
Full config layering, cluster TLS/auth/redirection and persistence/auth validation
remain separate gaps. Do not claim all Go configuration or failover is ported.

## Runtime

User development server is cadence-rust:rust, HTTP 8080 / gRPC 7833 / history
7834 / matching 7835 / TChannel 7933, data-dir .cadence-data, scheduler and
semaphore enabled. It has NOT been restarted with this session's changes.
The earlier isolated migration-server on ports 18080/17833-17835/17933 was
stopped after its checks. The full runner uses fresh free ports and cleans up.
The user prefers CLI; leave the web UI stopped.

## Remaining gaps

Production SQL/NoSQL persistence and Go schema compatibility, transactions and
crash durability; history version branches and real replication application;
distributed ownership/failover, independent services, worker/background roles;
archival, advanced visibility, dynamic config/quotas/auth/telemetry; exhaustive
workflow/history/failure/recovery and Go/Java SDK replay coverage. JSON snapshots
are development storage, not production durability.

Scheduler gaps still include Go scheduler-workflow histories/visibility,
shared activity-budget semantics and transient failure/retry paths. Jitter and
PauseOnFailure exist in API types but are not acted on in the pinned Go scheduler;
do not invent behavior for them. Legacy numeric Rust buffers lacked trigger
origins; they load with schedule-source fallback because lost data is unrecoverable.
Unqualified/Local cron zones still assume UTC. Sparse zoned cron scans minutes
up to the five-year horizon.

## Stable lessons

- SDKs use native TChannel, so HTTP/Thrift alone is insufficient.
- Thrift timestamps use nanoseconds; stored history uses milliseconds;
  protobuf timestamps and enum offsets require explicit conversion.
- Domain identity must use UUIDs, including delete/recreate isolation.
- History cursors bind run and snapshot end; visibility cursors need stable ordering.
- gRPC message-size checks must account for inner JSON/base64 expansion.
- Existing development server binaries lag source until rebuilt/restarted.
- A passing limited suite never establishes complete production replacement.
