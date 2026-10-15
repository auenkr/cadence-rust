# Current resume checkpoint (2026-10-15)

Latest follow-up: named TZ=/CRON_TZ= cron support now uses bundled IANA rules
(chrono-tz), including DST folds/gaps and historical second offsets. See the
last progress.md entry and tests/cron-timezone-compat.go. Workspace 263 tests,
strict clippy, final common-crate tests and six Go oracle cases pass. Running
server was not restarted. Local/unqualified schedules still assume UTC;
sparse zoned schedules scan minutes up to the five-year horizon.

Follow root AGENTS.md. All 164 RPCs across all 13 services now have concrete
Rust adapters (commit 9ba9938ec, pushed to origin/master). Adapter presence
does not prove complete engine behavior; many encode honest single-host
semantics. Read `migration-inventory.md` (107/107 adapters) and `progress.md`
(tail entries: H2a, H2b, ShardDistributor) for the latest state.

Runtime: native Rust server in tmux `cadence-rust:rust`
(`CADENCE_ENABLE_SCHEDULER=1 CADENCE_ENABLE_SEMAPHORE=1
rust/target/debug/cadence-server start --port 8080 --data-dir
.cadence-data`; HTTP 8080, gRPC 7833 incl. Admin + ShardDistributor,
TChannel 7933, internal history 7834, matching 7835). Web UI window was
stopped earlier per user direction; CLI at `/home/ubuntu/.local/bin/cadence`
remains available. Source changes require rebuilding/restarting before live
checks; compat drivers: `node tests/internal-grpc-compat.cjs`,
`node tests/grpc-compat.cjs`, `cd tests/go-sdk && go run .`.

Latest validation: 263 workspace tests pass, strict workspace clippy clean,
all three compat drivers exit 0 against the deployed binary.

Next engine-depth gaps (no adapter work remains): durable SQL/NoSQL
persistence (JSON snapshots are dev-only), version branches/history
versioning, replication event application (ReplicateEventsV2 is a validated
no-op), distributed ownership/failover and service independence, worker
background roles (scanners, archival, batch), archival backends, advanced
visibility query language, dynamic config/quotas/auth. Upstream
DrainShards/UndrainShards/GetDrainedShards are unimplemented upstream too
(INTERNAL, mirrored exactly).

User now requests Cadence CLI instead of web UI. Web window was stopped; the
cadence-web submodule remains available. Native Rust server runs in tmux
`cadence-rust:rust` (HTTP 8080, gRPC 7833, TChannel 7933, internal history 7834,
matching 7835). Source changes require rebuilding/restarting before live checks.

Current binary: sticky-full (195 workspace tests/strict clippy/build passed).
Existing SDK passed its default sticky worker (activity + idle signal), absent
sticky owner timeout fallback and ResetStickyTaskList, alongside all earlier
transient failure/timeout, inline task and workflow/cron cases.
`/tmp/cadence-sticky-sdk-live.exit`0, domain
`rust-tchannel-sdk-df8706c7-aafc-4891-91a1-ac16d813869a`.

Internal original-IDL sticky/reset check running in internal-sticky-ready:
`/tmp/cadence-internal-sticky-ready.log` and `.exit`. Startup readiness was added
after an initial ECONNREFUSED before listeners bound. Public reset and internal
history reset are concrete adapters now; inventory36/57 public +18/107 internal.

Next areas: real SDK query compatibility (existing legacy query emulation still
requires review), local activity/local dispatch, remaining task policies/reset,
then broader remaining service/persistence/replication scope. Sticky runtime
supports routing/fallback/reset, but not dynamic per-domain TTL or full client
metadata/distributed matching ownership. Read service-map and inventory.

Cadence CLI installed at `/home/ubuntu/.local/bin/cadence` and available on PATH.
Feature version 1.7.0, built from checked-out `go/cmd/tools/cli`. Go CLI is a client;
no Go Cadence server is running. `tests/cli-compat.sh` passed retry timeout history
and visibility across both transports. CLI errors may exit zero, inspect output.
Use `--address 127.0.0.1:7833 --transport grpc` or port 7933/tchannel.
Tmux window `cadence-rust:cli` exports address/transport/domain so plain
`cadence domain describe` works. Domain `rust-cli-compat` retained for inspection. `domain list` is not a
command in this CLI; domain describe and workflow list work.

Read progress.md, migration-inventory.md, service-map.md and port-ledger-summary.md.
Public adapters 36/57; internal/admin 18/107, all 13 proto services/164 RPCs defined.
Definitions and adapter counts do not prove behavior. Production persistence,
remaining workflow-task/sticky/replay behavior, retry/cron edge cases,
distributed services, XDC/admin and other features remain unfinished.

Correction: Go server rejects retry backoff coefficient below 1; SDK default2
is sent explicitly. Earlier notes claiming a server default2 are inaccurate.
Non-UTC cron TZ is still unsupported. JSON snapshots are development storage.

## Earlier checkpoint (historical; superseded above)

# Resume the Cadence Rust rewrite

Latest checkpoint: 2026-10-14. Follow root `AGENTS.md`. The objective is complete
Go behavior in Rust, preserving existing SDK APIs and histories. Migration is
incomplete. Read `migration-inventory.md` for scope, `progress.md` for chronology,
and `../tests/README.md` for compatibility driver commands.

## Runtime

Use tmux session `cadence-rust` for every long process. Existing windows:

- `rust`: `rust/target/debug/cadence-server start --port 8080 --data-dir .cadence-data`
  launched from the repository root. JSON and YARPC Thrift HTTP: 8080; native
  public gRPC: 7833; native TChannel Thrift: 7933.
- `web`: source submodule `cadence-web`, running
  `NEXT_TELEMETRY_DISABLED=1 CADENCE_WEB_PORT=8088 CADENCE_GRPC_PEERS=127.0.0.1:7833 npm run dev`.

Both `/health` on 8080 and `/api/clusters/cluster0/domains?pageSize=100` on 8088
returned HTTP 200 at this checkpoint. The web API sees Rust domain/workflow data.
The running binary includes the validated activity retry changes below.
Development persistence is JSON snapshots, not production Go storage parity.

Root cadence-web submodule revision: `be95f6ff2543c1106187cee81bd1913fb8ebdbb9`.
Public Rust bindings use `go/idls` revision
`bfbf273b41b9e36ce21e47a49600d22d73e4614c`; the web client uses its own upstream
IDL pin `3ee08a98cf704ed8b2cc2101abe71ab80eccf542`. Node 24/npm 11 and generated
web IDL are installed. Source UI avoids the unavailable ARM64 Docker image.

## Verification evidence and limits

- Before latest retry edits: workspace tests passed 130 checks; strict workspace
  clippy passed. Narrow generated-code/tonic Status lint exemptions are explicit.
- `tests/grpc-compat.cjs` passed using the upstream web Node gRPC client: domain
  lifecycle/paging, rich already-started errors, workflow/activity lifecycle,
  heartbeat, typed history, visibility and paging. Completed history: 11 events.
- `tests/go-sdk` passed with existing Go SDK v1.4.0-rc.3 client AND worker over
  native TChannel. 1,650,000-byte payload round trip, concurrent calls during long
  polls, fragmented requests/replies, CRC32 and visibility/schema checks passed.
  Completed history: 14 events. No Go Cadence server was used. Sticky execution
  is explicitly disabled; sticky/replay/recovery parity is not established.
- After retry changes: all 131 workspace tests passed (42 persistence tests) and strict workspace
  clippy (`cargo clippy --workspace --all-targets -- -D warnings`) passed.
  Rebuilt the server and restarted tmux `rust`.
- The updated existing Go SDK driver now passed against the rebuilt Rust server,
  including an activity that fails on attempt 0 and completes on retry. It checks
  the retry history contains one Scheduled event and one final Started event.
  Full payload/TChannel/visibility checks passed as well. Upstream Node gRPC
  compatibility driver also passed against this build (11-event workflow).
- After correcting Go's default backoff coefficient, rebuilt/restarted once more
  and reran the SDK driver successfully. Retry history had 14 events. Latest SDK
  domain: `rust-tchannel-sdk-2c2b7f7d-e434-48dd-a241-082fecaf3169`.
- Useful local logs: `/tmp/cadence-rust-tests.log`, `/tmp/cadence-rust-clippy.log`,
  `/tmp/cadence-sdk-compat.log`, `/tmp/cadence-retry-check.log`. These may disappear;
  keep verified conclusions in project memory.

SDK sample visible in UI: domain `rust-tchannel-sdk-d30c34f1-4405-44f9-9ead-cffe68955d32`,
workflow `echo-d30c34f1-4405-44f9-9ead-cffe68955d32`,
run `d03a5060-850c-4206-b0e2-9bbb5e66d353`.

## First unfinished implementation: activity retries

Source: `rust/crates/cadence-persistence/src/executions.rs`.
Relevant helpers: `activity_retry_time`, `materialize_activity_started`,
`requeue_due_activities`, and `resolve_activity_mut`.

Go behavior references:

- `go/service/history/execution/mutable_state_builder_methods_activity.go`:
  retry keeps ScheduleID, increments attempt, records pending last failure, and
  uses transient StartedID -124 until the final attempt is materialized.
- `go/service/history/execution/retry.go`: backoff, expiration, maximum attempts,
  cancellation, fatal/non-retryable errors, jitter and override semantics.

Edits already made: retain schedule ID, defer retry-enabled starts, refuse retry
without policy, preserve heartbeat details, reject stale attempt tokens, apply
Go backoff/expiration rules and materialize final starts with original timestamp.
New pending-activity fields have serde defaults. A `rand` dependency was added.

Next actions:

1. Review the retry implementation against edge cases still uncovered by the
   tests: heartbeat restoration, cancellation, transient started timestamp fidelity.
2. Continue the remaining inventory. Buffered event IDs, activity/task/workflow
   timeouts, workflow retry/cron and sticky execution remain major engine gaps.

## Lessons to preserve

- Generated default gRPC stubs return UNIMPLEMENTED. Count concrete adapters,
  not generated methods. Current count is 35/57; schedules/semaphores unbound.
- The old Thrift table claimed generation but omitted methods. Its full 56-method
  table now comes from pinned IDL using `rust/tools/generate-thrift-methods.py`.
- Thrift timestamps are nanoseconds; internal stored timestamps are milliseconds.
  Protobuf timestamps and reserved-zero enum offsets require explicit conversion.
- Go clients use native TChannel by default. HTTP Thrift alone is insufficient.
  Empty argument boundary markers, cumulative fragmented CRCs, cancellation and
  request TTL matter. TChannel code is in `cadence-server/src/tchannel.rs`.
- UI domain filtering requires actual local cluster metadata. Dev config uses
  `clusterGroupMetadata` and current cluster `cluster0`; defaults and config alias
  handling were corrected. SDK examples stopped hardcoding invalid cluster `active`.
- History continuation must bind to run and snapshot end. Domain and visibility
  cursors must not skip entries when new rows are inserted before the cursor.
- In-process gRPC adapters reuse HTTP handlers; disable the inner JSON body limit
  because base64 expands bytes after tonic has already enforced its message limit.
- Service registry labels do not imply independent history/matching/worker services.
  JSON snapshots and basic visibility filters do not imply production persistence.
- Browser preview snapshot twice failed with “Preview tab ... was not found for
  snapshot.” HTTP UI data routes were checked; do not claim visual browser validation.
- All current code changes are uncommitted. Preserve them; staged submodule entries
  and unstaged/new source files are intentional work in progress.
