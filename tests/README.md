# Compatibility checks

The server is Rust. Go and Node below are SDK/client compatibility drivers.
The cadence-web source submodule provides the unmodified upstream Node gRPC
client and pinned web IDL, so the gRPC check crosses language and client stacks.

Run the Rust server from the repository root, preferably in tmux session
`cadence-rust`:

```sh
cargo run --manifest-path rust/Cargo.toml -p cadence-server -- start --data-dir .cadence-data
```

It listens on HTTP JSON/YARPC Thrift port 8080, protobuf gRPC port 7833, and
native TChannel Thrift port 7933. The gRPC and TChannel ports and bind settings
come from the frontend RPC config; CLI flags can override the ports.

## gRPC wire and workflow compatibility

Prepare cadence-web once:

```sh
git submodule update --init cadence-web go/idls
cd cadence-web
npm ci
npm run install-idl
npm run generate:idl
```

From the repository root:

```sh
node tests/grpc-compat.cjs
```

This checks domain registration, description by name/UUID, paging and invalid
cursors; workflow start idempotency and typed protobuf error details; worker
polling, activity headers/payloads/heartbeat/completion; workflow completion,
visibility listing/counting, timestamps, and paged history.

## Existing Go SDK over native TChannel

```sh
cd tests/go-sdk
go run .
```

Set `CADENCE_TCHANNEL_PEER` to override `127.0.0.1:7933`. This uses the existing
Cadence SDK's worker and client, runs an activity/workflow with a 1.65 MB payload,
and verifies native request/response fragmentation and checksums, concurrent
long polls and ordinary calls, nanosecond Thrift timestamps, search attribute
schema, legacy/advanced visibility, and domain paging. It also runs a real SDK
workflow whose activity fails once and succeeds on retry, then checks the retry
history has one Scheduled event and only the final Started event. Sticky execution is
explicitly disabled because the full sticky task lifecycle is not ported yet.

## Ringpop JSON over TChannel boundary

```sh
python3 tests/run-ringpop-tchannel-compat.py
```

This starts a test-only Rust listener with an injected typed handler and calls it
using the pinned Go `tchannel-go/json.Client`. It checks join, ping, ping-req,
application errors, malformed JSON, and unknown method/service behavior. It
validates transport interop only; it does not implement production Ringpop or
SWIM membership.

The gRPC and SDK workflow drivers leave completed workflows and their domains
available in the web UI for inspection. They create unique IDs so repeated runs
are isolated.

Rust unit/integration checks also run with `cargo test --workspace` in `rust/`.
See `memory/migration-inventory.md` for unported capabilities; these checks are
not a claim that the entire Cadence server has been migrated.

## Cadence CLI against Rust (current preferred live check)

Build the upstream CLI from the checked-out Go API revision (no Go server):

```sh
mkdir -p "$HOME/.local/bin"
cd go
go build -o "$HOME/.local/bin/cadence" ./cmd/tools/cli
```

Then use the Rust endpoints:

```sh
~/.local/bin/cadence --address 127.0.0.1:7833 --transport grpc --domain rust-cli-compat domain describe
~/.local/bin/cadence --address 127.0.0.1:7933 --transport tchannel --domain rust-cli-compat workflow list
./tests/cli-compat.sh
```

The driver registers a unique local domain, starts a workflow with two attempts,
checks retry/timeout histories by exact run ID across both transports, and checks
visibility. `CADENCE_CLI_BIN` overrides the installed binary path. The CLI can
print errors with exit code zero; the driver also checks actual command output.
Use tmux session `cadence-rust` for installation and long checks. The web UI is
stopped at the user's request to use the CLI instead.

The SDK driver also exercises workflow failure retry (attempt 0 to 1), successful
cron recurrence with last completion result, and an activity ScheduleToStart
timeout on an unpolled task list. It terminates its cron workflow after checking
successor history. Full workflow-task buffering/sticky/recovery remain unfinished.

Cron time zone oracle (uses the pinned Go robfig parser; matching Rust cases
are in `cadence-common::cron::tests::named_time_zones_and_dst`):

```sh
(cd go && go run ../tests/cron-timezone-compat.go)
cargo test --manifest-path rust/Cargo.toml -p cadence-common named_time_zones_and_dst
```

Scheduler boundary differential checks use one fixture file for Rust and Go:

```sh
python3 tests/run-scheduler-compat.py
```

The runner tests the Rust evaluator, then overlays a test into the original Go
scheduler package to call its actual `computeNextRunTime` and
`computeMissedFireTimes` functions. It leaves the Go source tree unchanged and
requires no Go Cadence server. Fixtures cover inclusive schedule bounds, expired
windows, and DST. Rust engine tests also cover delayed ticks and backfill bounds.

The same runner also compares catch-up counters, watermarks, and per-unpause
policy overrides against Go's `processMissedRunsAt`, including continuation
across activity budgets and the 1000-fire scan cap. Rust recovery tests save
and reload a snapshot between catch-up batches.

Backfill fixtures also run through Go's actual `handleBackfill` and
`processBackfills`: paused queues, pending-ID deduplication, range rejection,
queue limits, and progress across batches. Rust saves/reloads each paused queue
before draining it. Completed work disappears from `ongoingBackfills`.

Scheduler identity fixtures compare deterministic request UUIDs with Go's
`generateRequestID` (including the nested DNS namespace) and enforce the Go
1000-entry buffer/concurrency ceilings. Engine tests preserve buffered backfill
origins across snapshots and verify policy changes count dropped queued fires.

Overlap fixtures compare Go's actual cancel/terminate client requests with Rust
history and close status. `CANCEL_PREVIOUS` remains cooperative; only the latest
started target receives either action, even if older runs are still open.

Run the full current compatibility suite against an isolated Rust server:

```sh
python3 tests/run-compatibility.py
```

This runs workspace tests and strict clippy, compares scheduler behavior with
Go, builds Rust, selects free local ports, and runs the public/internal gRPC
clients and existing Go SDK over TChannel. It enables schedules and semaphores,
stops its Rust test server afterward, then runs a separate original-Go service
boundary probe with Rust durable queue/History delivery, and writes logs
plus `results.json` to a printed temporary directory. Use `--artifacts-dir DIR`
for a specific results directory. Existing development servers are unaffected.
This suite covers the implemented behavior; a pass does not establish complete
production replacement compatibility (see `memory/migration-inventory.md`).

The full runner also copies the built server to its artifacts directory and
boots it there, using embedded development defaults with no Go checkout in the
working directory. It separately verifies that an explicit Go YAML config still
parses. The copied binary and server working directory are retained with logs.

History version-branch primitives are compared with the pinned Go persistence
implementation using 82 shared fixtures:

```sh
python3 tests/run-version-history-compat.py
```

The fixtures cover inclusive version ranges, append validation, lowest common
ancestors, branch selection/ties, rebuilt branches, and exact error/panic text.
The full runner also executes this differential check. Branch utilities alone
do not establish replication application or persistence-backend compatibility.

The version-history differential suite also checks exact Go binary branch-token
bytes and ancestor clipping through Go's real SQL history store with a mocked DB.
Execution-state tests cover committed tips, buffered/transient events, reset
ancestry, snapshots, and legacy zero-version histories. Internal gRPC checks
validate local `EmptyVersion=-24`, branch tokens, and version histories in both
mutable-state responses. Real replication application remains separate work.

Raw-history persistence blobs are checked against the original Go serializer:

```sh
python3 tests/run-history-blob-compat.py
```

This regenerates fixtures in a temporary directory and checks byte equality for
signal-event and empty batches, including the version preamble, nanosecond
timestamp, negative event version and opaque payload. The internal gRPC driver
also passes an actual Rust raw-history response to Go's persistence decoder and
checks pagination with omitted bounds and changed-bound rejection. These checks
are included in the full compatibility runner. Raw-history export does not imply
remote event application or distributed replication is implemented.

Cluster version ownership and failover arithmetic are compared with Go:

```sh
python3 tests/run-cluster-metadata-compat.py
```

The shared fixtures call the actual Go cluster.Metadata methods and cover
signed division, overflow, migration offsets, disabled-cluster resolution,
local EmptyVersion, unresolved versions, and enabled/remote sets. The full
compatibility runner first boots the copied Rust binary with embedded defaults,
then boots it with a standalone YAML containing cluster0 (offset 0) and remote
(offset 2), increment 10. The public gRPC driver checks global registration at
version 2 and failover to version 10. The original-Go-IDL client also checks
versions 12 and 20 plus new execution history at version 20; it can encode the
newer destination-check override missing from the older web IDL. To run that driver independently, configure the same remote cluster.
These checks do not establish distributed ownership or passive replication.

The cluster metadata differential runner also compares 34 configuration cases
with Go's actual `ClusterGroupMetadata.FillDefaults` and `Validate`. Cases cover
canonical/legacy precedence, empty and absent metadata, disabled clusters,
duplicate/negative/out-of-range offsets, migration-offset presence, and RPC
name/address/transport defaults. Validation errors are compared as sorted sets
because Go iterates cluster maps in unspecified order. The full runner checks
33 defaulted cases through the copied Rust CLI and verifies startup rejects a
zero increment before opening listeners. Wider Go configuration layering,
persistence validation, TLS/auth and redirection remain separate migration work.

Active-active domain state helpers are compared with the original Go package:

```sh
python3 tests/run-active-active-compat.py
```

A Go test overlay calls actual registration, validation, update and merge
functions against 62 shared cases. Coverage includes nil/empty scope maps,
strictly newer versions, stale/equal conflicts, new and untouched attributes,
server-computed versions, migration offsets and explicit same-cluster targets.
A separate byte fixture from the Go mapper/codec verifies zero-valued Thrift
attribute versions are emitted and deprecated region fields are omitted.

The original-IDL gRPC driver exercises registration's initial offsets, partial
failovers preserving other scopes, negative update rejection, no-change attribute
rejection, explicit same-cluster domain requests, sorted destination validation,
and attribute versions in ListFailoverHistory. The full runner includes these
checks. UpdateDomain configuration/failover dispatch, graceful failover, workflow
attribute routing and passive replication remain separate unfinished work.

UpdateDomain controller differential checks:

```sh
python3 tests/run-domain-update-compat.py
```

The Go overlay invokes the actual `UpdateDomain` handler with mocked persistence
and replication transport. Its 34 cases compare errors, returned configuration,
persisted state, write counts, config/failover versions, previous version and
end-time metadata. The oracle explicitly uses a fixed clock, zero cooldown,
retention minimum 1, bad-binary limit 10 and disabled archival; these cases do
not validate production cooldowns or archival providers. They preserve Go's
bad-binary-only response-without-write behavior.

The original-IDL gRPC driver checks masks, empty values, merges, rollback,
local/global restrictions, AA promotion/partial updates and graceful-AA rejection.
The existing Go SDK driver checks Thrift `emitMetric=false`, omitted fields,
data merging and zero-retention rollback over TChannel. Runtime dynamic policy,
archival state machines and distributed graceful failover still require porting.

The combined runner defaults to one Cargo build job to bound peak memory during
large RPC test links. Set `CARGO_BUILD_JOBS` explicitly to override that choice.

Matching service primitive differential checks:

```sh
python3 tests/run-matching-compat.py
```

Original-Go overlays check task-list partition naming/tree arithmetic, actual
manager partition-config validation and update versions/no-ops, and queue ack
watermark/backlog operation sequences. Rust checks use the same fixtures and
also test local config cache refresh/recovery. The combined runner includes this
suite. These checks do not establish independent Matching service replacement:
that requires Rust-owned durable task queues and mixed Go-service integration.
See `memory/service-migration-plan.md` for the active service completion gates.

The original Go four-service baseline is reproducible with isolated SQLite
files, free local ports, original configuration and automatic schema setup:

```sh
python3 tests/run-go-service-baseline.py
# Reuse an existing Go build:
python3 tests/run-go-service-baseline.py --binary /path/to/go/cadence-server
```

Run this long check in tmux `cadence-rust`. Logs, configuration, databases and
results stay in its printed artifacts directory; its server is stopped afterward.
The same `service-workflow-compat.cjs FRONTEND_PEER MATCHING_PEER` probe runs
against both the all-Go baseline and the isolated Rust server, verifying direct
Matching partition RPCs and an 11-event workflow/activity history. This is a
baseline comparison, not yet a mixed Go/Rust service deployment.

The combined Rust runner explicitly overrides `frontend.failoverCoolDown` to
zero through the admin endpoint because the fixture drivers immediately perform
successive failovers. The server retains Go's one-minute default. The override
is recorded in the runner's artifacts directory.

Matching SQL persistence interoperability:

```sh
python3 tests/run-matching-storage-compat.py
```

This checks the actual Go SQL task/list serializers (plain Thrift and Snappy),
then exchanges one SQLite file between the original Go TaskStore and Rust:
Go seeds rows, Rust reads and mutates them, Go validates Rust metadata/tasks and
performs a bounded deletion, and Rust verifies the result. Rust tests also cover
lease fencing across connections/restarts and atomic batch rollback. This is
included in the combined runner. The SQLite component is not yet selected by
server configuration. An opt-in Matching RPC backend and mixed-service harness
now exercise its decision/activity delivery path.

Rust Matching queue delivery against original Go History:

```sh
python3 tests/run-go-service-baseline.py --matching-delivery
```

This starts the original four Go roles with SQLite, then drives a workflow through
a Rust Matching protobuf listener, durable queue and outbound History client. Rust tokens are
completed through Go Frontend; payload/header, heartbeat and 11-event history are
checked. The Rust delivery process reopens its SQLite queue on each poll. This
probe tests the RPC/History boundary and seeds queued tasks itself.

Actual mixed-service routing with no Go Matching process:

```sh
python3 tests/run-go-service-baseline.py --rust-matching
```

This builds a test-only Go CLI overlay using Fx discovery decoration, starts the
original Go Frontend/History/Worker, and advertises the Rust Matching gRPC peer.
The probe calls Go Frontend for all worker operations and also runs an existing
Go SDK workflow/activity over its TChannel API. Domain metadata is seeded into
the Rust test server; this is not production discovery/domain-cache parity.
The combined runner includes both probes. Use `--go-mixed-server-bin` to reuse
this overlay binary; `--go-server-bin` expects an ordinary original Go server.
The harness precompiles the Rust integration executable before service startup
so workflow deadlines exclude compilation time.

The Matching SQLite interop stage also checks cross-domain activity row keys:
task rows use the source domain while transaction fencing uses the target
task-list lease. It verifies both Go-to-Rust and Rust-to-Go reads and rollback
under a stale lease. Queue registry tests cover concurrent single ownership,
shutdown acknowledgements, fencing and stale-manager teardown.

Original-Go poller history differential check:

```sh
python3 tests/run-matching-poller-compat.py
```

This checks identity deduplication, start/end timestamps, outstanding and
isolation-group state, cancellation, empty IDs/identities, strict five-minute
TTL expiry and 5,000-entry MRU eviction. The opt-in native Matching backend now
uses this state for DescribeTaskList, with durable queue read/ack/block fields.
Actual rate enforcement, isolation metadata and controller metrics remain
separate incomplete work. The mixed probe verifies DescribeTaskList through
Go Frontend as well as existing SDK retries, child workflows, timeouts and signals.

Matching garbage collector differential check:

```sh
python3 tests/run-matching-gc-compat.py
```

Fourteen original-Go cases check the deletion cursor, thresholds, force, batch
draining, failures and lock contention. Rust also checks exact time equality and
error cooldown. Native delivery now uses Go's default 100-row/one-second GC
policy, and teardown forces completed-row drainage after final acknowledgement.
Full controller shutdown/in-flight coordination remains incomplete.

Matching idle liveness differential check:

```sh
python3 tests/run-matching-liveness-compat.py
```

This compares Go TTL equality, refresh and clock rollback behavior. Native
controllers check the default five-minute TTL every half TTL, refresh only task
adds/new poll waits, and retain pending rows on idle unload. Tests also cover
old/new owner cleanup races, nonblocking stop, and registration/fatal wakeups.
Production per-task-list config and full in-flight teardown coordination remain
incomplete.

The mixed probe also advances a test-only idle clock after observing an active
poll through Go Frontend. It checks that closing its owner returns an empty
successful poll, then verifies an unacknowledged queued workflow task survives
unload and completes after lease reload. The fixture clock avoids a five-minute
wait; workflow/History behavior remains in the original Go services.
