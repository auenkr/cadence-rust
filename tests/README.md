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

Both drivers leave completed workflows and their domains available in the web
UI for inspection. They create unique IDs so repeated runs are isolated.

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
starts no Go Cadence server, stops its test server afterward, and writes logs
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
