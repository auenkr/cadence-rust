# Mixed Go/Rust gates

`tests/run-go-service-baseline.py` boots the cluster and runs the shared SDK probe.
Each service is either the original Go server or its Rust binary `cadence-<service>`.
`--rust-services` picks the Rust set; every other service stays on Go.

The launch table is `tests/gates/services.json`. A name is accepted by
`--rust-services` only when its entry has `"rustSupported": true`. Today only
`matching` is supported. The `cadence-frontend`, `cadence-history` and
`cadence-worker` crates are `not_migrated` stubs, so selecting them fails with the
recorded reason.

## Choosing the topology

| Topology | Command flags | Go processes | Rust processes |
| --- | --- | --- | --- |
| All-Go baseline (default) | none | frontend, history, matching, worker | none |
| Rust Matching, Go everything else (production lane) | `--rust-services matching` | frontend, history, worker | `cadence-matching start --services matching` |
| Same, prebuilt binary (no cargo) | `--rust-services matching --rust-production-binary PATH` | frontend, history, worker | PATH, which must be named `cadence-matching` |
| Legacy Go-overlay Matching lanes | `--rust-matching` or `--matching-delivery` without `--rust-services` | frontend, history, matching, worker (Go test-binary overlay) | Rust test executable |

Other subsets follow the same rule: `--rust-services history`, `worker`, and so on,
once that service's entry is supported and it has a probe. Until then the harness
rejects them before building anything.

`go_services` is computed as the services not in `--rust-services`, and that list
becomes the Go `start --services` argument.

## Build and run

Every cargo call goes through `cargo-safe` (`/mnt/storage/service/bin/cargo-safe`, or
`$CARGO_SAFE`). That wrapper holds the host-wide lock and applies job and memory caps.
The shared target directory is `$CARGO_TARGET_DIR`, defaulting to
`/mnt/storage/service/cargo-target-shared`.

The production lane runs one `cargo-safe build -p cadence-matching` before any service
starts. The binary path comes from cargo's JSON artifact report, not from a guessed
path. The legacy `--matching-delivery` and Go-overlay lanes add one
`cargo-safe test -p cadence-matching --test matching_delivery --no-run`. No cargo runs
once services are up.

Recommended flow, in tmux session `cadence-rust`:

```sh
# 1. Build the gate binaries only (one cargo-safe build, plus the Go server).
python3 tests/run-go-service-baseline.py --rust-services matching --build-only \
  --artifacts-dir /mnt/storage/service/gate-runs/matching-build

# 2. Run the gate with the recorded binary; no cargo runs during the gate.
python3 tests/run-go-service-baseline.py --rust-services matching --matching-restart \
  --rust-production-binary "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["rustMatchingBinary"])' /mnt/storage/service/gate-runs/matching-build/build-only.json)" \
  --artifacts-dir /mnt/storage/service/gate-runs/matching-sqlite
```

Use `--dry-run` to check a lane's planned config and commands without building or
starting anything. It writes `dry-run.json` and config files and exits 0.

Load rules from AGENTS.md apply: check `uptime` and `free -h` first, run one gate at a
time, do not run cargo while a gate is running, and stop any gate containers when the
gate ends.

## Database choice

| Flag | Store | Settings |
| --- | --- | --- |
| (none) | SQLite files under the artifacts dir | none |
| `--mysql` | MySQL 8.0, Go v8 schema, one shared database | `CADENCE_MYSQL_ADDR` (host:port, default `127.0.0.1:3306`), `CADENCE_MYSQL_USER` (`root`), `CADENCE_MYSQL_PASSWORD` (`cadence`), `CADENCE_MYSQL_DATABASE` (`cadence`), `CADENCE_MYSQL_VISIBILITY_DATABASE` (`cadence_visibility`), optional `CADENCE_MYSQL_DOCKER_CONTAINER` for SQL observations |
| `--postgres` | PostgreSQL, Go-configured schema, one shared database | `CADENCE_POSTGRES_ADDR` (`127.0.0.1:5432`), `CADENCE_POSTGRES_USER` (`postgres`), `CADENCE_POSTGRES_PASSWORD` (`cadence`), `CADENCE_POSTGRES_DATABASE`, `CADENCE_POSTGRES_VISIBILITY_DATABASE`, optional `CADENCE_POSTGRES_SSLMODE` and `_CA_FILE`/`_CERT_FILE`/`_KEY_FILE` |

`--mysql` and `--postgres` require `--rust-services matching`. They are mutually
exclusive. The GC and domain-metadata SQL observations are MySQL-only, so PostgreSQL
runs record `productionDatabaseEvidence` as not collected.

`--mysql` also checks that the database holds exactly the Go schema plus the setup
tables (`goSchemaOnly` in `results.json`).

## Ports

The harness reserves loopback ports at random on every run and records them in
`ports.json` in the artifacts directory. Key names:

| Key | Use |
| --- | --- |
| `FRONTEND_PORT` / `FRONTEND_PORT_GRPC` | Go Frontend TChannel and gRPC; the SDK and probe connect here |
| `HISTORY_PORT` / `HISTORY_PORT_GRPC` | Go History (Rust Matching calls it) |
| `WORKER_PORT`, `MATCHING_PORT` and their `_GRPC`/`_PPROF` keys | Go Worker, and Go Matching in the Go-only lanes |
| `RUST_MATCHING_PORT` | Rust Matching TChannel (the production Ringpop member) |
| `RUST_MATCHING_PORT_GRPC` | Rust Matching gRPC, the probe's direct client |
| `RUST_MATCHING_SECONDARY_PORT` / `_GRPC` | second Rust Matching instance, only with `--matching-failure-recovery` |

Ringpop bootstrap hosts are the Go Frontend, History and Worker ports, plus the
Rust Matching ports. The Go Worker port stands in for Matching seeds in Rust lanes.

## Probe scenarios

The production lane runs `tests/matching-service-compat.cjs` against the Go Frontend.
It covers the Go SDK samples in `tests/go-sdk` (echo, activity retry and timeout,
child and workflow retry, decision retry, signal, cancellation), query dispatch,
isolation with CancelOutstandingPoll, native partition config, idle eviction and
recovery, and heartbeat/timeout RPCs.

Other scenarios:

- `--matching-restart`: SIGTERM drain and recovery, then SIGKILL restart and
  recovery, plus task GC and domain metadata checks. Needed for the full set.
- `--matching-failure-recovery`: two Rust Matching instances. The probe checks the
  ring, typed non-owner `ABORTED` responses, SIGKILLs the owning instance, and
  checks that the surviving instance takes over. Needs `--rust-services matching`.
  The kill-one-node step was not yet passing when this was written (see
  `memory/evidence/matching-twonode-20261023`).
- `--matching-idle-check-seconds N`: shortens the idle task-list check interval.
- `--matching-exporter prometheus|statsd|m3`: captures one exporter and validates
  SDK-driven metrics (including a SIGTERM flush for statsd and m3).
- `--history-transport grpc|tchannel`: Rust Matching's outbound History transport.

Results:

- `results.json` holds the topology, ports, probe exit code, `runExitCode`, and
  `acceptanceEligible` (false in the production lane, by design).
- `run.exit` holds the run's exit code.
- `probe.log`, `server.log`, `rust-server-*.log` and `rust-build.log` hold logs.
- `--evidence-dir DIR` copies the configs, binaries' SHA-256 sums and results into
  `DIR` before the artifacts directory is removed.

## Adding a Rust service later

1. Make the crate's binary real. It must accept `start --config ... --services <svc>`.
2. Set `rustSupported: true` for the service in `tests/gates/services.json`.
3. Add its launch args, ports and probe to the harness. The probe must drive that
   service end to end while the others stay Go.
4. Add the service to the table above and run `--dry-run` before any gate.

## Not covered here

- Other `tests/run-*.py` and `scripts/*.sh` still call `cargo test -p cadence-server`.
  That target no longer contains the `matching_delivery` test after the crate split.
  They are out of scope for this harness and need the same change.
- Go Frontend, History and Worker are the originals. `cadence-frontend`,
  `cadence-history` and `cadence-worker` are stubs, so they cannot be selected yet.
