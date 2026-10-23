# Gate harness per-service layout (branch matching-gates-per-service-20261023)

Scope: the mixed Go/Rust gate harness now launches the per-service `cadence-matching`
binary instead of `cadence-server`. Go Frontend/History/Worker are unchanged.
The full gate was NOT run here; the orchestrator schedules it.

## What changed
- `tests/run-go-service-baseline.py`: `--rust-services` (default none = all-Go),
  `--build-only`, `--dry-run`, one `cargo-safe build -p cadence-matching` for the
  production lane (or `--rust-production-binary`), legacy test build moved to
  `-p cadence-matching --test matching_delivery`.
- `tests/gates/services.json`: launch table. Only `matching` is `rustSupported`.
- `tests/gates/README.md`: topologies, build flow, DB choice, ports, scenarios.
- `tests/matching-service-compat.cjs`: wording only; the binary still comes from
  `CADENCE_RUST_MATCHING_PRODUCTION_BINARY`.

## Evidence (no gate run)
- `python3 -m py_compile` and `node --check`: pass.
- `--help`: lists the new flags.
- `--dry-run`: all-Go default, `--rust-services matching --mysql`,
  `--rust-services matching --postgres --matching-restart`, and `--matching-delivery`
  each exit 0 and write `dry-run.json` (no cargo, no Go build, no services).
- Rejected as expected (exit 2): `--rust-services history` (stub),
  `--matching-delivery` with Rust matching, `--rust-production-binary` not named
  `cadence-matching`, `--mysql` without `--rust-services matching`,
  `--build-only` with `--dry-run`.
- Back-compat: generated `rust-matching.yaml`, `rust-dynamic-config.yaml`,
  `ports.json`, and `config/*.yaml` for
  `--rust-services matching --mysql|--postgres` match the legacy
  `--rust-matching --matching-restart --rust-production-binary` output (ports,
  paths and the diff of directory names normalized).
- Build: `run-go-service-baseline.py --rust-services matching --build-only
  --binary /bin/true` ran one `cargo-safe build -p cadence-matching` (exit 0,
  record `/mnt/storage/service/gate-runs/gates-build-20261023/build-only.json`).
  Resolved `cadence-matching` at `/mnt/storage/service/cargo-target-shared/debug/cadence-matching`.
- Binary accepts `start --config --root --services --port --history-grpc-port
  --matching-grpc-port --matching-tchannel-port --data-dir`.

## Not verified
- A full mixed gate with the new layout (SDK, restart, two-node): not run.
- Rust History/Frontend/Worker lanes: not possible until those crates are real.
- Other `tests/run-*.py` and `scripts/*.sh` still call `cargo test -p cadence-server`
  (the `matching_delivery` test moved to cadence-matching in the crate split).

## How to run the production gate (after build)
    python3 tests/run-go-service-baseline.py --rust-services matching --matching-restart \
      --rust-production-binary <build-only.json rustMatchingBinary> \
      --artifacts-dir <dir> [--mysql|--postgres] --evidence-dir <dir>
