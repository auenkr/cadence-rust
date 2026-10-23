# Matching metrics parity validation — 2026-10-23

Worktree: `/mnt/storage/service/github/wt/metrics`.
Branch: `matching-metrics-parity-20261023`, based on candidate `4f5fb6722`.
Code frozen at `cd9309695` (subsequent evidence-only commits do not change code).
Go IDL submodule initialized at `bfbf273b41b9e36ce21e47a49600d22d73e4614c`.
Root checkout was not edited.

## Reproduction

Run from `rust/`, with `CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared`.
The replacement worker uses tmux session `cadence-rust`, window `metrics-single`.
It waits for mixed gates to stop, load <= 6, and MemAvailable >= 2 GiB, checking
once per 60 seconds. Only the single host-wide lock is used:

```sh
flock /tmp/cargo.lock cargo clippy --workspace --all-targets -j3 -- -D warnings > final-clippy.log 2>&1; echo $? > final-clippy.exit
flock /tmp/cargo.lock cargo test --workspace -j3 > final-test.log 2>&1; echo $? > final-test.exit
```

No separate final cargo check is run: clippy type-checks. The old `check` log
(exit 0) predates the final source. The old queued clippy was canceled with
SIGTERM and never wrote an exit file; see `requeue.md`. Only `final-*` supplies
final-source validation. Each command is run after a fresh resource/gate check.

## Results

Pending final-source command completion. This file will be updated with real
exit codes after all processes finish. No mixed-cluster metrics parity is claimed.

## Tested contracts

RecordingMetricSink assertions cover manager/scaler scope dimensions,
canonical tasklist/semaphore keys, duration/integer histogram metadata,
RPC-scope inheritance for engine counters, persistence names/error semantics,
overall/per-domain dimensions, and shard routing metrics on failed lookup.
Exporter tests cover the existing duration/integer bucket paths; the new
persistence histogram uses the Go Default1ms100s formula in all exporters.

## Outstanding

The implementation and matrix are partial. See the matrix's unresolved parity
dependencies. In particular, no fake forwarding metrics were added where the
candidate lacks the corresponding backlog-forwarding behavior. No Go logic
was changed and no mixed-cluster or external SDK run was performed by this worker.
