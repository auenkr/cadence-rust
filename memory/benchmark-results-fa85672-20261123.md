# Benchmark rerun on fa85672d6 — PAUSED, not final

Status: paused at the orchestrator's request until the soak and History gate failures are fixed; results on this master would be stale.

- Branch: `benchmark-rerun-fa85672` (base fa85672d6).
- Harness change: `frontend-matching-benchmark.yml` gained `rust_services` input (default `frontend,matching`; all-Rust cluster = `frontend,matching,history`), passed to `tests/frontend-gate/benchmark/paired-run.py --rust-services`. The strict artifact verifier is skipped when the input is non-default (it hardcodes Go History).
- Dispatched, then CANCELLED (no results): 45233403139 (FE+Matching Rust), 45233414717 (all-Rust). Both were still building the Rust image; head e7c0c55c6.
- Machine spec capture added to the "Capture CI runner details" step (`out/machine-spec.md` in the artifact).

## Machine spec (template, filled per lane from out/machine-spec.md)
- CPU model / cores / threads:
- nproc:
- RAM:
- Disk:
- Kernel / OS:
- Runner image / name:
- Database version:
- Container limits per service:

## Comparison vs 41767896292
TBD.
