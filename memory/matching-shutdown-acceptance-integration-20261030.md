# Matching shutdown trace + acceptance fixes integration (2026-10-30)

Status: **PROVISIONAL; not acceptance evidence.** Combined source tip is the merge of `e3802467c0ccc0e7b012e207164d3e212c653e70` (`origin/t3/shutdown-trace-postgres-validation`) and `3479f19342db200a79802d49333dd7c4bea18d09` (`origin/t3/matching-acceptance-fixes`). The latter is based on `d0ef6bf3a4fb8056b22a0b10d56ccd16e8f495c0`, which is an ancestor of the former, so both commit chains are retained.

The merge preserves the PostgreSQL workflow shutdown-trace flag and diagnostic memory from the first chain, plus the owner-aware restart/crash assertions and their acceptance-fixes memory from the second. The merge was clean; no conflict resolutions or behavior changes were needed.

Limitations / coordination: active gate `41100032910` remains owned by `main` at pin `4a5da9fb`; this integration did not change the pin, dispatch CI, or inspect/poll that run. Process capture `c9c470` remains separate and pending. Semantic review `573851` is running; its parent owns any follow-up integration. No Cargo, Go, build, formatting, test, runtime, or Node syntax checks were run. No acceptance pass is claimed. Lightweight ancestry and `git diff --check` checks only.
