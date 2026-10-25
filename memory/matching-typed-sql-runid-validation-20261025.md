# Typed SQL Matching RunID dependency

Reviewed and integrated source `21c16e33b6f150ade87acbc968f8fd523191574d`.
The five-file change carries malformed SQL RunID identity through a typed
persistence error instead of flattening it into backend configuration text.
It does not install process-fatal behavior.

- Persistence focused CI: https://github.com/auenkr/cadence-rust/actions/runs/40220438457 — success, source SHA above.
- Matching focused CI: https://github.com/auenkr/cadence-rust/actions/runs/40220724914 — success, source SHA above; Clippy and tests passed.

These are focused dependency checks, not final fresh workspace or mixed
acceptance evidence. Phase 2 `c884dde214b7b0b92d061de856b60dbe1a6f14e1`
remains unmerged pending lifecycle review and a configured-process regression.
Its SQL parser acceptance corrections are also not in this dependency merge.
Matching remains incomplete. No local Cargo, Go, or formatter was run.
