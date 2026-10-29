# Matching formatted regression candidate — 2026-10-29

Candidate branch: `t3/matching-formatted-regression-candidate`, based on `25e74c28bef0d7e4311ca2edc6132df50557f328` (`t3/review-matching-startup-regressions`). Cherry-picked formatting commit `1066ee1680b31d536f28cf9c332c60aa44d15823`, originally based on `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`. Its provenance and verified hosted artifact archive hash are recorded in `memory/matching-startup-format-artifact-20261029.md`.

Reviewed the three source diffs. The two conflicts in `matching_runtime_native.rs` were resolved retaining the newer subprocess exit/output/selected-case assertions and all added engine/control configuration cases, while incorporating compatible formatting. The adapter and semaphore regressions and source corrections in the base remain intact. Other changes in `matching_runtime.rs` and `matching_postgres.rs` are formatting. Formatting of newly overlapping code is not verified; request the hosted formatting patch below. No local Cargo, rustfmt, or Go commands were run. `git diff --check` is a whitespace check only, not a formatting or test pass.

Actions inspected once: [run 40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628), head `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`, was `in_progress` with no conclusion. It was last known to be running workspace tests. No new CI was dispatched while it remained live. After it is terminal and no other intensive CI or mixed gate is active, dispatch exactly:

```sh
gh workflow run rust-ci.yml --repo auenkr/cadence-rust --ref t3/matching-formatted-regression-candidate -f package=cadence-matching -f clean_target=true -f format_matching_parity_files=true
```

Record the run URL, actual head SHA, status and eventual conclusion; inspect and apply any relevant hosted format patch before claiming formatting success. This candidate is unverified until hosted CI. The independently authored coordinated inversion test is not included and must be integrated before final workspace validation. Matching parity and mixed-cluster acceptance remain open.
