# Matching integrated startup formatting CI — 2026-10-29

## Evidence

- Requested candidate: `t3/matching-startup-integration-review` at `d4ee53f00d505246dc0b7fbbe353d7e989a91c49` (fetched from origin). Integration note: `memory/matching-native-startup-integration-review-20261029.md`.
- Prior diagnostic: [run 40839466118](https://github.com/auenkr/cadence-rust/actions/runs/40839466118), head `12c3063ea91e0dc5abd552a79140a14690edb8bd`, terminal failure. Per handoff, formatting failed at `matching_runtime.rs:925`; clippy and tests were skipped. This is not validation evidence for the candidate.
- Before dispatch, queried recent Actions once; returned runs were terminal, including diagnostic run 40839466118. No active job appeared in the list.
- `.github/workflows/rust-ci.yml` supports `format_matching_parity_files` (input description: apply rustfmt to Matching parity files and upload patch; artifact `matching-format-patch`, retention 3 days). Its file list includes `matching_runtime.rs`, `matching_postgres.rs`, `matching_membership.rs`, and `membership.rs`, covering the runtime error and integration-note pool/membership files.
- Dispatched `rust-ci.yml` on the candidate with `package=cadence-matching` and `format_matching_parity_files=true`: [run 40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628), head `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`, standard hosted runner. At the single status check, run status was `in_progress`; no conclusion or formatting artifact was available. No patch was collected or applied.
- No Cargo, rustfmt, Go, mixed-cluster gate, or local validation was run. Clippy/tests remain unverified.

## Next commands

1. Once run 40840392628 is terminal, collect artifact `matching-format-patch` with `gh run download 40840392628 --repo auenkr/cadence-rust -n matching-format-patch -D /tmp/matching-format-40840392628`.
2. Inspect the patch (`git apply --stat` and `git apply --check`) and confirm its file paths are limited to the workflow's listed Matching parity files. Apply only if it is formatting-only; then commit and push that formatting change to the candidate branch. If the run failed before artifact upload or the patch is out of scope, report that without claiming formatting complete.
3. On the resulting immutable head, dispatch focused clean-target CI in serial order: `gh workflow run rust-ci.yml --repo auenkr/cadence-rust --ref t3/matching-startup-integration-review -f package=cadence-matching -f clean_target=true`, then the persistence package workflow. Record each run URL, head SHA, and conclusion. Do not report pass based on the formatting run.
4. After those prerequisites, follow the integration note's dependency-oracle, full workspace, and mixed-cluster gates. Do not start workspace/PG validation before the focused prerequisite succeeds.

Other workers' adapter-lock and semaphore test changes are not integrated here. Frozen `matching-native-final-20261029` and master were not moved or merged.
