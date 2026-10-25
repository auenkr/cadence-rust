# Matching integration evidence (2026-10-25)

## Run 40177106445 (branch t3/respond-query-task-parity)
- URL: https://github.com/auenkr/cadence-rust/actions/runs/40177106445
- Head SHA: 78006b797e658d3169828e8cab8f481c3894e2b4
- Conclusion: success
- Artifact exit values (rust-ci-logs): clippy=0, test=0, fmt=0, query-format=0, tls-format=0, workspace-format=1
- go-oracle (matching-describe-tasklist-go-oracle): go-oracle.exit=0
- workspace-format=1 comes from the non-blocking `cargo fmt --all -- --check` step (rust-ci.yml, continue-on-error): format.log lists diffs in 92 files (e.g. cadence-api/src/admin.rs, b64.rs, lib.rs, schedule.rs), i.e. pre-existing repo-wide rustfmt drift, not the query/TLS-touched files (query-format=0). A workspace-wide fix must go through the CI formatting mechanism, not local cargo.

## Run 40186673773 (integration head)
- URL: https://github.com/auenkr/cadence-rust/actions/runs/40186673773
- Branch: t3/matching-integration-20261025, head SHA aa268a8d38919dafde0704146539c38c76c9fba9
- Status when checked: in_progress (job clippy-test, no conclusion yet). Re-check with `gh run view 40186673773`.
