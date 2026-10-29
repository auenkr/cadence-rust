# Matching startup formatting artifact — 2026-10-29

- Source run: [40840392628](https://github.com/auenkr/cadence-rust/actions/runs/40840392628), branch `t3/matching-startup-integration-review`, head `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`.
- One `gh run view` confirmed the exact run/head. At inspection the run was `in_progress`; Clippy and formatting steps succeeded, the workspace `Test` step was still running, and later evidence steps were pending. Tests and overall conclusion are unverified.
- One artifact-list API response identified `matching-format-patch`, artifact ID `12882367703`, attached to the same run/head; not expired. GitHub digest: `sha256:93ffd2e5180a336ace530f14a24d7653dc147dfc4853994af45a0eacac9f77ac`.
- Downloaded once from the artifact zip endpoint. Local archive SHA-256 matched the API digest. The archive contained only `matching-format.patch` (19,152 bytes).
- Patch provenance was checked against the exact candidate. It contains only `rust/crates/cadence-matching/src/matching_runtime.rs`, `matching_runtime_native.rs`, and `rust/crates/cadence-persistence/src/matching_postgres.rs`. These are within the startup integration's touched runtime and PostgreSQL pool scope. No unrelated workspace patch was included.
- Applied the artifact patch verbatim to a branch based on the exact candidate; `git apply --check` and `git diff --check` passed. No Cargo, rustfmt, Go, CI dispatch, or additional validation was run. This commit is formatting-only; the source artifact's test and run conclusions remain unverified.
- Follow-on integration branch: `t3/matching-startup-formatting`. Formatting commit and exact changed file list are recorded by Git at this commit.

## Compact provenance

```text
run=40840392628 status=in_progress head=d4ee53f00d505246dc0b7fbbe353d7e989a91c49 branch=t3/matching-startup-integration-review
artifact=matching-format-patch id=12882367703 api_sha256=93ffd2e5180a336ace530f14a24d7653dc147dfc4853994af45a0eacac9f77ac local_zip_sha256=93ffd2e5180a336ace530f14a24d7653dc147dfc4853994af45a0eacac9f77ac
steps: matching formatting=success; clippy=success; formatting=success; workspace tests=in_progress; overall conclusion=unverified
patch files: rust/crates/cadence-matching/src/matching_runtime.rs; rust/crates/cadence-matching/src/matching_runtime_native.rs; rust/crates/cadence-persistence/src/matching_postgres.rs
```
