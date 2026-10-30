# Matching dynamic config reader audit review — 2026-10-30

## Candidate identity and ancestry

- Candidate: `caa32882f9a3fd1da4450c8b216607c053996893` (`Fix Matching dynamic config audit source scan`).
- Git decoration identifies `t3/matching-dyncfg-reader-audit-fix` and
  `origin/t3/matching-dyncfg-reader-audit-fix` as the candidate branch.
- Parent: `0032e7b3341275c028952daa82ba302d9ede0f02` (`Name Matching query dispatch test callback type`).
- Candidate changes only `rust/crates/cadence-matching/src/matching_dyncfg_defaults_tests.rs`.

## Source review

The change replaces stopping at the first exact top-level `#[cfg(test)]` with
stopping at the first exact top-level `#[cfg(test)]` followed by either
`#[path = ...]` or `mod ...`. In the candidate's `grpc_matching.rs`, the early
top-level test-only `QueryDispatchFault` alias (line 385) precedes later
production dynamic-config readers (starting around line 582); the first
external test module is at line 4384. The new boundary therefore preserves the
production readers after the alias. It also preserves nested test-only fields,
initializers and methods, as its comment says.

The assertions were not weakened: the reader table, quoted-key lookup,
700-byte default window, and shared-constant equality checks are unchanged.
The delimiter search has no Rust lexer or brace/comment/string handling. By
source inspection, the candidate `grpc_matching.rs` has no earlier exact-line
false delimiter, and every key in the current reader table has a production
reader before any later test-only references. Thus I found no concrete false
pass or truncation in this candidate's existing cases that justifies a source
change. The helper remains a textual heuristic: a matching marker embedded in
a source string/comment, or a key occurring only in a nested `#[cfg(test)]`
block, could confuse it. Those cases are not demonstrated by the current
inputs and would need dedicated cases before changing the implementation.

No local Cargo, Go, formatter, or runtime validation was run.

## CI checkpoint

Inspected GitHub Actions run
https://github.com/auenkr/cadence-rust/actions/runs/41033025800 once. It is
`in_progress` for exact head `caa32882f9a3fd1da4450c8b216607c053996893` on
`t3/matching-dyncfg-reader-audit-fix`. The fresh-target job has selected its
target, but formatting/clippy/tests and evidence upload were still pending at
inspection. No artifact, clippy result, workspace-test result, binary hash, or
terminal conclusion is available yet. Per the review instruction, this is a
checkpoint: do not poll it or dispatch
`matching-production-validation.yml` until a future review verifies terminal
success and the required clean uncached target results.

Source freeze retained; no production or test source files changed in this
review.
