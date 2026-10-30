# Matching candidate repair checkpoint — 2026-10-30

## Candidate source and ancestry

The candidate starts from the exact reviewed parity tip
`377fb2f8eea691f6740b895aaa4583ac6963b2eb` on
`origin/t3/review-matching-parity`. Its merge base is current `origin/master`
`5b0423a8781eecfa506e5b2726c874c597f754ac`; the tip carries the Matching
implementation integration and its evidence notes. Separately reviewed
`95735d2efae76ff2b0fb42a1cb052c2f1702efd5` on
`origin/t3/matching-ring-peer-removal`; it has the same merge base and changes
only `ringpop_provider.rs` plus `memory/matching-ringpop-removal-20261030.md`.
The source delta is bounded to logging the first eight successfully handled
inbound PINGs (source, accepted changes, and current membership). It does not
change membership handling or readiness. These histories were combined without
duplicating their source changes.

The semaphore fix renames the captured weak registry reference from
`managers` to `registry` in
`rust/crates/cadence-matching/src/matching_semaphore_manager_native.rs`.
`get_or_create_with_idle_ttl` still holds the registry `MutexGuard` across
manager construction and insertion, then leaves the lexical block before
`manager.start().await`. The `OnStop` callback still captures only a `Weak`
reference and removes only the same manager instance. This preserves both
creation serialization and callback lifetime behavior.

## Failed CI and discovery diagnosis

Fresh-workspace run
https://github.com/auenkr/cadence-rust/actions/runs/41025116183 failed at
head `377fb2f8eea691f6740b895aaa4583ac6963b2eb`. The full action logs show
format checks and all earlier Go/schema/audit oracles succeeded. Workspace
Clippy then failed twice with E0599 at
`matching_semaphore_manager_native.rs:670`: the `managers` identifier resolved
to `Weak<Mutex<HashMap<...>>>` because the callback capture shadowed the guard
declared at line 652. The compiler notes identify the guard as the intended
binding. Formatting and workspace tests were skipped after Clippy failed; no
other compile errors appeared in that run.

The outstanding production discovery diagnosis remains as documented in
`matching-ringpop-removal-20261030.md`: Go Ringpop removed the only Rust
Matching peer after it had been added, leading Go Frontend's Matching hash ring
to become empty. The retained logs do not establish whether Go PING reached
Rust or why the peer was evicted. The bounded PING trace is intended to
distinguish that transport/protocol path on a later mixed gate.

## Validation and acceptance remaining

No local Cargo, Go, formatter, or runtime commands were run. The workflow
`.github/workflows/rust-ci.yml` supports optional `package` (empty means
workspace) and `clean_target` (true selects a fresh runner target). Dispatch
the fresh uncached workspace run on the pushed candidate with package unset
and `clean_target=true`; record its run URL, exact head SHA, and conclusion
below. It must finish Clippy followed by workspace tests before this candidate
has fresh workspace validation.

After that pass, Matching is still not accepted until the mixed Go-services /
Rust-Matching gates exercise SDK workflows, no-Go-Matching topology, failure,
restart, and recovery, with Rust binary SHA256 recorded. Do not start a mixed
gate before the fresh workspace pass is verified. Then complete the Matching
parity audit before beginning History migration.

## Fresh workspace CI evidence

Dispatched `.github/workflows/rust-ci.yml` with `package` unset and
`clean_target=true` on candidate source head
`f8739bfbfc9ce1921b3404dd9cf6fd6193cb9db5`:
https://github.com/auenkr/cadence-rust/actions/runs/41026357394

The run was observed in progress at `2026-10-30T08:25:42Z` (UTC), with its
`clippy-test` job starting. No conclusion, Clippy result, or test result was
available at that observation. Run head is the exact candidate source SHA
above; this evidence note is a follow-up commit and does not change the
dispatched run's source. Inspect the run once when resuming; do not start a
mixed gate unless it completed successfully.
