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
`get_or_create_with_idle_ttl` constructs a candidate manager before acquiring
the registry mutex. The mutex protects lookup/insertion only and is released
before `manager.start().await`. Concurrent cold requests may construct losing
unstarted candidates; only the inserted manager is started. The `OnStop`
callback captures only a `Weak` registry reference and removes only the same
manager instance. The candidate's validation and in-memory initialization have
no observed external effect when a losing candidate is dropped.

## Startup scan cancellation regression

The semaphore test fixture now permits its startup scan to be blocked and then
unblocked without sleeps. The new
`matching_semaphore_aborted_start_unregisters_releases_waiter_and_rebuilds`
test parks an acquire future while the registered manager's scan is pending,
aborts the `get_or_create` task, checks the waiter receives `NotReady` and the
registry entry is removed, then retries and checks the replacement is registered
and reused. This exercises the existing `LoadInFlight::drop`/`stop()` path; no
production defect was established, so implementation code was unchanged.
The test has not been run locally (local Cargo/Go/formatter/runtime execution
is prohibited); parent owns CI scheduling and validation. No CI or mixed-gate
result is claimed for this test revision.

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

Pending dispatch and completion.
