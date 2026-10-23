# Matching membership gap G1: failed lookup asks the provider to refresh (2026-10-24)

Branch `matching-membership-g1-20261024` (from origin/matching-membership-tests-20261023 at 176888b48).
Outcome: **no code change, stopped**. The brief allowed implementation only if a provider refresh seam already
exists in Rust. It does not, so no test was added and no CI run was made (nothing code-side changed).

## Go behaviour

- `go/common/membership/hashring.go:175-182` `Ring.LookupRaw`: when the ring has no owner for the key
  (`ErrInsufficientHosts`), it calls `r.signalSelf()` before returning the error.
- `hashring.go:240-246` `signalSelf`: non-blocking send into `refreshChan` (capacity 1).
- `hashring.go:336-350` `refreshRingWorker`: `refreshChan` receive calls `Refresh()`, which calls
  `provider.GetMembers`. The same worker also refreshes on a 2s ticker.
- `Lookup` (`hashring.go:185-192`) goes through `LookupRaw`, so a failed Lookup always signals.
- Note: `resolver.go` `MultiringResolver.Lookup` (line 187) is a pass-through to the ring; the refresh trigger is in the
  ring, not the resolver.
- Test: `hashring_test.go:165-183` `TestFailedLookupWillAskProvider`. `GetMembers` is expected twice (first on
  creation, second from the failed Lookup on an empty ring). `Lookup("a")` must return an error and the second refresh
  must happen within `maxTestDuration`.

## Rust behaviour

- `rust/crates/cadence-matching/src/matching_membership.rs:108-115` `Membership::owner` returns
  `Status::internal("Not enough hosts to serve the request")` on an empty ring. It never signals anything.
- `Membership` holds only the ring, channel cache, a `watch::Sender<u64>` for outgoing change notifications, TLS and
  metrics. It has no inbound refresh channel and no reference to the lifecycle worker.
- `rust/crates/cadence-matching/src/matching_peer_provider.rs:169-181` `MatchingPeerProvider` trait: `start`,
  `members`, `subscribe`, `self_evict`, `stop`. There is no refresh method.
- `rust/crates/cadence-matching/src/matching_membership_lifecycle.rs:87-130` worker loop refreshes only from the
  provider's broadcast `PeerChange` events (`events.recv()`) and the 2s `ticker`. It does not listen to any
  membership-originated signal.
- The fake provider used by lifecycle tests (`matching_membership_lifecycle.rs:233`) is private to that test module.

## Finding

The provider refresh seam for failed lookups does not exist in Rust. Adding one is a design change, not a
one-line port:

1. A shared inbound signal (for example a capacity-1 `tokio::sync::mpsc` or `Notify` held by `Membership` and
   received by the lifecycle worker), with a non-blocking send matching Go's `signalSelf`.
2. A new select arm in the lifecycle worker that calls `snapshot` and `replace_if_addresses_changed`. It must keep
   the `MIN_REFRESH` throttle semantics that the existing tests pin.
3. A signal call on the empty-ring path only, matching Go `LookupRaw`. The current `owner()` is also used by
   `owns`, `remote_channel` and `check_task_list`, so every caller would inherit the signal.

Because (1)-(3) add a new seam and change worker behaviour, the brief's stop condition applies. Decision needed from
the owner: whether to add this seam in a separate change, and whether the signal should fire from every `owner()`
call or only from the lookup entry points.

## Test plan for when the seam exists

Mirror `TestFailedLookupWillAskProvider` in `matching_membership.rs` or the lifecycle tests with a fake provider that
counts `members` calls:

- Start the lifecycle with an empty peer set (or a set that yields no matching hosts), so the initial ring is empty.
- Call `owner("a")` and assert it is an error.
- Wait for `members` count to reach 2 within a bounded timeout, and assert it does not stay at 1.

Not done: no unit test was written, because a test against a non-existent seam would only be a placeholder.

## CI

No `rust-ci.yml` run was dispatched for this branch. The only commit is this notes file.

## Implementation (2026-10-24, branch `matching-membership-g1-impl-20261024`)

Status: **G1 closed for the owner-lookup path.** Seam added, test added, CI green.

Change (commit `df89143fb`, `rust/crates/cadence-matching`):

- `matching_membership.rs`: `Membership` holds `refresh: Arc<tokio::sync::Notify>`, a shared signal that stores at
  most one permit (Go `refreshChan` capacity 1, non-blocking `signalSelf`). `owner()` calls `notify_one()` only on
  the empty-ring error path, matching Go `Ring.LookupRaw`. `refresh_signal()` hands the Notify to the lifecycle.
- `matching_membership_lifecycle.rs`: the worker has a new `refresh.notified()` select arm. It runs the same snapshot
  and `replace_if_addresses_changed` body as the ticker arm, behind the same `MIN_REFRESH` guard. Go's `Refresh()` has
  the same rate limiter (`hashring.go:292-295`), so this is a mirror and not a new throttle.
- Test `matching_membership_lifecycle::tests::failed_owner_lookup_asks_provider_to_refresh`
  (`start_paused`): mirrors Go `TestFailedLookupWillAskProvider`. Start gives 1 fetch. Each failed `owner("a")` on the
  empty ring gives one more fetch (2, then 3). Paused time means the 2s ticker cannot cause those fetches.

Design choices:

- The signal fires from `owner()`, not from `owns`/`remote_channel`/`check_task_list` directly. Those callers all reach
  `owner()`, and the empty-ring error is the only path that signals. This matches Go, where every `Lookup` goes through
  `LookupRaw`.
- The brief said "immediate" refresh. The arm does not bypass `MIN_REFRESH`. That follows Go's rate limiter. The first
  refresh after a failed lookup on a never-changed ring is still immediate, because `last_changed` is `None`.

CI:

- Dispatched: `gh workflow run rust-ci.yml --ref matching-membership-g1-impl-20261024 -f package=cadence-matching`
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40031038533 (head `df89143fb`), **success**
  (`clippy-test` job green; `cargo test` ran `running 569 tests`: 558 passed, 0 failed, 11 ignored; the new test is `ok`).
- A push-triggered run on the same SHA (`40031032295`) was cancelled. It was superseded by the dispatch.
- Not done here: no local cargo run (per instruction). No workspace-wide or mixed Go/Rust gate was run.
  Other crates were not affected by this change, but that was not run as a separate CI gate.
