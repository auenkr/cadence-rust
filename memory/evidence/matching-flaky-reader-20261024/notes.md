# Matching flaky reader test: `matching_reader_dispatch_success_uses_known_or_default_group`

Date: 2026-10-24. Branch: `matching-flaky-reader-20261024` (from `origin/master` at `e7285da4f`).
Scope: test-only change in `rust/crates/cadence-matching/src/matching_reader.rs`. No production code changed.

## Original failure

- Whole-workspace CI run 40036834896 failed at `matching_reader_dispatch_success_uses_known_or_default_group`
  (assert at the `known.task_id, 1` line): `left: 2, right: 1`.
- Rerun 40037552344 on identical code passed.

## Cause

`TaskReader::poll_group(group)` for a known isolation group does a `select!` over two sources
(`matching_reader.rs`, `poll_group`):

1. the group's own isolated channel, and
2. the shared default channel, via `self.poll()` (Go `matcher.Poll` semantics: a known group also reads the shared channel).

The test appends `[partition_task("a"), TaskData::default()]` (task 1 in group `a`, task 2 in the default group),
then called `poll_group("a")` first. Task 2 is on the shared channel, so `poll_group("a")` can legitimately
receive it before the default poller does. Whichever source is ready first wins the `select!`, and the
result depends on dispatcher timing. Hence `known.task_id == 2` sometimes.

The production behaviour is correct. The test assumed an ordering the shared channel does not guarantee.

Checked: `Rendezvous::poll` moves ownership synchronously (`matching_reader.rs`, comment at `poll`), so
the losing `select!` branch never drops a task. The race is purely in which poller the test reaches first.

## Fix

Poll the default poller first, then the known group:

- `plain = reader.poll()` can only ever receive task 2, because only task 2 is on the shared channel. Deterministic.
- Then `known = reader.poll_group("a")` receives task 1 from its own channel, since the shared channel is now empty. Deterministic.

Same change applied to the sibling test `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group`,
which had the identical race and failed in the first verification run (below).

Other `poll_group` tests in the module were reviewed and are not affected:
- `matching_reader_isolation_buffers_avoid_cross_group_head_blocking` only ever has the shared channel fed by a
  task it then fetches itself.
- `matching_reader_misconfigured_isolation_group_dispatches_to_default` uses `poll()` only.

## Commits

- `c1911c509` Make matching reader dispatch test order deterministic (first fix).
- `53d05584d` Fix same poll order race in unknown-group fallback test (second fix; the code head verified below).

## Verification (GitHub Actions, `rust-ci.yml`, `package=cadence-matching`)

- Run 40064815525 on `c1911c509`: FAILED. Not the original test. `matching_reader_pump_falls_back_to_default_for_unknown_isolation_group`
  panicked at `matching_reader.rs:2276` with `left: 2, right: 1`. Same race, in the sibling test. 646 passed, 1 failed.
  This run is the reason for the second commit. It is not a verification run for the final head.
- Run 40065023681 on `53d05584d`: success.
- Run 40065224222 on `53d05584d`: success.
- Run 40065401601 on `53d05584d`: success.

Run URLs:
- https://github.com/auenkr/cadence-rust/actions/runs/40065023681
- https://github.com/auenkr/cadence-rust/actions/runs/40065224222
- https://github.com/auenkr/cadence-rust/actions/runs/40065401601

## Notes

- No local cargo was used (per instructions). Verification is only from the GitHub Actions runs above.
- The push-triggered run on the branch (`paths: rust/**`) is separate from the three `workflow_dispatch` runs.
- Branch not merged.
