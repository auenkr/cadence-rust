# Matching membership + standby completer regressions (2026-10-27)

UNVERIFIED: nothing was run locally. The parent dispatches the Actions gate (`gh workflow run rust-ci.yml --ref <branch> -f package=cadence-matching`).

## New tests (`rust/crates/cadence-matching/src/matching_membership_go_tests.rs`, registered in `grpc_matching.rs` as `membership_go_tests`)
| Rust test | Go row | Behavior pinned |
|---|---|---|
| `membership_change_keeps_owned_semaphore_bucket_and_stops_moved_one` | `go/service/matching/handler/membership_test.go` `TestShutDownNonOwnedSemaphoreManagers` | Two-host ring. The owned bucket stays registered and serves. The moved bucket is stopped and unregistered. |
| `membership_lookup_failure_unloads_no_semaphore_bucket` | `...SurfacesALookupFailure` | Empty ring makes `owns` fail. Nothing is unloaded, and the loop returns Ok. |
| `semaphore_ownership_check_distinguishes_lookup_error_from_not_owned` | `TestGetTaskListManager_OwnerShip` LookupError row (semaphore bucket) | A lookup failure is Internal "failed to look up the owner of semaphore bucket ...". A bucket owned by another host is Aborted with OwnedBy/Me. |

Production under test: `grpc_matching.rs` `maintain_membership_until` and `check_semaphore_ownership`. No production change.

## WhoAmIError row: seam limitation
Rust `Membership::identity()` is an infallible string set at construction, so there is no WhoAmI call that can fail. The row has no Rust analog (as in the function map: MOCK). A test would have to mirror a fake. Not added.

## Task completer error rows (domain cache / History failure)
- Go policy `common.CreateTaskCompleterRetryPolicy` (`go/common/util.go:232`) has 1s initial, 10s max and 300s expiration. Rust `matching_standby.rs` now builds it in `completer_retry_policy()` (a pure extract of the old inline policy, no behavior change). Equal.
- New test `standby_completer_retry_policy_backs_off_then_gives_up_after_expiration` drives `compute_next_delay` with explicit elapsed times. It checks the 1,2,4,8,10... backoff with 80-100% jitter, giving up by 300s, and `None` past 301s.
- Limitation: `cadence_common::Retrier` reads `std::time::Instant`. A tokio paused-time test of `checker` would auto-advance sleeps while the 300s expiration never elapses, so it would spin. A deterministic end-to-end exhaustion test needs a clock seam in `cadence-common/src/backoff.rs` (outside this task's ownership), or a 5-minute wall-clock sleep (rejected).
- Existing evidence for terminal behavior: `exhausted` in `standby_completion_failure_excludes_not_started_but_counts_young_missing` (error returned, failure metric counted for History/cancel/deadline errors) and `standby_cancelled_checker_counts_one_completion_failure` (domain-cache failure keeps retrying, cancel counts once).

## CI selectors
`cargo test -p cadence-matching membership_go_tests` and `cargo test -p cadence-matching standby_completer_retry_policy`.
