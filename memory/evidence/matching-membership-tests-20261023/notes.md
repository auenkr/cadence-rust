# Matching membership Go test port (2026-10-23)

Branch `matching-membership-tests-20261023` (from origin/master c5f95ea13). Scope: `go/common/membership/*_test.go` mapped to
Rust. No program logic changed; only test code added. Cargo was not run locally; verification is GitHub CI (see end).

## Step 1: Go test inventory

Source: `go/common/membership/*_test.go`, 41 top-level `Test` functions in 7 files. "Subtests" = `t.Run` cases or table rows.

| File | Go test | Subtests |
|---|---|---|
| hostinfo_differential_test.go | TestHostPortAndPortLabelOracle | table loops (no t.Run) |
| hostinfo_test.go | TestBelongs | 0 |
| hashring_test.go | TestDiffMemberMakesCorrectDiff | 5 |
| hashring_test.go | TestFailedLookupWillAskProvider | 0 |
| hashring_test.go | TestFailingToSubscribeIsFatal | 0 |
| hashring_test.go | TestHandleUpdatesNeverBlocks | 0 |
| hashring_test.go | TestHandlerSchedulesUpdates | 0 |
| hashring_test.go | TestFailedRefreshLogsError | 0 |
| hashring_test.go | TestRefreshUpdatesRingOnlyWhenRingHasChanged | 0 |
| hashring_test.go | TestRefreshWillNotifySubscribers | 0 |
| hashring_test.go | TestSubscribersAreNotifiedPeriodically | 0 |
| hashring_test.go | TestSubscribeIgnoresDuplicates | 0 |
| hashring_test.go | TestUnsubcribeIgnoresDeletionOnEmpty | 0 |
| hashring_test.go | TestUnsubcribeDeletes | 0 |
| hashring_test.go | TestMemberCountReturnsNumber | 0 |
| hashring_test.go | TestErrorIsPropagatedWhenProviderFails | 0 |
| hashring_test.go | TestStopWillStopProvider | 0 |
| hashring_test.go | TestLookupAndRefreshRaceCondition | 0 |
| hashring_test.go | TestEmitHashringView | 4 |
| resolver_test.go | TestSubscribeIsCalledOnPeerProvider | 0 |
| resolver_test.go | TestNewCreatesAllRings | 0 |
| resolver_test.go | TestMethodsAreRoutedToARing | 0 |
| resolver_test.go | TestNonExistingRingReturnsError | 0 |
| resolver_test.go | TestCallsAreForwardedToProvider | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Lookup | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Lookup_ExcludedFallsBackToHashRing | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Lookup_NilSpectatorFallsBackToHashRing | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Lookup_RecordsRoutingPath | 3 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Start | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Stop | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Subscribe | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_UnSubscribe | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Members | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_MemberCount | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Refresh | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_AddressToHost | 0 |
| sharddistributorresolver_test.go | TestShardDistributorResolver_Lookup_ExcludeShortLivedTaskLists | 3 |
| tasklist_differentiator_test.go | TestTaskListExcludedFromShardDistributor | 13 |
| tasklist_differentiator_test.go | TestIsbelowPercentage | 5 |
| percentage_onboarded_test.go | TestPercentageOnboarded | 3 |
| percentage_onboarded_test.go | TestStaticPercentageOnboarded | 0 |

Totals: 41 functions; 36 `t.Run` subtests (hashring 9, shard-distributor resolver 6, tasklist 18, percentage 3).

## Step 2: Go to Rust mapping

Rust homes: `cadence-common/src/membership.rs` (Ring, fingerprint32), `cadence-matching/src/matching_membership.rs`
(Membership snapshot, owner, change watch, metrics), `matching_membership_lifecycle.rs` (refresh/provider lifecycle),
`matching_shard_routing.rs` (shard-distributor resolver routing), `matching_history_membership.rs` /
`matching_history_peer_resolver.rs` (history ring), `matching_peer_provider.rs`.

Status legend: PORTED = added in this branch; COVERED = existing Rust test (name verified with grep); GAP = behavior differs (see
parity gaps); N/A = no Rust surface by design.

| Go test | Status | Rust test / note |
|---|---|---|
| TestHostPortAndPortLabelOracle | COVERED | `matching_history_membership.rs` `go_port_and_split_host_port_edge_cases_are_preserved`; `cadence-common/tests/ringpop_bootstrap_selection_oracle.rs` `capture_host_matches_go_split_host_port_matrix` |
| TestBelongs | MISSING | no Rust `Belongs`; not ported (outside membership focus) |
| TestDiffMemberMakesCorrectDiff (5) | PORTED | `go_diff_cases_report_change_only_for_address_set_differences`. Bool only; Go's added/removed lists are not exposed (gap G5) |
| TestFailedLookupWillAskProvider | GAP G1 | Go failed Lookup triggers provider refresh; Rust `Membership::owner` returns Status and does not refresh |
| TestFailingToSubscribeIsFatal | GAP G4 | Go `log.Fatal` (Goexit); Rust returns error. `startup_failures_stop_provider_and_start_is_idempotent` (lifecycle) covers the error path |
| TestHandleUpdatesNeverBlocks | N/A | Rust uses a `tokio::sync::watch` channel, non-blocking by construction |
| TestHandlerSchedulesUpdates | COVERED | `hint_inside_min_refresh_waits_for_two_second_ticker` |
| TestFailedRefreshLogsError | COVERED | `mutable_provider_updates_coalesce_and_failures_preserve_last_good` (log assertion absent) |
| TestRefreshUpdatesRingOnlyWhenRingHasChanged | COVERED | `failed_and_unchanged_snapshots_preserve_go_refresh_timestamp`; plus PORTED `go_subscribers_wake_only_when_placement_addresses_change` |
| TestRefreshWillNotifySubscribers | PORTED | `go_subscribers_wake_only_when_placement_addresses_change` |
| TestSubscribersAreNotifiedPeriodically | COVERED | `hint_inside_min_refresh_waits_for_two_second_ticker`, `closed_event_stream_keeps_periodic_refresh_alive` |
| TestSubscribeIgnoresDuplicates | N/A | no named subscriber registry in Rust (gap G2) |
| TestUnsubcribeIgnoresDeletionOnEmpty | N/A | gap G2 |
| TestUnsubcribeDeletes | N/A | gap G2 |
| TestMemberCountReturnsNumber | PORTED | `go_member_count_tracks_replaced_ring_hosts` |
| TestErrorIsPropagatedWhenProviderFails | COVERED | `mutable_provider_updates_coalesce_and_failures_preserve_last_good` |
| TestStopWillStopProvider | COVERED | `shutdown_self_evicts_before_stop_and_clears_placement` |
| TestLookupAndRefreshRaceCondition | PORTED | `go_lookup_and_refresh_race_never_observes_missing_owner` |
| TestEmitHashringView (4) | GAP G7 | `go_hashring_view_identifier_matches_go_emit_vectors`, `#[ignore]`. Existing `changed_ring_view_uses_go_gauge_scope_once` checks the gauge name and tags, not the value |
| TestSubscribeIsCalledOnPeerProvider | COVERED | `shutdown_self_evicts_before_stop_and_clears_placement` asserts order `start, subscribe, evict, stop` |
| TestNewCreatesAllRings | N/A | gap G3 (no named multi-ring resolver) |
| TestMethodsAreRoutedToARing | PORTED (partial) | Lookup part: `go_lookup_routes_key_on_two_member_ring`; MemberCount part: `go_member_count_tracks_replaced_ring_hosts`; unknown-ring errors are G3 |
| TestNonExistingRingReturnsError | N/A | gap G3 |
| TestCallsAreForwardedToProvider | COVERED | `shutdown_self_evicts_before_stop_and_clears_placement` (SelfEvict/Stop forwarding; WhoAmI has no Rust analog) |
| TestShardDistributorResolver_Lookup | COVERED | `eligible_route_uses_spectator_and_parses_named_ports_and_ipv6` |
| TestShardDistributorResolver_Lookup_ExcludedFallsBackToHashRing | COVERED | `excluded_and_missing_spectator_routes_use_live_ring_snapshot` |
| TestShardDistributorResolver_Lookup_NilSpectatorFallsBackToHashRing | COVERED | `excluded_and_missing_spectator_routes_use_live_ring_snapshot` |
| TestShardDistributorResolver_Lookup_RecordsRoutingPath (3) | COVERED | `policy_is_read_per_lookup_and_uuid_exclusion_precedes_spectator`, `failed_ring_lookup_still_records_go_routing_dimensions` (`routing_path` = `shard_distributor` / `hash_ring`) |
| TestShardDistributorResolver_Start / _Stop | MISSING | no Rust shard-resolver lifecycle surface; nearest `spectator_shutdown_before_start_is_terminal_and_wakes_waiters` (`matching_shard_distributor.rs`) |
| TestShardDistributorResolver_Subscribe / _UnSubscribe / _Members / _MemberCount / _Refresh | N/A | pure delegation to the ring in Go; Rust exposes `Membership::subscribe`, `hosts()` (gap G6) |
| TestShardDistributorResolver_AddressToHost | COVERED | `lookup_n_and_address_conversion_always_delegate_to_ring` |
| TestShardDistributorResolver_Lookup_ExcludeShortLivedTaskLists (3) | COVERED | `policy_is_read_per_lookup_and_uuid_exclusion_precedes_spectator` |
| TestTaskListExcludedFromShardDistributor (13) | COVERED | `go_farmhash_percentage_and_uuid_exclusion_vectors` (`matching_shard_routing.rs:466`); vector-based, not re-checked case by case |
| TestIsbelowPercentage (5) | COVERED | same vector test |
| TestPercentageOnboarded (3) | MISSING | gauge `percentage_onboarded_to_shard_manager`; `record_onboarding` exists but no value test. Not ported (outside focus) |
| TestStaticPercentageOnboarded | MISSING | trivial; not ported |

Counts (41 Go functions, one row each):
- PORTED: 5 (Diff, RefreshWillNotifySubscribers, MemberCount, LookupAndRefreshRace, and MethodsAreRoutedToARing partially).
- COVERED by existing Rust tests: 17.
- PARITY GAP with ignored test: 1 (EmitHashringView, G7).
- PARITY GAP without test: 2 (FailedLookupWillAskProvider G1, FailingToSubscribeIsFatal G4).
- N/A by design: 11 (HandleUpdatesNeverBlocks, 3 subscriber-registry tests, NewCreatesAllRings, NonExistingRingReturnsError,
  ShardDistributorResolver Subscribe/UnSubscribe/Members/MemberCount/Refresh).
- MISSING: 5 (TestBelongs, ShardDistributorResolver Start/Stop, TestPercentageOnboarded, TestStaticPercentageOnboarded).
Total 41.

## Ports added (cadence-matching/src/matching_membership.rs, module `go_membership_port_tests`)

1. `go_diff_cases_report_change_only_for_address_set_differences`: 5 Go cases, expected bool from Go `ChangedEvent` emptiness.
2. `go_member_count_tracks_replaced_ring_hosts`: 0 then 2 hosts.
3. `go_lookup_routes_key_on_two_member_ring`: Lookup("key") on {127,128} = 127 (same expected value as Go resolver_test.go:59-70).
4. `go_subscribers_wake_only_when_placement_addresses_change`: subscriber woken on address change, not on identity-only change.
5. `go_lookup_and_refresh_race_never_observes_missing_owner`: 50 lookups vs 50 replaces, owner always present.
6. `go_hashring_view_identifier_matches_go_emit_vectors` (`#[ignore]`): expects 835 (sorted and unsorted, 3 hosts) and 242 (empty).

## Parity gaps (Rust differs from Go)

- G1 `TestFailedLookupWillAskProvider` (hashring_test.go:165): Go failed Lookup triggers a provider refresh; Rust `Membership::owner`
  has no refresh trigger. No ignored test added: a faithful test needs the lifecycle fake provider (private to
  `matching_membership_lifecycle.rs`). To do next.
- G2 Named subscriber registry (`Subscribe`/`Unsubscribe` by name, duplicate and empty-delete semantics; hashring.go:223-268,
  hashring_test.go:372-402): Rust exposes one `watch` channel. Structural difference.
- G3 Named multi-ring resolver (`getRing`, unknown ring errors; resolver.go:177-220, resolver_test.go:53-120): Rust hard-codes one
  `Membership` per role. Structural difference.
- G4 Subscribe failure is fatal in Go (`log.Fatal`, hashring_test.go:185-215). Rust returns the error to the caller
  (lifecycle `startup_failures_...`). Process-level difference, intentional-looking.
- G5 `ChangedEvent` added/removed lists (hashring.go `diffMembers`): Rust `replace_if_addresses_changed` returns only bool.
- G6 Shard-distributor resolver pass-through surface (Start/Stop/Subscribe/Members/MemberCount/Refresh): no Rust equivalent.
- G7 `parity gap: hashring view identifier uses fingerprint32, Go uses farm.Hash32; Go hashring.go:360-396, hashring_test.go:463`.
  Go `emitHashIdentifier` calls `farm.Hash32` over the descending address list; Rust `replace_if_addresses_changed` calls
  `cadence_common::membership::fingerprint32` (Go `Fingerprint32`, which the ring uses). `cadence-persistence` already uses
  `farmhash::hash32` with Go-verified vectors, so the fix is likely a one-line switch to `farmhash::hash32`. Not changed (no program
  logic change allowed); the ignored test pins Go's values.

## CI

(filled after the run)
