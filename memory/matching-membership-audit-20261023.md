# Matching membership / Ringpop parity audit (2026-10-23)

Branch `matching-membership-parity-20261023` (from master a028331fb). Rows: audit v3 "Ringpop bootstrap and membership",
"Task-list ring ownership and remote calls", "Matching startup/shutdown". Evidence:
`memory/evidence/matching-membership-parity-20261023/`.

## Compared (Go vs Rust, no difference found)

| Area | Go | Rust | Result |
|---|---|---|---|
| Hash fn / replicas | `hashring.go:53,117` farm.Fingerprint32, 100 replicas | `cadence-common/src/membership.rs:7,117` | same; short-input sign-extension handled |
| Replica key | ringpop `hashring.go` `replicaPointForServer`: `identity+idx` if identity==address else `identity#idx` | `membership.rs:118-123` | same |
| Tie order / wrap | points ordered hash, address, index; lookup first point >= hash, wrap | `membership.rs:105-141` | same; **new golden vectors** from Go (`ring_vectors.out`) |
| LookupN | unique servers walking ring | `membership.rs:146` | same (golden) |
| Duplicate address | `serverSet` first wins | `membership.rs:113-114` | same |
| Excluded short-lived names | `tasklist_differentiator.go:13-23` uuid regex OR hash%100>=pct | `matching_shard_routing.rs:305-326` | same |
| Ring refresh | `hashring.go:292-326` 1 s min interval (refreshed only on change), 2 s ticker, cap-1 signal, address-only diff, empty ring on Stop | `matching_membership_lifecycle.rs:19-131,179` | same |
| Hash identifier gauge | `hashring.go:360-396` | `matching_membership.rs:88-101` | same |
| GetMembers filter | `provider.go:203-260` serviceName label, Alive/Suspect, tchannel label else address port, grpc label | `matching_ringpop_provider.rs:163-222`, `matching_peer_provider.rs:29-65` | same |
| Identity | `provider.go:274-282` `__identity` label else address | `matching_ringpop_provider.rs:176-179,142` | same |
| Labels after bootstrap | `provider.go:145-168` | `matching_ringpop_provider.rs:354-359` | same order |
| Ringpop config | `ringpop.go` config Validate/bootstrap modes | `cadence-service-core/src/config.rs:318-394`, `ringpop_discovery.rs` (hosts/file/dns/dns-srv, custom rejected, 10 s default) | same |
| SelfEvict | `swim/self_evict.go:152-` Faulty, ping ceil(0.4*n) bounded by maxP | `ringpop_provider.rs:720-769` | same |
| Ownership error shape | `engine.go:1601-1660` TaskListNotOwnedByHost / "not known" | `matching_membership.rs:215-236` | same |
| Excluded-from-SD ring check | `engine.go:1648-1658` | `matching_runtime.rs:53-62` | same |

## Differences found and fixed

1. **Lookup failure message** (`engine.go:1649-1651`, `:1682-1684`): Go wraps `ErrInsufficientHosts` as
   `failed to lookup task list owner: ...` / `failed to look up the owner of semaphore bucket <id>: ...`.
   Rust returned the bare `Not enough hosts...` (`matching_membership.rs` `check_task_list`, `check_semaphore`). Fixed;
   raw `owner()` (Go `Lookup`/`getHostInfo`) stays unwrapped. Test `lookup_error_parity_tests`.
2. **Shutdown-drain flag timing** (`service.go:143-151`, `engine.go:206-208`): Go sets engine `shutdown` (ownership checks answer
   "not known", SD executor stops) only in `handler.Stop`, after EvictSelf and the drain sleep. Rust
   `MatchingRuntime::drain_with` (`matching_runtime.rs`) flipped the flag *before* the sleep, so requests for SD-managed task
   lists were rejected for the whole drain window. Fixed: flag set after evict+sleep. Test
   `drain_flag_flips_after_evict_and_sleep_and_evict_errors_are_ignored`.
3. **EvictSelf error** (`service.go:145` discards the error): Rust propagated it and failed shutdown (exit non-zero) both in
   `drain_with` and `launcher.rs` (`withdrawing Matching membership`). Fixed: logged to stderr, shutdown continues.
4. **Broadcast address rendering** (`provider.go:92-99,322-337`): Go uses `net.ParseIP` then `IP.String()`, so IPv4-mapped IPv6
   (`::ffff:10.1.2.3`) advertises `10.1.2.3:port`; Rust advertised `[::ffff:10.1.2.3]:port`, a different ring identity. Error
   text now Go's `failed parsing broadcast address "x", err: %!w(<nil>)` (Go formats a nil err). Test
   `broadcast_address_matches_go_parse_ip_and_string_rendering`.

## Known differences not changed

- Semaphore `isShuttingDown` rejection (`engine.go:1676-1680`): Rust `check_semaphore` has no drain input. After the drain the
  listeners are closed immediately by the launcher, so only the narrow window between handler stop and listener drop differs.
  Not a provable behavior bug; left.
- `matching_membership.rs` `remote_channel_for_owner` error for a peer without a gRPC label (`Unavailable`, Go: `port "grpc" is not
  set for ...` from `GetNamedAddress`) — wording only.
- Rust startup-failure cleanup path (`launcher.rs:1348-1380`) has no Go analogue (Go uses fx Fatal).

## Needs gate (live multi-node)

- Go Frontend/History discover Rust Matching and vice versa through the shared ring (checksum/identity/label gossip on a real
  Go ringpop node); existing two-node gates failed (exit 1/143) per audit v3 and are not rerun here.
- Peer death/restart convergence: FAULTY/TOMBSTONE timing, healer rejoin after partition, incarnation refutation against Go.
- Drain window behavior: Go peers stop routing to Rust Matching within `ShutdownDrainDuration` after FAULTY gossip; ownership
  rejection behavior of SD-managed lists during the (now correct) window.
- Broadcast-address / DNS / dns-srv bootstrap against real resolvers in k8s.
