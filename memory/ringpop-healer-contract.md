# Ringpop v0.10.0 healer and reverse full-sync contract

Reference: `github.com/uber/ringpop-go@v0.10.0/swim` (`node.go`,
`heal_via_discover_provider.go`, `heal_partition.go`, `ping_handler.go`,
`disseminator.go`).

## Discover-provider healer

- Start after successful Bootstrap/Join and SWIM start; stop on node shutdown.
- Default period: 30 seconds. Default base probability: 3.
- Each tick first waits one full period, then attempts with probability
  `base / max(previous_discovery_host_count, reachable_member_count, 1)`.
- Refresh `Hosts()` only on a selected attempt. Failed discovery is a failed
  tick; successful reads replace the saved host-list size.
- Candidate addresses are discovery results absent from membership or whose
  status is Faulty-or-worse. Shuffle candidates. Try targets until exhausted
  or 10 failed JOIN/repair calls. On success, remove every address known on
  the remote side from the remaining candidate list.
- `AttemptHeal` is a JOIN request with a one-second timeout, without applying
  returned membership automatically. Compare both memberships. Where one
  partition's winning change would demote a currently pingable member to
  unpingable, send that winning change as Suspect with source fields cleared
  to the opposite partition and apply the local-side changes. Otherwise merge
  remote membership locally and send the complete local membership in a PING.
- Return pingable remote membership addresses to candidate filtering.

## Reverse full sync

- Receiver ping handling supplies the normal receiver changes/full-sync to the
  current caller immediately.
- If receiver `issue_receiver` selected full sync, asynchronously JOIN the
  ping source with a one-second timeout and merge that JOIN membership locally.
- Bound concurrently running reverse jobs to five; omit an additional job
  when full. Shutdown owns and drains/cancels these tasks.

## Rust implementation/evidence boundary

The Matching provider owns the discovery loop and its lifecycle. The Ringpop
node owns membership repair, receiver full-sync scheduling, and reverse job
limits. Differential tests should compare deterministic candidate order,
probability divisors, reincarnation deltas and merged memberships with Go
oracle data; a two-partition protocol test must demonstrate convergence after
healing and reverse full sync. Builds are deferred while the shared task-store
build lane is occupied.
