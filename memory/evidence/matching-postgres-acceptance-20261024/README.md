# PostgreSQL mixed deployment validation

Run https://github.com/auenkr/cadence-rust/actions/runs/40084530178 succeeded.
Source 1dd23e8a5d987e9a8bebfa9de00f62665a6fab97, fresh uncached target,
binary SHA256 90bcca0fac50d73beec7d0d1ad3b4904371661df356fc5dc5cacb5fd22875452.
Go Frontend/History/Worker remained running, no Go Matching instance.
Build, gate, run and probe exited 0.

Inspected passing scenario JSON for PostgreSQL GC/domain metadata, query standby
forwarding, partition listing/owner agreement, durable partition restart/drain,
AutoConfigHint. Failure-recovery checkpoints cover idle eviction, two-node
ownership, crash recovery; probe result includes existing SDK workflow and
process restart with exactly one completion. The corrected listing fixture
keeps its three-partition config scoped to the tested roots; crash persistence
assertions are unchanged. Listener reservations avoid ephemeral source ports.

Limits: this is one successful PostgreSQL deployment lane, not a whole-service
completion sign-off. Harness results retain acceptanceEligible=false because
its formal acceptance marker is Cassandra-only. Unmerged OpenFeature/metrics
changes are not validated by this run. Repeat required lanes on final candidate.
Next transport lane: https://github.com/auenkr/cadence-rust/actions/runs/40085137140
(thrift, pending). No local Cargo or mixed gate ran.
