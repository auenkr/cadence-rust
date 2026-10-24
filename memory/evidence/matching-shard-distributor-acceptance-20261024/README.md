# Matching shard-distributor mixed acceptance

Run: https://github.com/auenkr/cadence-rust/actions/runs/40087044393
Source: 14953932ff68769cb00ba7e907b8d9df630fae66. Conclusion: success.
Fresh target build and mixed gate exit files, command, binary SHA256 and full
probe output are preserved here. Go Frontend, History and Worker communicate
with Rust Matching only, on MySQL with the real Go shard distributor.

The lane exercises registration/heartbeat and assignment ownership alongside
SDK workflows, idle recovery, SIGKILL recovery, graceful drain and restart.
Inspect the shardDistributor object in gate/run/probe.log for exact observations.
This predates runtime-reader and Unleash lifecycle integration; final candidate
validation remains required. The Cassandra-specific acceptanceEligible marker
is preserved and does not establish full service parity.
