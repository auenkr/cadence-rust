# Matching MySQL extended probe acceptance

Run: https://github.com/auenkr/cadence-rust/actions/runs/40089419226
Source: ae442535c1d4ed708daeb38face1ab0e9d84f88a. Conclusion: success.
Fresh target build and mixed gate exit codes, exact command, binary SHA256,
results and full probe output are retained here.

Topology: original Go Frontend/History/Worker with Rust Matching only, shared
Go MySQL schema and Ringpop discovery. The lane enables backlog forwarding,
query standby, partition listing, durable partition restart/drain and
AutoConfigHint probes, plus SDK workflows, idle recovery, owner SIGKILL and
process restart/drain. Consult probeScenarios in the retained probe output for
actual observations. Existing integrated workspace run40089398117 also passed
this source, including typed runtime reads and Unleash lifecycle.

This predates pending metrics/backoff integrations; final candidate acceptance
and requirement audit remain open. The harness Cassandra-specific
acceptanceEligible=false marker is preserved.
