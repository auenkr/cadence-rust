# Active offer mixed gate — run 40157002080

Run: https://github.com/auenkr/cadence-rust/actions/runs/40157002080  
Workflow source/head: `b1fda7fbff558f7f58b5c98cce9a563c93e9ecd9`  
Production Matching binary SHA256: `59e202b88c25ad43e0da27c13a1809235f1c88cb53b3105db86ec6fe3a58ef8b`

The build, lane, and probe exit files all contain `0`. The topology ran Rust
Matching with Go Frontend, History, and Worker; `goMatchingStarted` is false,
and the Go services remained alive after the probe. The gate command is in
`command.txt`; detailed machine-readable results and probe output are in
`gate/run/results.json` and `gate/run/probe.log`.

The SDK active-offer scenario passed: Go History scheduled a decision task,
Rust Matching delivered its task token, and the audit observed both zero
persisted task rows and zero task insert attempts. The same probe also records
query dispatch, isolation dispatch, idle unload/recovery, and graceful drain
recovery outcomes. The existing Go SDK workflow completed in this gate.

Preserve the probe's classifications exactly: `result` is
`preliminary-passed`, while `acceptanceEligible` is `false`. This is successful
scenario evidence, not service acceptance. Other requested dimensions such as
backlog forwarding, configstore, shard distributor, Cassandra-specific
acceptance, and the separate durable restart probe were not requested by this
lane. In addition, Matching remains INCOMPLETE pending the independently owned
gRPC TLS missing-identity startup parity fix and its revalidation, as well as
the complete parity audit.

## Workspace checkpoint

Fresh integrated workspace run:
https://github.com/auenkr/cadence-rust/actions/runs/40155846010  
Source/head: `1fc3a899c00c6eac5b2079b933092ca0df270313`  
Result: strict workspace clippy followed by workspace tests succeeded with a
fresh dedicated target. Workspace-format advisory remains `1`; focused required
formatting check is `0`. This is source-bound workspace validation, separate
from the mixed run above. Previously recorded backend, transport, configstore,
and shard-distributor lanes remain green only for their individually named
sources; after the TLS fix lands, the final workspace and mixed integration
lanes require revalidation on the resulting source.
