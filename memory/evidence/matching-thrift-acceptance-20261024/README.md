# Mixed Thrift/TChannel deployment validation

Run https://github.com/auenkr/cadence-rust/actions/runs/40085137140 succeeded.
Source bfa303eeb9c0ba6c8b8b9f3f05c8cd98f68b44d9; Rust binary SHA256 f7069f236eb797dc7b23362e2730e188418bac4f71c63c2cd87ed5f196af44ac.
Fresh uncached target. Build/gate/run/probe exit codes are zero. Go
Frontend/History/Worker remained running with no Go Matching instance.
Matching transport thrift; Rust outbound History transport tchannel.

Probe evidence includes existing SDK workflow completion, two-node ownership,
idle recovery, crash recovery, process restart, graceful drain and AutoConfigHint.
This is one transport-lane pass, not a whole-service sign-off. Its source
predates metric migration and unfinished OpenFeature integrations; repeat final
required lanes after those changes. Harness acceptanceEligible=false is retained
because its formal acceptance marker is Cassandra-only. No Cargo ran locally.
