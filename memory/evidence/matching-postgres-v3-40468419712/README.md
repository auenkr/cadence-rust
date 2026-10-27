# PostgreSQL mixed validation on candidate v3

Run https://github.com/auenkr/cadence-rust/actions/runs/40468419712 succeeded
on `ff3b638b49190f7653c2de3b062232d7ccc57f07`, immutable tag
`matching-acceptance-20261027-v3`. Build, gate and runtime exits are zero.
Rust Matching binary SHA256:
`adb469745e34a617fb0377d69b4c7f40a79432a3d46eef3fbb7613a59fc7f678`.

Inspected actual runtime results: Go Frontend/History/Worker, Rust Matching,
no Go Matching; gRPC and production Ringpop routing. `scenario-results.json`
retains SDK workflow completion, graceful drain with queued rows recovered,
process restart with durable work completed, PostgreSQL task GC and metadata,
standby query forwarding, partition listing, and durable partition restart/drain.
Database cleanup output is retained. The artifact's `acceptanceEligible:false`
and preliminary labels remain unchanged: this is one lane, not service signoff.

Adaptive retry and cache-caller evidence remain open. A later production source
change requires final-source validation; this run proves only its recorded pin.
