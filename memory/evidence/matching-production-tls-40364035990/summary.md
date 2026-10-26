# Production Matching TLS supporting evidence

Run https://github.com/auenkr/cadence-rust/actions/runs/40364035990 succeeded
on01148d6cf9c1e18a26e1e3385337d5f0c53bd534. Fresh runner build/gate/lane/probe
exits are0. CI-recorded binary SHA256 is in binary.sha256; executable was not
retained, so this hash cannot be independently recomputed from the artifact.

Actual topology: original Go Frontend/History/Worker and standalone Rust
Matching, no Go Matching. Artifact development.yaml config enables host
verification with localhost server name and CA; Rust config has TLS certificate.
The pinned Go client decoded typed ownership error over the verified TLS path.
Recorded scenarios include both-node ownership agreement, idle recovery with
one workflow completion, SIGKILL takeover with range/ack progression and zero
queued rows, and SIGTERM exit0. See compact scenarios.json and original artifact
at /mnt/storage/service/ci-evidence/40364035990.

The run explicitly retains acceptanceEligible:false. It predates fatal writer
lifecycle integration, so rerun on final combined source; this does not establish
Matching completion or mutual-TLS coverage beyond this lane's configured mode.
