# Matching configstore mixed acceptance

GitHub Actions run: https://github.com/auenkr/cadence-rust/actions/runs/40086531679

Source: e0aeae9e8995ffa3e55fca967032c866ec08e0a4. Conclusion: success.
Fresh target build and mixed gate exit codes are preserved beside this note.
The binary SHA256 is recorded in binary.sha256; command.txt preserves the command.

Topology: Go Frontend, History and Worker; Rust Matching only, using MySQL and
Go Ringpop discovery. Configstore long-poll expiry was seeded into Go's
cluster_config and verified via an empty direct poll to the discovered owner.
The lane also exercises SDK workflow completion, idle unload, two-node ownership,
SIGKILL recovery, graceful drain and process restart. Full probe output is retained.

The failed preceding run 40086025472 polled the wrong owner. This source fixes
routing while preserving the timing assertions. This pass predates pending
runtime-reader and Unleash lifecycle integration and is not final service sign-off.
The harness acceptanceEligible=false marker is Cassandra-specific and preserved.
