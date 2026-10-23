# Resume checkpoint: backlog v3 implementation, validation waiting

Worktree /mnt/storage/service/github/wt/backlog2; branch matching-backlog-forward-v3-20261023, based on 4f5fb6722. Latest implementation commit is 373b97be5 (startup-order fix). Everything is pushed through that commit; no Go source changes. Read implementation.md and policy-update.md.

Implementation includes Go MustOffer backlog forwarding, shared sync/backlog rate and request limits, two-second failure retry/local fallback, typed busy/ForwardError handling, child ack+GC on forward success, correct wire kind and Go persisted schedule-timeout semantics, safe local-offer withdrawal, and forwarder installation before restored-reader startup. Added reader, transport and two-partition tests are not yet fully validated.

Actual initial statuses are preserved in initial/: check 0; clippy 101 (type-complexity failure subsequently fixed with aliases); test 143 (deliberately stopped because source edits made this build stale; no executed-test result claimed). The obsolete queued check only waited for Cargo's build-directory lock and was stopped under the new policy. Its artifacts are in obsolete-check/; they do not describe the latest implementation.

No Cargo process from this worker is active. Validation is waiting for host availability: no mixed gate, no other cargo, load <=6 and MemAvailable >=2 GiB. Other workers have the shared /tmp/cargo.lock queue; do not add a third waiting build. Poll no faster than every 60 seconds. Do not use cargo-1.lock or cargo-2.lock, and do not run separate cargo check.

Prepared final script: /tmp/backlog-v3-serial-gate.sh. It exits 75 without building if initial readiness checks fail. When eligible, launch in tmux session cadence-rust; it runs:

    export CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared
    flock /tmp/cargo.lock cargo clippy --workspace --all-targets -j3 -- -D warnings > clippy.log 2>&1; echo $? > clippy.exit
    flock /tmp/cargo.lock cargo test --workspace -j3 > test.log 2>&1; echo $? > test.exit

Full log paths are in this directory; commands execute from worktree/rust. The script records source.sha and UTC start/finish timestamps. It waits at least 60 seconds between readiness checks before tests. Do not finish while this worker's actual jobs are running. Fix any failures and repeat necessary checks. /tmp/backlog-evidence.py generates validation.md from the final actual statuses and aggregate test summaries after finished.txt exists. Review that report before committing it. Commit/push final evidence with Co-Authored-By: Codex <noreply@openai.com>, then report SHA, real exit codes, test counts and gaps. No mixed Go/Rust or multi-host gate was run by this worker.
