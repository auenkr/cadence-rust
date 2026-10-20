# Frozen two-host Matching recovery run

Command (from the `matching-dns-provider` worktree, inside tmux session `cadence-rust`):

```sh
NODE_PATH=/home/ubuntu/github/auenkr/cadence-rust/cadence-web/node_modules \
python3 tests/run-matching-production-failure-recovery.py \
  --rust-production-binary "$PWD/artifacts/frozen-production/cadence-server-679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa" \
  --artifacts-dir "$PWD/memory/evidence/matching-two-host-frozen-20261020/run"
```

The frozen binary SHA-256 is `679e0b0380da6dde7ee2291e3a0596bc26730d4af1ee950c24e185778b4254aa`. The Go Frontend, History and Worker ran with two Rust Matching members; no Go Matching process was started. The runner used gRPC for Matching-to-History and an idle check interval of three seconds. It compiled only the Go baseline server.

The run exited 1 in the later isolation compatibility section. After successful recovery and restart assertions, its direct Matching `AddActivityTask` call received `ABORTED` for task list `isolated-4752eac5-8b3f-40e8-ae93-9f4abc6b88e7`, with owner `127.0.0.1:60123` and responding host `127.0.0.1:33281`. See `run/rust-matching/production-failure-recovery-checkpoints.json` and `run/rust-matching/production-typed-ownership-go.log`.

The failure was initially mistaken for Go Frontend retry exhaustion. Inspection of the stack and harness showed it was instead a direct test call: the isolation section replaces its client with a direct primary Matching gRPC client, then sends `AddActivityTask` for an arbitrary task-list name which Ringpop assigned to the secondary. This path bypasses Go Frontend routing. The dedicated public non-owner and Go typed-error checks had already passed. The harness now selects a task-list name whose owner agrees across both Rust Ringpop views and targets that owner for its direct isolation-only calls; the SDK and workflow calls remain through Go Frontend. This correction is pending rerun evidence.

Passed checkpoints before that later failure:

- Both Rust Matching instances returned the same Ringpop ownership map for all 64 probes, including keys assigned to both instances.
- Production idle eviction released the long poll, and the existing Go SDK completed its recovery workflow exactly once.
- A request sent directly to the non-owner returned `ABORTED`; the pinned Go Matching oracle decoded the typed ownership error and verified owner, self and task-list fields. Its output is `run/rust-matching/production-typed-ownership-go.log`.
- The owner-crash recovery SDK workflow completed once; the persisted range advanced from 1 to 2, acknowledgement advanced from 0 to 1, and queued rows returned to zero. These details are in the checkpoint JSON.

The revised harness cleanup attempted both Rust Matching children independently. The outer runner stopped the Go services. A post-run process query for the Go baseline runner, compatibility probe, cadence-server services and Go typed oracle returned no processes; this was recorded in `process-cleanup.txt`. Full output is in `run.log`, `run/probe.log` and the service logs. `exit` records the expected failing gate status `1`.
