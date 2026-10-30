# Matching startup stage test evidence (2026-10-30)

## Source and CI identity

- Instrumentation source: `b1f1b7204b086731e47c5425b47cfeb99f47b2f1`.
- CI wrapper commit: `683628e4e2f7051a803c0e14bbcc895e9e9ec730` on
  `t3/matching-startup-stage-diagnostics`.
- Earlier source-only run: [41035815248](https://github.com/auenkr/cadence-rust/actions/runs/41035815248)
  completed successfully on `5755021dbaaeeb70b87ec9ba3c69bd06065398e4`.
  Clippy, formatting, and workspace tests passed on that SHA. The downloaded
  small `matching-raw-string-oracle` artifact records that source SHA and
  `go-oracle.exit` = `0`. This does not validate the later startup-stage
  instrumentation.
- Requested fresh-target run: [41037120288](https://github.com/auenkr/cadence-rust/actions/runs/41037120288)
  failed on `683628e4e2f7051a803c0e14bbcc895e9e9ec730`. The failure was
  `Check changed Matching runtime formatting`, exit 1. `rustfmt --check`
  requires wrapping the startup-cleanup `eprintln!` at
  `rust/crates/cadence-matching/src/launcher.rs:1571`. Clippy, Formatting, and
  Test were skipped. No PostgreSQL production lane was dispatched because this
  fresh-source gate did not pass.

## Instrumentation review and marker map

The diagnostic commit only inserts fixed-message `eprintln!` calls and
refactors a few awaited results into local variables to log success. It does
not change branches, awaited operation order, lifecycle state, or admission
timing except for the time spent writing diagnostics. In
`matching_ringpop_provider.rs`, the lifecycle mutex guard acquired at
`start()` remains held through the new stage writes and provider startup until
the `Started` assignment. A blocked stderr sink can therefore lengthen that
critical section; this is a timing interaction to account for in a trace.

Startup interpretation, in source order:

1. `Ringpop provider lifecycle lock begin` -> await the provider lifecycle
   mutex. If `lock acquired` is absent, the wait is at this mutex. After
   acquisition, state checks and seed discovery lead into `join_cluster()` and
   `start_swim()`; the next new marker is `Ringpop ready`. Existing JOIN
   attempt diagnostics are needed to split a stall inside that interval.
2. `Ringpop ready` -> create stop channels, spawn the membership monitor ->
   `Ringpop membership monitor started`; configure/spawn the healer ->
   `Ringpop healer started`; build labels -> labels publication begin/complete;
   snapshot publication begin/complete; set lifecycle `Started` ->
   `Ringpop provider lifecycle started`.
3. Provider lifecycle started -> return to membership lifecycle and complete
   `provider.start()` -> subscribe begin/complete -> initial refresh begin.
   Refresh logs snapshot begin/complete, then membership publication
   begin/complete -> initial refresh complete -> spawn refresh worker ->
   `membership refresh worker started`.
4. Refresh error: snapshot begin may be the last refresh marker because
   `snapshot(...).await?` has no local failure marker. Caller then logs
   `initial membership refresh failed`, provider-stop begin/complete, and
   launcher startup cleanup begins. Provider-start error similarly logs
   `provider.start failed`, then calls `provider.stop()` without a local
   begin/complete marker.
5. `membership refresh worker started` -> launcher proceeds to
   `opening admission`, marks the Matching and runtime started, then logs
   `admission opened`; listener tasks are spawned afterward.
6. Startup-cleanup markers advance through membership withdrawal, runtime
   shutdown, membership teardown, and then abort/join the prebound TChannel
   task. If a cleanup begin lacks its complete marker, that awaited cleanup
   call is the boundary to inspect. Listener abort/join itself has no stage
   markers.
7. Normal shutdown trigger (signal, listener completion/failure, or fatal
   writer) is not labeled with its cause. The next markers are membership
   withdrawal begin/complete, runtime shutdown begin/complete, and membership
   teardown begin/complete; serving listeners are then aborted and joined.

For a mixed PostgreSQL trace, retain the `matching-production-postgres`
artifact: `out/gate.log`, `out/gate.exit`, `out/command.txt`,
`out/commit.txt`, and `out/binary.sha256`; include the nested gate run logs
(especially the Rust Matching server log) if present. `gate.log` is the
workflow wrapper output, while the nested run logs carry process stderr where
these markers appear. If no nested server log is present, retain the complete
`out/gate/` tree and the runner's `out/gate.log` before teardown. The current
rust-ci formatting failure means this runtime trace has not yet been
collected.

## Disposition

No production source changes were made in this evidence task. Rerun the fresh
CI gate after the one rustfmt-required wrapping correction; only if clippy and
workspace tests pass on the exact instrumentation source should the PostgreSQL
production lane be dispatched on that immutable CI head.
