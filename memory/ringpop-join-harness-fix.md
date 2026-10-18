# JOIN compatibility runner probe freshness fix

`tests/run-ringpop-outbound-join-compat.py` previously reused
`/tmp/cadence-ringpop-join-probe` whenever that path existed. This allowed an
older executable without `JoinGroup` support to run and fail with a misleading
invalid socket address error when `--group` was parsed as a seed.

The runner now builds the `ringpop_join_probe` example from the checkout on
every default invocation, checks that Cargo produced an executable, and copies
it into a unique temporary directory for the Go peer run. It removes that copy
when finished. `RINGPOP_JOIN_PROBE` remains available as an explicit
caller-owned override; the runner does not rebuild or replace an override and
fails early unless it points to an executable file. The other probe runners
were left unchanged.

## Reproduction and evidence

Successful full run executed in `tmux` session `cadence-rust`, window
`probe64`, with `CARGO_BUILD_JOBS=1` and the inactive completed
`t3-975b4b2d/rust/target` cache:

```sh
CARGO_BUILD_JOBS=1 \
CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-975b4b2d/rust/target \
python3 tests/run-ringpop-outbound-join-compat.py
```

That invocation freshly built the example from the current checkout, then ran
both tests against real, pinned Go Ringpop v0.10.0 peers:

- `TestRustOutboundJoinAgainstGo`: PASS
- `TestRustOutboundJoinGroupAgainstGo`: PASS
- Python runner exit: **0** (`memory/evidence/ringpop-join-harness-fix/probe64/go-peer.exit`)
- Cargo build: **0**, `Finished dev profile` (`go-peer.log`)
- Fresh Cargo target artifact SHA-256:
  `ce77bbaf2553a9ae95440af59e96c015129ab2c0e32b9efe51978eb1f72f4063`
  (`built-probe.sha256`)
- Example source SHA-256:
  `2c6e353c794974ba065c1ef7e524e36a8f2889835d123352fcf525727392ab81`
- Invalid explicit override check: exit **1**, with the expected
  `RINGPOP_JOIN_PROBE must name an executable file` message
  (`override-invalid.exit`, `override-invalid.log`).

An earlier build was interrupted with exit 130 while another Cargo test was
active; subsequent process inspection showed that test used the separate
`t3-e426148a` target, not the 49 cache. Its log and exit marker are in
`memory/evidence/ringpop-join-harness-fix/probe64-initial/`. The runner was then
rerun after confirming the 49 cache had no active Cargo process. The separate
63 restart activity used the 53 cache and its window/cache/ports were not touched.
