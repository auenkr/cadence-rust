# Ringpop bootstrap selection oracle

This is a narrow executable oracle for seed normalization, option selection,
partitioning, and `SelectGroup` only. It calls the original unexported routines
in `github.com/uber/ringpop-go v0.10.0` through a Go overlay. No live bootstrap,
network, retry, readiness, or failure/restart cases are included.

From the repository root, reproduce it with:

```sh
python3 tests/run-ringpop-bootstrap-selection-oracle.py
```

The runner requires the pinned module cache at
`/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0`, copies that module
to a temporary directory, applies `tests/ringpop-bootstrap-selection-oracle_test.go`
as an overlay test in `package swim`, and removes its temporary copy on exit.
The test emits `tests/fixtures/ringpop_bootstrap_selection.json` and the runner
checks that the output identifies the pinned implementation and has all nine
cases. Membership and selection results are sorted for serialization; runtime
assertions inspect preference/cap invariants without assuming Go map iteration
order.

The prior `memory/ringpop-bootstrap-next.md` says identical hostports collapse.
The pinned `util.HostPortsByHost` at v0.10.0 actually appends each input without
de-duplication. The oracle records this: an identical seed appears twice in
`bootstrapHosts`, `potential`, the same-host class, and the selected group.
Likewise `Init(joined)` filters `potentialNodes` through `CollectPotentialNodes`,
but `CollectPreferredNodes`/`CollectNonPreferredNodes` do not filter joined
addresses; the oracle records potential exclusion and the selection helpers'
actual classes separately. Rust parity should follow whichever behavior is
confirmed against this pinned source, not the prose claim in the earlier note.

## Recorded run

Executed in tmux session `cadence-rust`, window `oracle51-gofmt` on 2026-10-18.
Exit status: `0` (`oracle51.exit`). The Go output is in `oracle51.log` and the
normalized fixture is in `tests/fixtures/ringpop_bootstrap_selection.json`.

Pinned source hashes:

```text
b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6  swim/join_sender.go
3f5c41720b80c0a2897ea06469ab8e83f31ef8a5f6d09c820066fd90591a643f  util/util.go
```

Committed artifact hashes:

```text
125452035aa969e950d4d82fe14b1a9daf8666db71e49e80577e588a87fb75a8  tests/ringpop-bootstrap-selection-oracle_test.go
74418237470e17b202ab43101d66b75cc4b8af0ccce05b6158c5244dff6f6cdf  tests/run-ringpop-bootstrap-selection-oracle.py
9f20b24463379d6a16a0331d68c7960b032c82b06071051aef258be0fc49baf7  tests/fixtures/ringpop_bootstrap_selection.json
cdd8f5cc498870236a96c35b467f46f94b6e561fa3b678895e153bc63779cd16  oracle51.log
9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa  oracle51.exit
```
