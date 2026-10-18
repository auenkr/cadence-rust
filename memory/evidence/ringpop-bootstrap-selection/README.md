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
Default runner mode verifies the checked fixture and does not change it. To
regenerate intentionally, pass `--generate`. Membership and option fields are
compared after normalization; selected groups are checked for eligibility and
valid preference-class membership, not exact subset or order, because Go map
iteration and randomized `TakeNode` make those unstable. Eleven cases include
duplicate seeds, joined filtering, defaults/signed options, exhausted rounds,
IPv6, and hostnames.

The prior `memory/ringpop-bootstrap-next.md` says identical hostports collapse.
The pinned `util.HostPortsByHost` at v0.10.0 actually appends each input without
de-duplication. The oracle records this: an identical seed appears twice in
`bootstrapHosts`, `potential`, the same-host class, and the selected group.
Likewise `Init(joined)` filters `potentialNodes` through `CollectPotentialNodes`,
but `CollectPreferredNodes`/`CollectNonPreferredNodes` do not filter joined
addresses; the oracle records potential exclusion and the selection helpers'
actual classes separately. The Rust primitive follows these pinned behaviors.

## Rust consumer and recorded run

The actual Rust consumer is
`rust/crates/cadence-common/tests/ringpop_bootstrap_selection_oracle.rs`; it
exercises `cadence_common::ringpop_bootstrap_selection` against the checked Go
fixture. Run it and strict all-target clippy from `rust/`:

```sh
CARGO_BUILD_JOBS=1 cargo test --locked -p cadence-common --test ringpop_bootstrap_selection_oracle
CARGO_BUILD_JOBS=1 cargo clippy --locked -p cadence-common --all-targets -- -D warnings
```

Recorded in tmux `cadence-rust`, window `selector52` on 2026-10-18. The
reproducible command output and final status are in `selector52.log` and
`selector52.exit`. Both original-Go verification runs returned 0 without
fixture drift; focused Rust and strict all-target clippy statuses are in that
log. This bounded primitive covers no network, retry, readiness, or
failure/restart behavior.

Pinned source hashes:

```text
b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6  swim/join_sender.go
3f5c41720b80c0a2897ea06469ab8e83f31ef8a5f6d09c820066fd90591a643f  util/util.go
```

Final selection implementation and evidence hashes:

```text
8e5e74d2b2ce7d8ba64535e6b4b73c2d699bc803a1b9d2ca6430a3dcb27450fd  rust/crates/cadence-common/src/ringpop_bootstrap_selection.rs
6d0692db4e7b0800a2e26ae538d8ad4c256538b44a0d573a813378273b25e03c  rust/crates/cadence-common/tests/ringpop_bootstrap_selection_oracle.rs
5b7b703b2f591e20e67fdce50711fa8c959087f84c0c371fa9df4581367a272e  tests/ringpop-bootstrap-selection-oracle_test.go
8a9f60d27a2137c4beb1535108b2cf837a27f05ed2a8bc5d79072ff9d4a7eb47  tests/run-ringpop-bootstrap-selection-oracle.py
b7bbe79e4a5dd166598357f5e2731549e5af04c087688b4b34f3fc56e6f037d8  tests/fixtures/ringpop_bootstrap_selection.json
947a417cc3d6431992d5a13fe0221a870ebe106ddfded34c5c157437c3f46b74  selector52.log
9a271f2a916b0b6ee6cecb2426f0b3206ef074578be55d9bc94f6f3fe3ab86aa  selector52.exit (contains 0)
```
