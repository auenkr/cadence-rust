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

## Selector repair follow-up

The bounded repair compares Rust capture parsing against a Go-generated
`net.SplitHostPort`/`util.CaptureHost` matrix, including empty ports/hosts,
IPv6 zones, malformed brackets and colons, and nonnumeric ports. The fixture
now stores raw local/seeds/requested options alongside resolved Go values, and
the Rust oracle consumes these raw fields directly. Group checks cover exact
capacity, candidate multisets, preference ordering, partial rounds, exhaustion
and reset; option values use signed 64-bit values and Go-style wrapping for the
selection cap.

Two original-Go verification runs and the host/port matrix passed. Logs are in
`selector55/go-run-1.log` and `selector55/go-run-2.log`; `go.exit` contains 0.
The follow-up validation is complete in `selector56/validation.log` with final
exit `0` in `selector56/validation.exit`. It ran the Go oracle twice with no
fixture drift, all four Rust differential tests, and strict locked all-target
Clippy for `cadence-common`. Rust used the completed shared target at
`/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target` with one build
job. The fixture's `seeds: null` case is decoded as an empty seed list; its
recorded size is the Go selector's effective pre-`Init` cap, so the test checks
`effective_size` directly rather than comparing it to the uncapped Rust option
or recalculating after joined members are filtered.

## Selector signed-cap correction (2026-10-18)

The prior Rust conversion mapped a negative wrapped cap to zero. Original Go
`SelectGroup` stops only when `len(group) == cap`; a negative cap is never
reached, so selection drains the available preferred/nonpreferred round pool.
Rust now preserves this behavior, while zero stops immediately and a positive
cap selects up to that exact count. The checked fixture has 16 cases generated
by pinned original Go, including both negative operand combinations, joined
count equal to or above effective size, and a multiplication overflow whose
wrapped cap is negative. Assertions use the source preference pool because Go
preference classes may still contain already joined targets.

Reproduce with
`memory/evidence/ringpop-bootstrap-selection/selector57/run.sh`. It generates
the checked fixture, verifies it twice against Go, runs the four focused Rust
oracle tests, then strict locked all-target Clippy for `cadence-common`. All
five exit markers are `0`; output is in `selector57/`. Rust used
`CARGO_BUILD_JOBS=1` and the idle shared target
`/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target`.

Ordered source commits applied to this selector state:

```text
ac195cd5138c8e563d32364746f5c076cdd63b82 candidate selector implementation
0a63bbd6d2ccfaea7040ab9abb0cdc2424b01d76 oracle repair and raw-input cases
565a68483c58863c37ef9d8bc6a652af5966bc60 focused validation evidence
```

This correction is bounded selector work and does not establish Matching
service completion or network bootstrap parity.

SHA-256 for the corrected implementation and final run logs:

```text
008f45948a4b60d3b232eb839b80f965339ccca7fa175492c1e6d0c48e928134  rust/crates/cadence-common/src/ringpop_bootstrap_selection.rs
9563ed28a13e5f2b800645d2ace7880cb13ec842d7c0dafc246fe7aba32b229d  rust/crates/cadence-common/tests/ringpop_bootstrap_selection_oracle.rs
c3bf9545db52b5e6f2d45d49513f72fcdc6ac198278445849341e71e66083e09  tests/ringpop-bootstrap-selection-oracle_test.go
52f6a35e6be53b2c592e9e32f8f52c4e7621733fcbff9df7ff16172e8f55e9ca  tests/run-ringpop-bootstrap-selection-oracle.py
e618fe380421f4c395078608bb62aee1f47ee6c7d6dad1293632da5c2f56eba1  tests/fixtures/ringpop_bootstrap_selection.json
7ed31e538c03a781a955a4547c0469d5dfc079bb7070b1468c8b11c1122a4384  selector57/common-clippy.log
5af02d55281a8f2491d5143dd14c3d4997511005745db21977e8a1503bd8fe60  selector57/go-generate.log
ffe5b3afc838270dd76ab0f6a0b972c947cfe2006ceeabd7ec700817a9ae849a  selector57/go-run-1.log
b17512084134bed1bf854c5fff5f1a3bc6f8c0e26ba131cb40b9f259d8528a3e  selector57/go-run-2.log
d47d20c5fabdbd0df3c649b13543fbca919ecf2344349caf5adcacfcce33edc2  selector57/rust-focused.log
```
