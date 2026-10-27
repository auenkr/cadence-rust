# Matching History wire fixture reactivation

Source `47df125830183fc06c9980dbc428ac9ac1b26254` repairs the verifier's
pre-split cadence-server fixture path to cadence-matching. The fixture exists
and matches the reviewed SHA256 recorded in its companion Markdown file:
`de22e10620ffb4151deaf073c75c01f0f512bfda9ca30acdda265a2f237ad9bd`.

The Rust fixture consumer is now active in normal tests. `rust-ci.yml` first
regenerates the pinned Go ThriftRW bytes and compares them with the fixture.
The separate live Go peer helper also selects cadence-matching and respects
the supplied target directory instead of defaulting to an obsolete worktree.
That live helper was not executed; mixed Thrift acceptance remains required.

Validation: local shell syntax and git diff checks passed; no local Go/Cargo
commands ran. Actions https://github.com/auenkr/cadence-rust/actions/runs/40463643059
failed in the new Go comparison before Rust validation. The fixture generator
inherited wall-clock testdata timestamps and Go map iteration order. Repair
fb0cd3d08 fixes the testdata timestamps to the reviewed fixture's values and
compares all decoded Thrift values, ignoring map/set order only. Scalar bytes,
field IDs, container types, list order and absent-versus-empty values remain
significant. The fixture itself is unchanged. Focused Matching CI was dispatched
on master after the repair; no passing result is claimed yet.

Parent review of worker panic patch caccc07d4 is still open: the initial
implementation uses an approximate printable-Unicode exclusion list and
hex-escapes every nonprintable Latin-1 character. Go strconv escapes only
ASCII controls/DEL with hex and uses Unicode escapes for U+0085, for example.
Its printable classification must match pinned Go, including combining marks,
private-use/unassigned codepoints and nonbreaking space. Do not merge that
initial patch as complete parity merely because its NUL branch is corrected.
