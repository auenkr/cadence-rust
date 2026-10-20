# History TChannel wire oracle

The reviewed Go-generated JSON fixture is intentionally pending. Do not create
it by hand. Run `scripts/verify-history-tchannel-wire-oracle.sh` with Go 1.25.14
to generate a fresh observed file, review the bytes, then copy that output to
`history_tchannel_wire_oracle.json`. The script compares every future run to
that checked-in file and never overwrites it.

Until the fixture is checked in, the Rust consumer is ignored. Its expected
names cover five Args wrappers, five Result wrappers (including the empty
void result), seven exception Results, and present-versus-absent header, input,
queries, and version histories.
