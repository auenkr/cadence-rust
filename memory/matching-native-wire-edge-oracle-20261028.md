# Matching native wire edge oracle — 2026-10-28

Status: **implemented, unverified**. The Go oracle and native Rust integration
test are authored, but no Go, Cargo, rustfmt, or CI command was run in this
worker turn. Parent must run the configured CI workflow and record its run URL,
head SHA, and conclusion before treating these cases as verified.

`tests/matching-raw-string-oracle.go` retains its top-level JSON array and all
existing row keys. It adds `nativeWireEdgeCases` only to the first row. Each
case's wire bytes are independently assembled from tags, lengths, and values,
then decoded using the pinned YARPC protobuf codec backed by gogo. The recorded
result includes input hex, Go error text and concrete error type, scalar and
nested byte fields in hex, nested-message and duration presence, and map
entries as hex pairs (with no promised ordering).

Cases cover duplicate scalar strings (including a wrong wire type after a
valid value), raw map keys `ff` and UTF-8 replacement-rune bytes, omitted map
key/value defaults, repeated workflow-execution submessages, empty and zero
nested duration presence, unknown fields, truncated varint/length, and a
wrong-wire scalar. The Rust integration test consumes the fixture from
`MATCHING_GO_RAW_STRING_ORACLE`, compares success/error outcomes and the
recorded decoded values/presence, and sorts map pairs before comparison. It
does not predict protobuf-v2 behavior or weaken expectations to match Rust.

Potential defects remain open until CI executes the oracle and test: pinned Go
may expose an additional error/partial-decode behavior, or the Rust native
decoder may disagree on malformed inputs, map defaults/collisions, nested
merge, or presence. Any such assertion failure is evidence of a compatibility
gap; do not resolve it by changing these Go-derived expected values. This
worker did not modify production sources.
