# Matching native identifiers and RunID checkpoint

The previous focused common/API/persistence/proto CI **passed** at b2fccee5c:
https://github.com/auenkr/cadence-rust/actions/runs/40579351727
Compact source identity, exit codes, Go oracle and executed-test summaries are
in `memory/evidence/matching-raw-token-blob-40579351727/`.
This validates task-token JSON against the real original Go serializer and
the additive raw blobs. It does not validate newer RunID/native codec changes.
The tasktoken/go_string portions of the runner format patch are applied;
matching_blob formatting is deferred until concurrent source work is finished.

New parent source: `TaskListNameBytes` parses and routes arbitrary-byte roots
and preserves original bytes in invalid-name errors. The existing UTF-8
TaskListName::parse delegates to that parser, keeping its public API. Tests
exercise ff/fe/replacement rune/NUL roots, optional plus sign, bad suffixes,
overflow and parent routing. The Go oracle now emits actual NewIdentifier
root/partition/error/parent outputs for both raw roots and partitioned names,
and Rust compares them, including Go integer-overflow behavior.

The completed RunID worker adds raw backend resolution on TaskDataBytes and
preserves original bytes in SQL fatal error data. Parent extended the oracle
to pinned google/uuid and gocql ParseUUID; Rust compares accepted bytes and
failure classifications. Empty SQL remains absent while empty Cassandra
remains invalid. Cases include invalid UTF-8 UUID wrapper bytes and all 256
byte values. New source and expanded oracle still need CI.

Two workers still own native protobuf generator/codecs and decision Thrift
adapters. Do not commit partially written files from those tasks. No local
Cargo, Go, rustfmt or mixed gate ran. Matching is still incomplete; the next
immutable candidate and all same-pin acceptance gates are still required.
