# Production Matching domain cache wiring — source checkpoint (2026-10-19)

This is a bounded production integration checkpoint, not Matching completion.
The source changes attach the existing read-through cache to Rust Matching's
production runtime and consume Go's shared SQLite `defaultStore` metadata. The
initial cache scan and callback registration happen before the Matching handler
is constructed. Subsequent metadata refreshes update `InMemoryDomainStore`;
global non-active-active failovers advance its notification version and wake
eligible blocked pollers. Shutdown drains while listeners remain live, stops
Matching maintenance, unregisters the cache callback, then stops and joins the
cache refresh worker.

## Go SQLite configuration

`MatchingDomainCache::from_default_store` selects only the configured
`persistence.defaultStore`; it does not inspect another store, use a snapshot,
or fall back. This source supports file-backed SQLite only. Empty/memory stores,
NoSQL stores, other SQL plugins, unknown `connectAttributes`, and a literal `?`
in `databaseName` return explicit startup errors. The `?` boundary is because
Go's SQLite plugin builds `file:<databaseName>?<attrs>` without URL escaping,
so a question mark is already the URI query separator. Percent-escaped `%3F`
continues through SQLite's URI decoding. Relative names, leading `file:`,
spaces, `#`, and percent escapes are passed to SQLite unescaped, matching Go's
DSN construction.

The reader always opens `file:<databaseName>?mode=ro` with SQLite URI handling.
Configured `mode=ro`, `rw`, and `rwc` are accepted for file-backed stores, but
the read-only reader still requires an existing file; Go write-pool create/write
semantics are outside this adapter. `mode=memory` is rejected. Configured
`_pragma.busy_timeout` is parsed in milliseconds and caps each operation's
remaining Go-compatible three-second deadline. `_pragma.journal_mode` is not
applied by this SELECT-only adapter: Go config applies it on its read/write
connections, and the persisted database journal state is read as-is. `_timefmt`
is not used because Matching metadata timestamps are decoded from Thrift
DomainInfo blobs rather than SQL timestamp columns. All other explicit
attributes fail instead of being silently ignored.

The production Go probe creates a valid Go `sqlDomainStore` database for the
Rust Matching process and a separate Go domain-store oracle database containing
opaque malformed nested blobs. Rust raw persistence reads must preserve the
oracle bytes, while the Matching cache typed-conversion path must fail before
publishing a malformed scan. The live probe keeps a Go writer open after
ordinary and failover updates, requires non-empty SQLite WAL bytes, and waits
for the Rust cache to ignore the ordinary update but release a blocked Go
Matching poll on failover. The failover write remains open while Rust reads it,
covering live WAL visibility without close/checkpoint. It also checks that
explicit unsupported Cassandra config fails and no-config embedded Matching
still boots with `--data-dir`. It uses the Rust production process and has no Go
Matching server. It does not establish the required Go Frontend/History/Worker
mixed topology, SDK workflows, or restart/recovery parity.

The 2026-10-19 held-writer attempt failed its WAL assertion after native
Thrift passed. Existing `.ready` and `.release` marker files were reused from
earlier runs: a stale ready marker could skip waiting for the new Go commit,
and a stale release marker could let that helper close its SQLite pool before
WAL inspection. The harness now removes both markers before launch, verifies
the writer is still alive around Rust observations, and pins a pre-update
SQLite read snapshot so the committed frames cannot be checkpointed while
Rust reads them. The WAL assertion remains strict. This repair is source-only
until the serialized build slot is granted.

## Final bounded gate run (2026-10-19)

`memory/evidence/matching-production-domain-cache/attempts/20261019T200502Z-1851423/`
contains the fresh gate logs, exit codes, source hashes, diff hash, built binary
path, and ledger. Source HEAD at test time was
`6011effad649f9d6e5bc280f8cbf2cbc1e9426b6`. The tested diff SHA-256 was
`58457d4182de0eb892b38a4c02f6614046dae1911b741c92ffdb89686ef5623d`.
Every gate in `gates.tsv` passed:

- Persistence metadata tests: 10 passed, 1 ignored.
- Matching cache library tests: 18 passed.
- Explicit region-only failover notification regression: passed.
- Cadence server binary check and build: passed.
- Strict Clippy for persistence and server, all targets: passed.
- Go production probe: all native Matching Thrift methods, TLS/plain HTTP,
  embedded `--data-dir` startup, ordinary-update suppression, live Go WAL
  visibility, and Go poll release after failover passed.
- Ignored raw Go domain-store database interoperability test: passed against
  the separate `go-oracle.sqlite` database.
- `git diff --check`: passed.

The preceding attempt `20261019T200114Z-1846622/` is retained with the strict
Clippy failure from a redundant default update in the new regression fixture.
That fixture was corrected before the passing attempt, whose source hashes
identify the tested version. `run.sh` executes the gates serially with one Cargo
job in tmux window `cadence-rust:matching-domain-cache`; each attempt writes
logs and pending/running/passed gate states under a unique evidence directory.

These bounded gates validate this production cache wiring and storage boundary;
they do not establish Matching service completion. Unsupported SQL backends,
SQLite URI query syntax embedded in `databaseName`, the mixed Go
Frontend/History/Worker topology, SDK workflows, and failure/restart/recovery
parity remain open.
