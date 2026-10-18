# Ringpop static and file discovery slice (2026-10-18)

Implemented a separate Rust API in `cadence_server::ringpop_discovery` for
Cadence Matching seed discovery. `StaticHosts` returns configured entries in
order without trimming or deduplicating. `JsonFile` rereads on each call and
decodes exactly a JSON string array. Read and decode failures remain distinct
`Io` and `Json` errors. `Provider::from_config` selects `hosts` and `file`;
DNS, DNS-SRV, and custom are explicit unsupported modes here. Nothing is wired
to readiness, gossip, or main runtime.

`RingpopConfig` now maps the Go YAML fields, case-insensitive bootstrap mode,
signed duration nanoseconds, and Go validation strings, including zero's
10-second default. The actual-Go oracle is
`tests/run-ringpop-bootstrap-provider-oracle.py`; its checked-in result is
`tests/fixtures/ringpop_bootstrap_provider.json`. It calls Cadence's factory,
which selects the pinned Ringpop v0.10.0 providers. The Rust consumer test
loads that fixture. The oracle covers exact config validation messages,
default and signed/edge `time.Duration` mapping (including `i64` bounds),
seed strings, error types, and file rereads after replacement. File cases
include empty arrays, missing files, malformed JSON syntax, valid arrays,
object-versus-array errors, duplicates, and replacement between reads.
The Rust error API keeps file IO, JSON syntax/EOF, and JSON data/type failures
as separate variants to retain the Go failure categories.

Matching remains incomplete. DNS and DNS-SRV resolution, complete runtime
wiring/lifecycle/readiness, and mixed Go/Rust Matching validation remain open.
Do not begin another service based on this slice.

Pinned source SHA-256:

- `go/common/peerprovider/ringpopprovider/factory.go`:
  `6399dd07109751fce846d83495b5455782478073b4b6df45197688fb41f8a5a7`
- `go/common/peerprovider/ringpopprovider/config/config.go`:
  `8c9963d0a8f618497ef63413774b6875f03b2b2bc638cba6fc0ec5a76f933d4a`
- Ringpop `discovery/statichosts/lib.go`:
  `af155962084fba055c5f614f4306fabe723e5b1f6dca66fb829446b5bcab7c6e`
- Ringpop `discovery/jsonfile/lib.go`:
  `ff45b2d53b40bf793c784701cea7b5b6cbf98f8a338be8c28bf28ca8a753db16`

Evidence and exit markers are in `memory/evidence/ringpop-bootstrap-provider/`.
Final results: `go-oracle.exit`, `rust-focused9.exit`, and `clippy6.exit` are
all `0`. Focused Rust tests ran 20 Ringpop tests (189 filtered out); strict
locked Clippy used `cargo clippy --locked --offline -p cadence-server
--all-targets -- -D warnings`. Earlier interrupted attempts are retained in
the evidence directory and are not counted as passing runs.
