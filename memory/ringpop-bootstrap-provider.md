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

## Final review of candidate d55ad694 (2026-10-18)

The isolated review checkout already had `d55ad694aa3e924387c7390f021a8aaed8c4e5f2`
at HEAD (parent `593182641e545a7e088c962e97f76fc9ed4b541b`), so no duplicate
cherry-pick was needed. The follow-up compatibility fixes are in the commit
created after that candidate.

Go `encoding/json` probes showed that JSON `null` decodes successfully into a
nil `[]string`, and null elements in arrays become empty strings. Scalar,
object, nested-array elements, trailing JSON, and empty input fail with the
Go type or syntax error category. Rust `JsonFile` now reproduces those
success/failure classes and null conversions. The shared raw input set is
`tests/fixtures/ringpop_bootstrap_provider_inputs.json`; both the Go overlay
oracle and Rust test load that file and feed each same raw string to their
provider. Oracle output is checked in at
`tests/fixtures/ringpop_bootstrap_provider.json`.

The oracle test was moved to `tests/go-overlay/...`; the runner uses Go's
`-overlay` option to expose it to `go test`, leaving the original `go/` source
tree untouched. Additional Go observations confirm `HoStS` maps to hosts,
`DNS-SRV` maps to DNS-SRV, `WeIrD` is rejected, Go validation strings match,
and the tested durations map to the recorded signed nanosecond values. Rust
YAML parse errors add line/column context around the same unknown-mode error
text; the parity test checks the underlying message while confirming rejection.

Reproducible final commands and statuses (all exit `0`):

- `python3 tests/run-ringpop-bootstrap-provider-oracle.py`
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target cargo test --locked --offline -p cadence-server --lib ringpop_discovery::tests -- --nocapture`
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target cargo clippy --locked --offline -p cadence-server --all-targets -- -D warnings`

Statuses and full output are in `memory/evidence/ringpop-bootstrap-provider/review-go-final.*`,
`review-rust-final2.*`, and `review-clippy-final2.*`. The Go oracle's pinned
source hashes remain factory `6399dd07109751fce846d83495b5455782478073b4b6df45197688fb41f8a5a7`
and config `8c9963d0a8f618497ef63413774b6875f03b2b2bc638cba6fc0ec5a76f933d4`.
Review hashes: Rust discovery `a47ade4dee853998faecbc85d0893d7e04bfc2679a11d730a03d047ca317757e`,
Rust config `6b01bfc07f6c334d882f11e8d5f31572e90c95f82c1784e7687507ada4c0a51c`,
input fixture `5aa1bac7b65c4a893de5ab6e8aa198629b66da8d85ba15ce3f2b61ec626bcdfd`,
output fixture `8d9fe7211f181639434b85b447792f78ca098ff110116195c510c62e5cb7371e`,
Go overlay test `61b523d1af2192451f95323fb86ba3c43197332df7141b1947d0bc5eab4ced6d`,
and oracle runner `c4f0f2b3b8f52632361e9dd4f8cd40f6f01c8c3735d1587647142478750097a0`.

This provider slice is still not a Matching completion claim. DNS/SRV, runtime
wiring/lifecycle, and mixed-service validation remain out of scope here.

## Review parity follow-up (2026-10-19)

The final review extended the actual-Go differential for JSON byte semantics and
configuration duration precision. `JsonFile` now normalizes malformed UTF-8
only inside JSON strings, replaces lone escaped UTF-16 surrogates with U+FFFD,
validates the complete JSON document before returning a target-type error, and
preserves Go's `UnmarshalTypeError` category for oversized root numeric tokens.
The raw JSON visitor continues scanning after an invalid list element, allowing
a later syntax error to take precedence as it does in `encoding/json`.

The duration parser now follows Go `time.ParseDuration` fractional accumulation
and float evaluation. Pinned Go returned `1199999999999ns` for
`0.333333333333333333h` and `3600000000000ns` for
`0.999999999999999999h`; the previous rational truncation differed on the
second case. The oracle fixture records both exact values.

Final reproducible checks from the isolated review checkout, using the warmed
`t3-e426148a/rust/target` cache with one Cargo job:

- `python3 tests/run-ringpop-bootstrap-provider-oracle.py` — exit 0.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target cargo test --locked --offline -p cadence-server --lib ringpop_discovery::tests -- --nocapture` — 6 passed, exit 0.
- `CARGO_BUILD_JOBS=1 CARGO_TARGET_DIR=/home/ubuntu/.t3/worktrees/cadence-rust/t3-e426148a/rust/target cargo clippy --locked --offline -p cadence-server --all-targets -- -D warnings` — exit 0.

Full final logs and exit markers are stored under
`memory/evidence/ringpop-bootstrap-provider/parity-final-*`. This remains a
provider/config compatibility slice; production discovery wiring, DNS/SRV,
readiness/lifecycle, and mixed Go/Rust Matching validation are still open.

## DNS and DNS-SRV provider implementation (2026-10-19)

`ringpop_discovery` now has injectable DNS and DNS-SRV providers with per-`Hosts`
successful-answer caches, Go's randomized set-based seed traversal, host/port
formatting, not-found suppression, target lookup error behavior, SRV priority
and weighted ordering, and the Go SRV cache's service-name-only key quirk.
`Provider::from_config` selects both modes. Fake resolver cases are checked
against the pinned Go factory through `tests/run-ringpop-bootstrap-provider-oracle.py`
and `tests/fixtures/ringpop_dns_provider.json`.

On Linux, ordinary host lookups use libc `getaddrinfo` with Go's cgo flags
(`AF_UNSPEC`, `SOCK_STREAM`, `AI_CANONNAME|AI_V4MAPPED|AI_ALL`, no
`AI_ADDRCONFIG`), retain libc's returned order and duplicates, and map IPv6
scope IDs to interface names. IP literals are returned unchanged, including
scoped IPv6. Hickory 0.24.4 handles SRV queries; its resolver is initialized and
queried on a blocking thread, has answer caching disabled, and is dropped off
any active Tokio runtime.

This does not establish universal Go resolver parity. Linux libc matches Go's
cgo path and therefore follows the host's NSS services. Go can instead select
its pure-Go resolver through `netgo`, `GODEBUG=netdns=go`, platform rules, or
recognized NSS configuration; Rust has no corresponding Go resolver-selection
switch. Non-Linux host lookups currently use Hickory and do not implement all
platform NSS behavior. Hickory's cached SRV resolver configuration also does
not yet reload changed system resolver settings; Go's resolver polls/reloads
system DNS configuration. These remain explicit audit gaps. The provider is
also not yet wired into Matching's runtime lifecycle; mixed Go/Rust service
validation, restart/recovery, and end-to-end SDK workflows remain required
before declaring Matching complete.

Final focused Rust provider tests pass (15 tests), including a Tokio lifecycle test.
The machine-specific Go/Rust Linux localhost/literal comparison passes through
`tests/run-linux-system-lookup-parity.py`; strict `cadence-server` all-target
Clippy and the actual pinned-Go provider/DNS oracle also pass. Exact final source,
commands, logs, exits, hashes, and resolver limitations are recorded in
`memory/evidence/ringpop-dns-provider/final.md`. These checks validate this
provider slice only; runtime wiring, mixed service validation, restart/recovery,
and SDK workflows remain open.

## Production Matching provider draft (2026-10-19)

Candidate commit `e6fc0fd79` adds `MatchingRingpopProvider` and source-level
lifecycle integration. Its follow-up draft removes the role-specific snapshot
restriction so one provider can answer both Matching and History lookups,
captures the bound local address rather than assuming member index zero, and
matches Go `rpc.ListenIP` scoring for wildcard listeners by reading interface
UP/LOOPBACK flags. Explicit Ringpop `broadcastAddress` still takes precedence.
A pure score fixture covers UP, DOWN, IPv4, IPv6, and loopback cases.

At the time of this draft, the provider integration test had not been run. It
constructs two providers on prebound
TChannel listeners using `serve_matching_with_ringpop_listener`, gives them
Matching and History role labels, bootstraps them against one another, checks
role lookup, evicts one node, and restarts it at the same address to check
membership recovery. Its later validation result is recorded in the
2026-10-19 follow-up below.

At this draft checkpoint, the next planned steps were to run the Rust integration
test and then add a pinned-Go Ringpop peer on the same TChannel protocol path.
Those provider-level tests have since passed as recorded below. The full mixed
Go Frontend/History/Worker + Rust Matching SDK workflow remains the service
gate.

The Go peer harness is now present as source at
`tests/go-overlay/common/peerprovider/ringpopprovider/rust_provider_lifecycle_test.go`
with runner `tests/run-matching-ringpop-go-peer.py` and Rust probe example
`rust/crates/cadence-server/examples/matching_ringpop_go_peer_probe.rs`. The Go
test starts Cadence's pinned Ringpop provider on a real TChannel channel, and
the Rust example runs Matching's concrete provider and shared listener at a
reserved address. It checks Go-side role/port-label decoding, observes Rust
self-eviction, then verifies recovery when Rust restarts at the same address.
The Go assertion also requires the recovered member incarnation to exceed the
first observed incarnation. The runner uses a unique temporary artifact
directory and stores source hashes, per-gate status, and full Cargo/Go logs;
its Cargo invocation is locked and both build parallelism settings are one.
At the time of this draft, these harness files had not been compiled or run.
Their later mixed-Go result is recorded in the 2026-10-19 follow-up below.

Production startup recipe: bind Matching's TChannel listener; construct the
provider using that listener's concrete `local_addr()`; install
`provider.handler()` with `serve_matching_with_ringpop_listener` on that same
listener; call `provider.start()` (discovery JOIN, SWIM, readiness, role/port
labels); then install/refresh Matching membership. Shutdown order is
`lifecycle.withdraw()`, drain and stop the Matching handler, then
`lifecycle.stop_after_drain()` so peers can learn the FAULTY transition before
Ringpop is destroyed. `main.rs` remains unwired; the runtime owner must apply
this sequence.

## Matching provider and lifecycle validation follow-up (2026-10-19)

The source-only state above is superseded for the provider component by the
committed validation chain ending at `851e20445ddfdd4b3197f3a3603d2d3da05e18ca`.
Rust Matching provider, lifecycle, and Ringpop tests passed against that source
core; strict locked all-target Clippy also passed after the shared-listener probe
was added. The source list in
`memory/evidence/matching-swim-runtime/attempt-20261019T211342Z/rust-core-source.sha256`
matches the exact core source tested in
`memory/evidence/matching-swim-runtime/attempt-20261019T210514Z/rust-core-gates/`.
The final strict Clippy log is in the 211342 attempt directory.

The pinned Go Ringpop peer test passed with explicit phase acknowledgements:
Go validates the initial Rust member's TChannel and gRPC endpoints and
incarnation before allowing self-eviction, confirms removal before allowing a
same-address restart, and confirms the recovered endpoints and strictly higher
incarnation before allowing Rust shutdown. The saved Go output records initial
incarnation `1792444429800` and restarted incarnation `1792444429928` at
`127.0.0.1:37789`; the probe reports both `rust-peer-ready 2` and
`rust-peer-recovered 2`. Raw runner status, source hashes, Cargo output, Go log,
and overlay are under
`memory/evidence/matching-swim-runtime/attempt-20261019T211342Z/`.

These gates validate the concrete provider and lifecycle integration tests and
the actual pinned-Go Ringpop peer lifecycle. They do not wire
`MatchingRingpopProvider` and `MatchingMembershipLifecycle` into production
`main`, and they do not validate the complete Go Frontend/History/Worker plus
Rust Matching SDK topology, service-level failure/restart/recovery, or Matching
parity audit. Matching remains incomplete; do not start History migration from
this component result.
