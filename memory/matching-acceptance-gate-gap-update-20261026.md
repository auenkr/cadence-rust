# Matching acceptance gate gap update (2026-10-26)

Updated on `master` after reviewing `5d459e13b` and the current gate sources.
No gate was dispatched or run locally. Matching remains incomplete; this change
does not pin a candidate and does not claim service completion.

## Active-offer lifecycle coverage

`.github/workflows/matching-active-offer-gate.yml` now invokes the existing
MySQL mixed harness with:

```sh
tests/gates/run-mysql-lane.sh out/gate "$CARGO_TARGET_DIR/debug/cadence-matching" \
  --probe-active-offer --matching-restart --matching-failure-recovery \
  --matching-idle-check-seconds 3
```

The harness selects Rust Matching alone (`--rust-services matching` inside the
wrapper), leaving Go Frontend, History and Worker running and Go Matching
absent. The probe runs after the restart and failure-recovery lifecycle work;
the workflow keeps the command, lane exit, gate log, nested `results.json`,
source commit and Rust binary SHA-256 in the three-day artifact. On review,
confirm `out/commit.txt`, `out/binary.sha256`, `out/gate.exit`,
`out/gate/lane.exit`, `out/gate/run.exit`,
`out/gate/run/results.json`, `out/gate/run/activeOffer.json`,
`out/gate/run/probe.log` and logs under `out/gate/run/rust-matching/`. Check the actual restart, recovery and
`activeOffer` assertions/logs; a green overall exit alone does not prove which
scenarios ran.

## HTTP/TLS scope

`.github/workflows/matching-http-tls-interop.yml` builds `cadence-server` and
runs `tests/run-matching-thrift-compat.py`: a Go YARPC/Thrift client and Go
Ringpop stub talk to a Rust History fixture using SQLite. It has neither Go
Frontend/History/Worker service processes nor Rust Matching. It therefore gives
no Matching mixed-cluster listener/TLS acceptance evidence. Do not extend this
fixture as a shortcut; a true TLS check needs the production Matching binary
in `run-go-service-baseline.py`'s Go Frontend/History/Worker topology and an
actual TLS client/configuration on Matching's listener. That source-grounded
requirement remains open.

## Final serial dispatch (after parent integrates fatalfix and chooses an immutable SHA)

Do not overlap runs. Dispatch each command only after the previous run has
finished, and verify Actions `headSha` and artifact source identity match the
chosen SHA:

```sh
gh workflow run rust-ci.yml --ref "$BRANCH" -f clean_target=true
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=postgres
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=mysql-probes
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=thrift
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=configstore
gh workflow run matching-production-validation.yml --ref "$BRANCH" -f lane=shard-distributor
gh workflow run matching-mixed-gate.yml --ref "$BRANCH"
gh workflow run matching-cassandra-mixed-gate.yml --ref "$BRANCH" \
  -f extra_args="--matching-restart --matching-failure-recovery --matching-idle-check-seconds 3"
gh workflow run matching-active-offer-gate.yml --ref "$BRANCH"
gh workflow run matching-http-tls-interop.yml --ref "$BRANCH" # supplemental History fixture only
```

For each run inspect its run URL, `headSha`, conclusion, `out/commit.txt`, all
build/gate/lane/probe/scenario exit files, logs and `out/binary.sha256` (or the
Rust CI clippy/test exits and fresh target evidence). For shard-distributor,
require executor selection, TTL expiry, assignment cleanup and successful
recovery in artifacts. For active offer, inspect the files listed above and
confirm the three lifecycle/scenario assertions explicitly. The opt-in probe
writes `activeOffer.json` in the harness artifacts directory; also inspect
`probe.log` and the Rust process logs for both restart and takeover outcomes.
Keep runs serial.

The latest authoritative shard-distributor recovery success supersedes older
failure wording: Actions run `40218022772`, source
`a5a8e2a14638efcc2d7f6cc5bd6ec027271978dd`, Rust binary SHA-256
`5d9a60123b651e6cdd1fa3197a9955085351536a668daf8eb57085c8663d52e3`.
This evidence is only for that exact source and binary; rerun against the final
candidate SHA.

## Production Matching verified TLS mixed gate (implemented 2026-10-26)

Added `.github/workflows/matching-production-tls-mixed.yml`, a manual,
standard `ubuntu-latest` gate (90-minute timeout, concurrency cancellation,
three-day artifact retention). It builds `cadence-matching` in a fresh
run-specific target and uses `tests/gates/run-mysql-lane.sh` to run the
production Ringpop topology: Go Frontend/History/Worker, no Go Matching, and
standalone Rust Matching. The harness's `--matching-rpc-tls` option makes a
short-lived local CA and a matching server certificate with `localhost` and
`127.0.0.1` SANs. It configures Go's Matching outbound with `caFile`,
`enableHostVerification: true`, and `serverName: localhost`; Rust Matching
loads the matching identity on its production gRPC listener. The Node direct
Matching probe trusts that CA. No insecure certificate-verification bypass is
used in this TLS lane. The Go TLS expectation comes from
`go/common/rpc/params.go` using the target service's `RPC.TLS` as the direct
outbound config and `go/common/config/tls.go` mapping
`EnableHostVerification` to normal chain/name checking.

The gate also requests `--matching-restart` and
`--matching-failure-recovery`; both use the same TLS Rust config and the
probe's direct Matching client uses CA-backed TLS during primary/secondary
operations. SDK workflows continue through Go Frontend/History/Worker. Review
the lane's `out/gate/run/results.json`, `probe.log`, `run.exit`,
`rust-matching/matching-shutdown.json`, `rust-matching/rust-server-*.log`,
and `config-redacted/` TLS settings alongside top-level `out/commit.txt`,
`out/binary.sha256`, `out/build.exit`, `out/gate.exit`,
`out/gate/lane.exit`, `out/gate/run/run.exit`, `out/gate/run/binaries.json`,
and `out/gate/run/config-hashes.json`. Confirm result topology has only Rust
Matching and the three Go services, and that restart and failure-recovery
assertions completed. The workflow hashes the exact built Rust Matching
binary before the mixed run.

Dispatch only when the parent authorizes its serial mixed-gate slot:

```sh
gh workflow run matching-production-tls-mixed.yml --ref <branch>
```

No CI run was dispatched in this implementation turn. Lightweight checks
passed: Python AST parse, Node `--check`, Bash `-n`, `git diff --check`, and a
harness dry run that generated and inspected the Go/Rust TLS config. No Cargo,
Go build, services, or mixed lane ran locally. There is therefore no binary
SHA or runtime TLS acceptance evidence yet; obtain it from the Actions
artifact (`out/binary.sha256`) and record the Actions run URL, head SHA and
conclusion after the serialized dispatch. This is a gate implementation, not
Matching service completion evidence.

Independent TLS review found Matching CA credentials accidentally applied to
all Node service clients. Corrected Domain/Workflow/Worker/Semaphore clients
to retain plaintext credentials; only Matching clients use Matching TLS CA.
Runtime acceptance remains pending.
