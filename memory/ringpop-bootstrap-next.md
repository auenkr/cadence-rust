# Ringpop bootstrap and JoinGroup contract (bounded research, 2026-10-18)

Pinned source: `github.com/uber/ringpop-go v0.10.0`, module cache `/home/ubuntu/go/pkg/mod/github.com/uber/ringpop-go@v0.10.0`. This documents direct bootstrap/Join behavior for the current Matching membership implementation; no Rust production changes are part of this slice.

## Source behavior and integration plan

1. **Resolve and normalize seeds.** `swim/join_sender.go:117-167` calls `DiscoverProvider.Hosts()`, propagates provider errors, appends the local advertised address if absent, groups hostports by captured host using `util.HostPortsByHost`, and computes potential targets by excluding exact local address (`:188-204`). Empty seeds become self-only and therefore a successful single-node bootstrap. Repeated identical address entries collapse in `HostPortsByHost`; do not de-duplicate distinct ports on one host. Cadence's provider selects static hosts, JSON file, DNS, or DNS SRV in `go/common/peerprovider/ringpopprovider/factory.go:165-184`.
2. **Defaults and target count.** `join_sender.go:37-53,151-165`: per-request timeout 1s, max duration 120s, requested JoinSize 3, parallelism factor 2. `util.SelectDuration/SelectInt` means zero takes default; negative nonzero is retained. Effective size is `min(requested size, potential target count)`, including zero/negative results as the source computes them. Cadence `config/config.go:30-71` separately defaults `maxJoinDuration` to 10s before passing it to Ringpop, so normal Cadence configuration does not use Ringpop's 120s default.
3. **Host preference and batch selection.** `join_sender.go:188-285`: targets on hosts other than local captured host are preferred. It fills each batch from preferred nodes first, then same-host alternatives; cap is `(size - joined_count) * parallelismFactor`. Within a class ordering comes from Go map iteration and should be treated as nondeterministic. Failed targets are exhausted for the current round, then are eligible again after all round candidates are consumed. `JoinGroup` increments tries once and issues all selected requests concurrently (`:330-407`); successes and failures are appended by goroutines, so response list order is nondeterministic. Successful response membership is merged using `AddJoinList`.
4. **Retry, timeout, and failure.** `JoinCluster` (`join_sender.go:287-328`) succeeds when joined count reaches effective size; otherwise checks elapsed duration only after a whole group has returned, errors when elapsed is `> maxJoinDuration`, and sleeps via exponential delay before retry. Individual requests use a TChannel context with `JoinTimeout`; the request serializes the timeout as `time.Duration` nanoseconds (`:409-449`). Delayer defaults in `join_delayer.go:32-34,54-130`: 100ms initial, 60s cap, exponential `2^n` with jitter, then sleep. On all-seed failures the first retry delay is random in [0,100ms); no exact elapsed boundary should be asserted without injected sleeper/clock.
5. **Readiness and stopped mode.** `Node.Bootstrap` (`node.go:420-456`) requires channel; on `sendJoin` error returns without setting ready. On success, unless `Stopped`, starts gossip and healer, then marks ready and returns joined targets. `Stopped` suppresses those two starts, but still marks Ready. Remote JOIN handler checks Ready and returns `ErrNodeNotReady` before bootstrap; see `join_handler.go` line refs/hashes below. Thus “stopped” is not “not ready”.
6. **Cadence contract.** `Provider.Start` (`provider.go:197 onward`) calls Ringpop Bootstrap before adding its listener and setting port/service labels; `MaxJoinDuration` is the only BootstrapOptions field Cadence sets. Rust should mirror provider config/discovery separately from the Ringpop JoinSender state machine, then expose readiness only after successful bootstrap. Keep the work limited to Matching membership.

## Small Rust implementation slices

1. Add seed-provider contract and configuration mapping (hosts/file/DNS/DNS-SRV), preserving source address strings and errors; validate against Cadence config behavior.
2. Implement host capture/grouping and exact self insertion/exclusion plus potential-node/effective-size calculation. Differential-test self-only, duplicate exact seeds, several ports on same host, same-host and remote-host seeds, and multiple host groups.
3. Implement preferred-first group selection and exhausted-round retry state. Compare sets and preference classes, never unstable Go map order. Cover join-size and parallelism caps, joined-node removal, and negative option behavior.
4. Implement concurrent JOIN request outcome collection, per-request timeout and membership merge; test success/failure mixes and late/failed targets against the original live protocol.
5. Implement retry deadline checks and exponential jitter delay behind Rust clock/random/sleeper seams; preserve the Go boundary (`elapsed > max`, checked after group). Add deterministic unit cases only where seams permit.
6. Wire successful/failed bootstrap readiness and `Stopped` lifecycle; verify server rejects inbound JOIN/PING while unready and that stopped successful bootstrap is ready while gossip/healing stay inactive.
7. Then run mixed Go/Rust Matching compatibility and failure/restart/recovery workflows; do not begin History until this service's broader parity audit is complete.

## Oracle status and remaining evidence

The existing `tests/run-ringpop-join-addjoinlist-oracle.py` demonstrates the temporary copied-module plus Go overlay pattern, and fixture tests call original unexported Ringpop code. A new host-selection/BootstrapOptions oracle is **not yet implemented or passed** in this bounded slice. The next task should add an overlay test in `package swim` that calls original `newJoinSender` and `SelectGroup`, serializes normalized sets/classes/effective options, and invokes original `Node.Bootstrap` only for deterministic self-only and stopped-success cases. Do not port or duplicate the algorithm in the oracle.

Live recipe for timing-dependent cases: run a pinned Go Ringpop node with a real TChannel listener and a controllable peer that either accepts `/protocol/join`, returns an error, or withholds response. Record request JSON `timeout`, target set, number of `/protocol/join` calls, final joined list/error and `Ready()`. Use short explicit positive timeout/max duration and an injected `delayOpts{sleeper: recordingNoop}` only in the overlay package; because JoinCluster reads wall time directly, record observed elapsed values and avoid exact duration assertions. For default/random jitter, assert range and retry count rather than exact sleep duration. No success claim for these live timing cases is made here.

## Pinned source hashes (SHA-256)

- `swim/join_sender.go`: `b33fdb7b679dd3c53e8573442f1db5e0c197cffb3bf58a4295d0b4dbc4d094a6`
- `swim/join_delayer.go`: `f33d52c1835289fc176570011d046cccd0b299f063b62218b6a9925a4c202745`
- `swim/node.go`: `9537bc1b19c797eb44c4036e825ff68f4b9bf506d2df43c6726dde7edf74b913`
- `swim/join_handler.go`: `561bdc0c181d05af597c15f74553fe63ff52cf5a5d8714d3459624e0fda693f9`
- Cadence `ringpopprovider/provider.go`: `5e990c9356270f77d4103b8dc2144c36279a7702f4356127cd0794d1927a1f00`
- Cadence `ringpopprovider/factory.go`: `6399dd07109751fce846d83495b5455782478073b4b6df45197688fb41f8a5a7`
- Cadence `ringpopprovider/config/config.go`: `8c9963d0a8f618497ef63413774b6875f03b2b2bc638cba6fc0ec5a76f933d4a`

Regenerate with `sha256sum` against the module-cache paths before implementing, because module cache files are outside the repository and are not pinned by the fixture itself.
