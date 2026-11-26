# Recovery timing pooled (kill_frontend x3 runs, Cassandra check) 2026-11-26

Run [45708672523](https://github.com/auenkr/cadence-rust/actions/runs/45708672523), head `58b292e3abe117f23864ace5ca3db78027327086` (branch perf-recovery-pooled = master 22b0ef0b5 + harness changes). Push-triggered matrix: 3 MySQL jobs (`fe1..fe3`: `kill_frontend`, 10 reps, `rust_services=frontend,matching,history`) and 1 Cassandra job. Runner: AMD EPYC 9V45, 4 vCPU. Conclusions: fe1 success, fe3 success, fe2 failure (see lost workflow), cassandra failure (Task B).
Binary SHA256: Go cadence-server 780dccf76a25c271eaf97352fcb46a2cc2d6928b239792a13fa38ddaa60ad55a; Rust cadence-server f510cc3c7fd1fda7b5fa1b7c376fab803282685d5ea8e96fe3149a40f90c0f74, cadence-history 1280bea951bcba095f65d9375a9ed5d26896c252f33844318f22c0c6c23584b5. (The Rust sha is the same as in the earlier run: the rust source did not change.)
Harness changes (no rust/go code): `--database mysql|cassandra` in recovery-run.py, workflow input `database`, `plan` job + `push-matrix.json` (push trigger on branch perf-recovery-pooled only; master needs this merge before `workflow_dispatch` has the `database` input), Cassandra `schema_version` snapshot before/after each event, untimed bring-up when `cold_fresh` is not in the scenarios.

## Task A: kill_frontend, pooled 3 runs x 10 reps = 30 events per arm (ms, t0 = just before `docker compose kill -s KILL frontend`)
| metric | go median / p95 | rust median / p95 | rust-go median diff, 95% CI | rust-go p95 diff, 95% CI |
|---|---|---|---|---|
| stable (10 consecutive <2 s) | 2963 / 3404 | 2777 / 2961 | **-185 [-320, -135]** | -380 [-626, -211] |
| first NEW completion | 2508 / 2747 | 2493 / 2655 | -15 [-396, +1622] | -442..+481 (n.s.) |

Bootstrap = resample the 3 runs with replacement (10000 draws, seed 1), then pool events. With only 3 clusters the CI is coarse; run clusters are not independent of the harness, so read the CI as indicative. Stable: **Rust is not slower; it is ~190 ms faster at the median (CI excludes 0) and ~440 ms faster at p95.** First-NEW is statistically equal.

Recovery modes by first NEW completion (30 events per arm):
| arm | <500 ms ("fast") | 1.0-2.0 s ("mid") | >=2.0 s ("slow") |
|---|---|---|---|
| go | 11 (37%) | 2 (7%) | 17 (57%) |
| rust | 5 (17%) | 6 (20%) | 19 (63%) |

"Fast" (35-73 ms) = workflows started in the ~50 ms between t0 and when SIGKILL actually lands; those are not recoveries (stable still ~3.0 s Go / 2.8 s Rust for those events). The real recovery modes are mid (~1.6 s first NEW) and slow (~2.5 s first NEW), identical for both arms (Go slow: 2496-3011, Rust slow: 2452-3235). Rust has more mid-mode events (6 vs 2), not slower ones. Stable medians by mode: slow Go 2954 / Rust 2830; mid Go 2135 (n=2) / Rust 1842.

Why no slowness to explain: Rust is not slower. Startup order in the Rust frontend log (`services-*-rust.log`): gRPC + TChannel listeners come up first ("Cadence gRPC frontend listening", "TChannel frontend listening") and only then the Ringpop join (typically 1-2 attempts, ~one round trip), so the listener is not blocked by membership. I found no needless sleep or serial init to remove in the frontend start path (only `cadence-frontend/src/lifecycle.rs:87,90` failure-detection/request-drain, which are graceful-shutdown only, and `ringpop_provider.rs` join retry delays, which are Go-parity values `Duration::from_millis(100)`..60 s, so not a safe speedup). The ~2.5 s slow mode is the same in Go: SDK TChannel reconnect/backoff plus docker restart, not Rust startup. No speedup proposed.

### Lost workflow (Rust)
fe2 rust rep 9: 1 of the workflows lost (Rust 1 of 1,5xx workflows, Go 0): `aa8dd709-b7fd-48d3-896c-19d232ff57d6`, `TimeoutType: START_TO_CLOSE` after 60 s, started 47 ms BEFORE t0 (in flight when the frontend was SIGKILLed). Likely a task handed to a poller whose frontend died (standard Cadence behavior: lost until decision timeout) but it did not recover within the driver's 60 s execution timeout, which a Go in-flight workflow did not hit in 30 events. n=1; needs more reps or a decision-timeout investigation before concluding it is a Rust bug. The job failed only because of this (every event reached stable).

## Task B: Cassandra, kill_history + cold_restart, 5 reps: NOT COMPLETED, found a Rust bug
Job `recovery cassandra`: Go rep 0 passed fully (lost 0/343; kill_history 1553 ms, cold_restart 10226 ms; schema_version intact at 0.54 / visibility 0.10 before and after both events: `schema-version-00-go-*.txt`). Rust rep 0 then never became stable after an untimed bring-up (`RuntimeError: cluster never became stable`), so no Rust restart data exists.
Auto-setup finding (fine): on the fresh Rust bring-up the Rust history container's `update-schema` error was `error reading current schema version:reading schema version: table schema_version does not exist`, so the regex in rust/docker/cadence-auto-setup.sh (`schema_version.*(does not exist|unconfigured)`) matches the REAL text; setup-schema + update-schema then completed (schema 0.54 reached, visibility fresh path same). No regex change needed. Restart idempotency itself could not be checked, because the cluster never served.
Root cause of the failure (product bug, not touched): all Rust services on Cassandra exit with
`Go Cassandra domain metadata error: Type check failed ... UDT field bad_binaries_encoding ... Rust type Option<Vec<u8>> against CQL type ...` (matching/history/frontend all fail at domain-cache start; matching-1 and history-1 exit, frontend/worker then fail Ringpop join).
The Cassandra schema (`go/schema/cassandra/cadence/versioned/s0.0-0.23/schema_0_23.cql:266`) declares `bad_binaries_encoding text`, but `rust/crates/cadence-persistence/src/cassandra.rs:136` declares `pub bad_binaries_encoding: Option<Vec<u8>>` (blob) in `CassandraDomainConfigUdt` (same field is used in `matching_domain_cassandra.rs:485`, `matching_domain_metadata.rs:424,553`). Fix is most likely `Option<String>` (it is the only field of the struct whose Rust type disagrees with the CQL type; `isolation_groups_encoding` and `async_workflow_config_encoding` are already `Option<String>`). Suggested next step: fix + a cassandra unit test, then rerun `scenarios=kill_history,cold_restart database=cassandra` (needs master with this branch's harness).
Cassandra run URL: https://github.com/auenkr/cadence-rust/actions/runs/45708672523 (job `recovery cassandra`), head 58b292e3abe117f23864ace5ca3db78027327086, conclusion failure, binary sha256 above; lost-workflow count: Go 0/343, Rust n/a (never served).

## Task B follow-up: Cassandra `bad_binaries_encoding` fix + rerun (2026-11-26)
Fix: commit `d92db0f54` on branch `fix-cassandra-bad-binaries-encoding` changes `CassandraDomainConfigUdt.bad_binaries_encoding` to `Option<String>` (`cassandra.rs:136`, `matching_domain_cassandra.rs:488`, `matching_domain_metadata.rs:~425,~556`). The live schema is built by `update-schema` over `versioned/` (`bad_binaries_encoding text`, added in v0.15); the top-level snapshot `schema.cql:327` says `blob`, which auto-setup does not apply, so `text` is correct. I compared every other field of the Cassandra domain UDT structs (`CassandraDomainConfigUdt`, `CassandraDomainReplicationConfigUdt`, domain UDT) with the CQL types: no other mismatch. No further code change was needed.
CI (rust-ci.yml, head d92db0f54): package=cadence-persistence [45717183592](https://github.com/auenkr/cadence-rust/actions/runs/45717183592) success (fmt + clippy-test); full workspace [45720289113](https://github.com/auenkr/cadence-rust/actions/runs/45720289113) success (fmt + clippy-test).
Recovery run (recovery-timing.yml, database=cassandra, scenarios=kill_history,cold_restart, 5 reps): [45720302295](https://github.com/auenkr/cadence-rust/actions/runs/45720302295), head `d92db0f54ddc7379b22b01788ed667dc60b0d188`, conclusion **failure** (only because of 1 lost Rust workflow; all 10 events reached stable). Runner: Azure 4 vCPU 15 GiB (see machine-spec.txt in artifact).
SHA256: Go cadence-server 8d294212a61bc1b08c141dfb22db5ec40856bf9a0b6f0926c28492f62974fb67; Rust cadence-server e778ea2e1a3a9fcbb9afb84a0d04896274bb5f3ec38c2d3777e61d87a821e725, cadence-history d86114586c140074794f75d57f6ec28c23c1af4699a8cb63f7589dcff3badeb5.

- **Rust arm starts on Cassandra: yes** (all 5 Rust reps served; domain cache starts, 0 type-check errors).
- **Auto-setup restart idempotent, schema_version intact: yes.** `schema-version-*` snapshots before and after every kill_history and cold_restart event show cadence 0.54 / cadence_visibility 0.10 in all 10 Rust and 10 Go events (`schema_version_intact: true`).
- **Lost workflows:** Go 0/1701; Rust 1/1465 (rep 4, `6b9651e2-717b-4af4-be78-fe9819a99de9`, `TimeoutType: START_TO_CLOSE`, 3 attempts, started 1.09 s before the cold_restart kill, i.e. in flight at the full-cluster restart). Same signature as the earlier MySQL fe2 loss (in flight at kill, START_TO_CLOSE). 2 of 2 Rust-only losses of n=1: now suggestive of a real Rust recovery gap (in-flight decision task not re-dispatched after restart), not yet proven. Needs investigation (follow-up).

| scenario | arm | n | first completion median / p95 (ms) | stable median / p95 (ms) | not stable |
|---|---|---|---|---|---|
| kill_history | go | 5 | 1553 / 1866 | 1878 / 2479 | 0 |
| kill_history | rust | 5 | 35 / 1359 | 1532 / 1574 | 0 |
| cold_restart | go | 5 | 12548 / 12999 | 14857 / 15455 | 0 |
| cold_restart | rust | 5 | 9511 / 10851 | 12064 / 13409 | 0 |

Rust <= Go on stable time in both scenarios (kill_history -346 ms median, cold_restart -2793 ms median). Caveat: the Rust kill_history first-completion median of 35 ms is mostly workflows already completing before the kill takes effect, not recovery; use "stable". **Criterion "0 lost" is NOT met (1 Rust loss).**
