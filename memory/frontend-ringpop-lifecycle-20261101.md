# Frontend Ringpop listener lifecycle slice — 2026-11-01

Integration base: `71c3c273334836a9634cc2c509f4c2cdcf22671c`.
Implementation commits: `5a04cec034517f8a1669b49f4a3660bda82a0a09`,
`d1a2cdd54e9e9f439742ff6565b9a177ded95499`,
`898b4a27f2f4d28fb06f83c67835e978359053d2`,
`4596823884c6c79bd0d7cc58b92b3dd7fff8ada2` (Frontend-only AdminService
TChannel dispatch), `7b62ddc9ab38fdf9e344f8ec6996afd4213d0963` (Frontend
leave test), `0f4155fef729c74f59af61dc17377d6501dc5085` (CI formatting
artifact), and `da88b2bf64bb6bc19e12547c787490f3b4b6e1b3` (generated gRPC
trait import required by existing hook tests), `71c70ab2e` (Clippy initializer
fix), and merge `a80592e491fa893cdacd0acab615e4eedee0f1c4` from `origin/master`
plus `079139dc4` (order Frontend leave coverage after Matching recovery) on
`t3/frontend-ringpop-lifecycle`.

## Source and behavior evidence

- Rust Frontend launcher previously served TChannel with `tchannel::serve`
  (`rust/crates/cadence-matching/src/launcher.rs`, former line ~1944), which
  sets no Ringpop handler. `tchannel.rs::handle_call` returns an error for
  `service == "ringpop"` when the optional handler is absent.
- Matching already reserves its TChannel listener, creates the
  role-parameterized `MatchingRingpopProvider` from the bound address, starts
  the listener before JOIN bootstrap, and stops membership before tearing down
  the listener. This slice reuses that provider without changing Matching's
  role or call path.
- Go Frontend `Service.Start` starts `Resource` before handlers; Go
  `resource_impl.go::Start` starts the RPC factory and dispatcher before
  `MembershipResolver.Start`. `Service.Stop` fails health, drains/stops
  handlers and limiters, then calls `Resource.Stop`.
- Rust now reserves the Frontend TChannel port before provider construction,
  advertises `cadence-frontend` with the bound TChannel port and configured
  gRPC port, starts the final workflow/admin TChannel state plus Ringpop
  listener before provider bootstrap, and installs no Matching handler on that
  listener. Shutdown self-evicts and stops the provider before listener abort.
  Startup errors stop the provider and abort/join the listener task. Frontend
  bootstrap failure also withdraws/stops Matching membership and the Matching
  runtime already started earlier in the combined launcher, aborts the shared
  Matching listener, and shuts down its metrics.
- Focused provider regression coverage now crosses the Frontend listener with
  a Matching provider JOIN and checks the Frontend role and named TChannel and
  gRPC ports, then verifies Frontend self-eviction is observed by the Matching
  peer before Frontend provider stop. A focused service filter test checks
  Frontend workflow/admin allowance and unchanged Matching service allowance.
  This demonstrates Ringpop dispatch and role labeling only; it does not
  establish Frontend consumer routing parity.

## Validation

- `git diff --check`: passed before commit; no Cargo, rustfmt, or Go commands
  were run locally.
- CI runs 41328506551, 41328645787, 41328689458, 41328957772, and
  41329239984 were superseded/cancelled after source corrections; none is
  validation evidence.
- CI run 41328990185 reached Clippy and failed compiling the existing gRPC
  workflow-hook test module because its generated trait was not imported. The
  issue is fixed in `da88b2bf64bb6bc19e12547c787490f3b4b6e1b3`. Its
  `format-all-patch` artifact was inspected with `git apply --stat` and
  `git apply --check`, then applied with `git apply`; it formatted the touched
  launcher code plus two existing hook expressions in `grpc.rs`.
- CI run 41329817593 passed Clippy and formatting, then failed one focused
  Ringpop test: the Frontend member was stopped before the existing Matching
  recovery assertion, removing its bootstrap peer. The test now checks
  Frontend self-eviction after Matching recovery.
- Current GitHub Actions `rust-ci.yml` run:
  https://github.com/auenkr/cadence-rust/actions/runs/41330331495
  dispatched for `cadence-matching` with `format_all=true` at head
  `079139dc4c88ce18c19def1e7ef5e9431410ad65`. It was pending at the latest
  observation. Retrieve its terminal result and inspect/apply any new format
  artifact before treating this slice as validated.
- CI run 41329314627 reached Clippy and failed on the hook test's
  `field_reassign_with_default`; `71c70ab2e` fixes it without a lint
  suppression. The branch then merged `origin/master` at
  `a80592e491fa893cdacd0acab615e4eedee0f1c4`, including the same hook fixes,
  production Frontend WorkflowApi installation, and gate backend fix.
- Parent reports server CI 41329035362 succeeded after production hook
  installation. Mixed gate 41327590981 failed Ringpop bootstrap; its source and
  binary hash evidence is in `memory/frontend-mixed-gate-20261101.md`. This
  slice did not start a second mixed gate.

## Remaining discovery and lifecycle gaps

- No Frontend history/visibility/admin client was wired to role-filtered
  Ringpop snapshots here. JOIN response and `cadence-frontend` labels do not
  prove remote Frontend routing, load balancing, failure detection, or
  restart/recovery compatibility.
- The gRPC named port uses the configured Frontend gRPC bind port; only the
  TChannel listener is prebound by this slice. Dynamic/ephemeral gRPC port
  advertisement remains unverified.
- No mixed Go History/Worker + Rust Matching/Frontend runtime evidence is
  included. Do not claim Go History bootstrap succeeds until the already-running
  mixed gate has a terminal successful result against a matching binary SHA.
- Full Frontend parity, SDK workflows, broader failure/restart recovery, and
  service-consumer discovery remain incomplete.
