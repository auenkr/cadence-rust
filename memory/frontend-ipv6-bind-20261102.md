# Frontend/Matching IPv6 RPC binding (2026-11-02)

Branch: `t3/frontend-ipv6-bind`.

Frontend and Matching are launched from `rust/crates/cadence-matching/src/launcher.rs`.
Matching already accepted IPv6 `rpc.bindOnIP`; Frontend ignored it and selected only
IPv4 loopback/wildcard. Both now use one RPC bind-policy helper. Explicit bare IPs
(e.g. YAML `bindOnIP: '::1'`) produce `[::1]:PORT` socket addresses. Defaults remain
`0.0.0.0`, and `bindOnLocalHost` remains `127.0.0.1`. Invalid addresses and conflicting
bind settings fail. Frontend's TChannel listener shares this configured bind IP,
as Matching's already did. Discovery address selection remains unchanged.

Added unit tests for IPv4/IPv6 parsing, defaults, invalid/conflicting settings,
and real IPv4/IPv6 loopback bind/connect/accept. The IPv6 bind test skips only when
IPv6 is unavailable/unsupported. No local Cargo, rustfmt, or Go commands were run.

The optional MySQL shell-lane RPC assertion was not added: its Python harness owns
and terminates the services before returning, and existing mixed-cluster clients
and discovery use IPv4. A meaningful IPv6 RPC probe needs a dedicated harness mode
and service configuration rather than a probe after the shell's Python call.

CI evidence:
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41410942568
- Head: `087dd8c587d5314b4ed4b9b8d3b366dd57fc9815`
- Dispatch: `gh workflow run rust-ci.yml --ref t3/frontend-ipv6-bind -f package=cadence-matching -f clean_target=true`
- Conclusion: failure (rustfmt requested one line-wrap change; applied the runner diff).

Formatting/validation rerun:
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41411296796
- Head: `087dd8c587d5314b4ed4b9b8d3b366dd57fc9815`
- Dispatch: same command with `-f format_all=true`; formatting runs on CI only.
- Conclusion: cancelled as superseded by the formatted branch head.

Crate validation:
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41411339193
- Head: `e5ad5041116e5e9cf359694015d538408b351b73`
- Dispatch: `gh workflow run rust-ci.yml --ref t3/frontend-ipv6-bind -f package=cadence-matching -f clean_target=true`
- Conclusion: **success**. Formatting, clippy (`--all-targets -- -D warnings`),
  and `cargo test -p cadence-matching` passed in a fresh uncached target directory.

Both new tests explicitly passed in the crate CI log:
- `launcher::matching_http_listener_tests::rpc_listener_binds_ipv4_and_ipv6_loopback`
- `launcher::matching_http_listener_tests::rpc_listener_parses_both_address_families_and_preserves_defaults`

Final broader workspace validation:
- Run: https://github.com/auenkr/cadence-rust/actions/runs/41412637746
- Head: `e5ad5041116e5e9cf359694015d538408b351b73`
- Dispatch: `gh workflow run rust-ci.yml --ref t3/frontend-ipv6-bind -f clean_target=true`
- Conclusion: **failure**. Fresh-target workspace clippy passed, then workspace
  tests failed in `cadence-service-core/tests/e2e.rs`:
  `admin_cluster_config_and_workflow_maintenance` at line 1306 (None unwrap), and
  `admin_reapply_replication_dlq_and_cross_cluster` at line 1686 (500 History client
  is not configured, expected 200). These exercise service-core admin/history
  handlers rather than the changed Matching launcher; no unrelated fixes included.

No mixed gate was run and no IPv6 gRPC end-to-end claim is made. The required
crate-specific CI is green; the broader workspace failure remains for the parent.
The final evidence-only commit does not change the validated Rust sources.
