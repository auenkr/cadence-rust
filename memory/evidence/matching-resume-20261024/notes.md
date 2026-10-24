# Matching resumption — 2026-10-24

The root checkout was ff6d4b9f1, behind origin/master 4424a3b4d. Preserved its generated tracked Python bytecode change in stash `matching-resume-generated-pyc`, fast-forwarded, then integrated the previously unmerged branches matching-tests-port3-20261024 (includes port2), matching-testfix-20261024, and matching-twonode-20261024. Integration commit 55d8bc867; pushed to origin/master.

User now requires ALL Cargo work on GitHub Actions; AGENTS.md updated in fb8bb1dbb. Workers use gpt-6-luna, medium reasoning, normal/default service tier, up to five. Started three isolated workers: remaining matcher/forwarder tests, CI Cassandra acceptance, and final parity audit. Worker branches must be reviewed and integrated before completion.

425f50562 adds rust-ci workflow_dispatch clean_target=true: bypasses target cache and selects RUNNER_TEMP/cargo-target-final, records clippy/test exit files, commit and target directory. It also fixes a pre-existing workflow defect: echo after the clippy pipeline could hide a failure despite pipefail. Both commands now capture PIPESTATUS[0] and exit with it.

Fresh whole-workspace validation started at https://github.com/auenkr/cadence-rust/actions/runs/40073658725 for 425f50562. Confirmed live in Clippy with fresh-target selection succeeded and Rust cache step skipped. No pass claimed yet. Later worker changes require fresh validation again.

Source inspection found numerous ignored tests whose comments are stale or describe missing seams rather than behavioral gaps. Example: poller history callback production metric behavior is already implemented in PollLifecycle::begin and PollWaitGuard::drop, although matching_partial_upgrade_b ignores the callback API test. Do not add unused callback plumbing simply to reproduce Go class structure. Audit actual observable behavior and map replacement tests.

Matching remains incomplete. Required: worker outcomes, current-head acceptance evidence (Cassandra especially), fresh workspace CI success, and requirement-by-requirement parity closure. Existing MySQL two-node pass alone does not prove completion.

## Integration candidate

Reviewed and integrated existing unmerged semaphore params/pagination, P1 semaphore startup, lifecycle-v2, and nodiff A/B branches into `matching-completion-integration-20261024`. No master merge yet: candidate requires CI review. Resolved overlapping semaphore constructor conflicts in favor of the required stop callback and paginated persistence API, preserving both behavior fixes. Prior semaphore branch CI failed three lifecycle tests; combined candidate must prove these now pass.

Whole-workspace candidate run: https://github.com/auenkr/cadence-rust/actions/runs/40073886465 (pending). Remaining branch work includes OpenFeature (assigned to a replacement Luna worker), error seams, panic recovery and tracing; inspect before declaring any blocker closed. Current final-parity audit merged to master in d71bef324; its remaining-gap list predates these integrations.

TLS source evidence in nodiff-b notes resolves the TChannel question: pinned Go supports rpc.tls only on gRPC (and HTTP configuration separately); Go TChannel listener uses no credentials. Rust need not invent a TLS mode Go does not have. Source guard is supplementary, not runtime TLS evidence.
