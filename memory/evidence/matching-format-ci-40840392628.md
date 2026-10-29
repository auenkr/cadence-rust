# Matching formatting CI status — run 40840392628

- URL: https://github.com/auenkr/cadence-rust/actions/runs/40840392628
- Workflow: `rust-ci`
- Branch: `t3/matching-startup-integration-review`
- Exact head: `d4ee53f00d505246dc0b7fbbe353d7e989a91c49`
- One status query observed at `2026-10-29T08:15:01Z` (`updatedAt`).
- Run/job status: `in_progress`; no conclusion yet.
- Workspace `Format check`: success.
- `Check changed Matching runtime formatting`: success.
- `Clippy`: success.
- `Test`: in progress.
- Later Go panic quote evidence upload, live Go tally/Rust Matching emission comparison, metrics evidence upload, validation identity and log upload: pending.
- No terminal artifact, exit files, or test totals were available in this status response. Do not infer a terminal conclusion from these step results.
- This head predates the later semaphore and adapter regression changes under Terra integration/review; this run cannot validate those regressions.
- Matching remains incomplete. Frozen `bf9f3d9387a72c4ed26b4be0a85025e65fd485a1` is unchanged. No native mixed lane has passed; all ten required mixed gates remain.
- Next dependency: Terra completes and reviews regression integration, then one coherent candidate is frozen and validated by fresh-target CI before serial mixed gates.
