# Final acceptance CI dispatch

- Acceptance branch: `t3/final-acceptance-ci`
- Acceptance head: `10692b2d5a5b912da051a80ca15c30f8236484dd` (exact `origin/master` at assembly)
- Candidate source tree was clean before dispatch.
- Dispatch command: `gh workflow run rust-ci.yml --ref t3/final-acceptance-ci -f clean_target=true`
- Workflow: `rust-ci.yml`; whole workspace (`package` omitted); fresh uncached target requested.
- Run: https://github.com/auenkr/cadence-rust/actions/runs/40373705020
- Dispatch observation: `in_progress`, head SHA `10692b2d5a5b912da051a80ca15c30f8236484dd`.
- Review status: provisional pending approval of the separate Luna corrections review.
- No mixed-cluster gate was dispatched. The existing adaptive gate `40373145351` remains owned by the parent orchestration.
- Evidence is recorded on this branch, separate from the immutable acceptance source branch.
