# Matching pause checkpoint — 2026-10-25

User requested: pause after both current workers finish, save work, continue later.
Both Luna fallback workers finished and pushed clean worktrees. Work is paused;
do not resume implementation, dispatch CI, merge or spawn workers until requested.
Matching remains incomplete. Root master before this documentation checkpoint:
`a453ac12ede5a56d89dab5e1405e89a9d6f600f4`. No implementation branches were merged.

## Saved branches

- Request/error parity: `t3/matching-go-error-parity`,
  `1b1b37da6a0c81bfe04a0a92191c3e942b08799f`.
  Worktree `/home/auenkr/.t3/worktrees/cadence-rust/t3-8b29d60d`, clean and pushed.
  Latest fixture uses a valid inner activity PollRequest; explicit nil-request
  parity tests remain. Latest fixture change has no CI, per pause instruction.
  Run 40176252648 passed on `416d619117b372114af83a0bd3f54dabccedc1ae`.
  Run 40176622622 failed on `70a7eebb1e75e6b3d50c93077c63cf1bf686d7d9`
  in `go_thrift_handler_routes_each_tested_method`, because the routing fixture
  sent nil PollRequest and received the now-correct nil-dereference error.
- Raw RunID revision: `t3/matching-runid-raw-tests`,
  `055d5fd8b47ae45b76fefcdb7294460fe2e93306`.
  Worktree `/home/auenkr/.t3/worktrees/cadence-rust/t3-87ad210c`, clean and pushed.
  Includes checked raw serialization, queue size accounting and concrete malformed
  durable Add assertions. `git diff --check` passed; no compilation/tests/CI.
  Activity sync-match-to-poller coverage remains missing. Read the branch's
  `memory/matching-runid-raw-parity.md` for source evidence and limitations.
  Original raw worktree `t3-05598dd0` was inspected clean and left untouched.
- Query completion: `t3/respond-query-task-parity`,
  `78006b797e658d3169828e8cab8f481c3894e2b4`.
  Run https://github.com/auenkr/cadence-rust/actions/runs/40177106445
  completed **success** on that exact SHA (checked when pausing).
  Actual `query-format.exit` and `workspace-format.exit` artifacts still need
  inspection and durable evidence recording; job success alone is insufficient
  because format checks are non-failing. Includes Actions-generated mechanical
  formatting and nil-result/strong-query fixes. Not merged.
- Panic source contract note: `t3/matching-query-wire-contract`, `2f12f009c`.
  Source-derived, not wire-captured; not merged.

## Resume requirements

1. Inspect saved branches and memory notes; preserve unmerged work. Claude hit an
   API rate limit, so final two tasks used gpt-6-luna medium/default workers.
2. Verify query format exit files, review latest changes, record exact run evidence.
3. Review RunID changes and missing sync-match coverage. Do not claim exact parity:
   Go SQL malformed RunID may panic in the writer goroutine (source inference),
   while Rust returns a persistence error. Cassandra error mapping also needs review.
4. Dispatch focused CI for unverified branches only after resume. All Cargo, Go and
   formatting validation stays on Actions; no local validation commands.
5. Reconcile absent Query payload behavior and malformed domain UUID contracts;
   these remain open. Integrate only reviewed, verified changes.
6. After final production integration, pin exact SHA and run fresh uncached workspace
   Clippy then workspace tests, followed by mixed lanes serially. Record binary
   SHA256s. Old PostgreSQL/MySQL/Thrift evidence is not relabeled to new Rust code.
7. Clean merged branches/worktrees only after verifying merge ancestry, clean state
   and no live user/worker. No cleanup performed at this pause.

No new CI was dispatched by the two finishing workers. No local Cargo/Go/format
commands were run. No next-service work is authorized by Matching's current state.
