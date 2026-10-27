# Bounded historical Matching worktree cleanup — 2026-10-27

Performed from `t3/cleanup-merged-matching-worktrees`.

## Verification

- Read `AGENTS.md` and both earlier cleanup ledgers first.
- Selected four clean historical worktrees whose exact checked-out SHAs appeared under branches from `git branch -r --merged origin/master`; independently verified each with `git merge-base --is-ancestor <sha> origin/master` immediately before removal.
- Checked each worktree with `git status --porcelain`; all were clean. Checked `/proc/*/cwd` without reading process arguments; no process cwd was inside any selected path. All four branch tips predated the one-hour activity cutoff.
- Their code and evidence remain preserved because each recorded SHA is an ancestor of `origin/master`.
- Excluded the active cold-cache RPC mapping and adaptive comparison tasks, recent live name probe/report work, this cleanup branch, master, dirty worktrees, and the adaptive-scaler fixture branch due to conservative activity ambiguity.
- No Cargo, Go, build, test, formatting, or runtime commands were run. No artifacts or source outside the four worktrees were removed.

## Removed

| Branch | Exact head SHA | Removed worktree |
| --- | --- | --- |
| `t3/final-acceptance-ci` | `10692b2d5a5b912da051a80ca15c30f8236484dd` | `/home/auenkr/.t3/worktrees/cadence-rust/t3-08acf542` |
| `t3/matching-gc-evidence` | `a51939fd16ea4be485a61e43c6f1f1b1d3e5d56e` | `/home/auenkr/.t3/worktrees/cadence-rust/t3-28e538f8` |
| `t3/matching-launcher-fatal-test` | `ff91ef4bde65fe86972831f02177e7a6bd862102` | `/home/auenkr/.t3/worktrees/cadence-rust/t3-d1e94fa3` |
| `t3/respond-query-task-parity` | `78006b797e658d3169828e8cab8f481c3894e2b4` | `/home/auenkr/.t3/worktrees/cadence-rust/t3-fe2b009b` |

Removal followed the repository procedure: `git worktree remove --force` after clean-status verification, `git branch -d`, then `git push origin --delete`.
