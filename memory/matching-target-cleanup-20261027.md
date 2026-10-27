# Historical local Rust target cleanup — 2026-10-27

## Scope and safeguards

- Worktree inventory source: `git worktree list --porcelain`.
- Inspected only up to 15 oldest-mtime real `rust/target` directories from local `/home/auenkr/.t3/worktrees/cadence-rust/*` worktrees, excluding this cleanup worktree and the primary `/mnt/storage/service/github/cadence-rust` checkout.
- For each existing candidate, checked `rust/target` was a directory and not a symlink, measured it with `du -sb`, and inspected `/proc/<pid>/cwd` plus `/proc/<pid>/comm` only for process use; no command arguments were read.
- The bounded inventory found **zero** real `rust/target` directories in the candidate set. No targets were deleted; no ignore, artifact, tracked-file, active-process, lock, or recency eligibility checks were needed because there were no candidates.
- Before bytes: 0 candidate target directories, 0 bytes measured. After bytes: 0 candidate target directories, 0 bytes removed. Reclaimed bytes: 0 (no estimate).
- No source files, histories, worktrees, or branches were removed. Existing source WIP and unmerged branches remain intact.
- No Cargo or Go command was run; no local validation was performed.
