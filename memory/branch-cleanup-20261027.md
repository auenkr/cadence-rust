# Historical branch cleanup — 2026-10-27

Performed on branch `t3/cleanup-merged-matching-branches`.

## Procedure and verification

- Refreshed origin and inventoried `git branch -r --merged origin/master`.
- Restricted selection to historical matching branches from Oct 23–24 and capped the batch at 15 worktrees.
- Before each removal, rechecked that the remote head was an ancestor of `origin/master`, the worktree had no `git status --porcelain` output, it was not locked, and no process had a cwd inside the worktree (checked `/proc/*/cwd` and `/proc/*/comm`, without reading process arguments).
- For each selected entry, ran `git worktree remove --force`, `git branch -d`, and `git push origin --delete`.
- Post-check: every recorded head still passes `git merge-base --is-ancestor <head> origin/master`; all 15 local branches and worktree entries are absent, and their recorded worktree paths no longer exist.
- `git ls-remote --heads origin <the 15 branch names>` returned no refs after deletion.
- The heads were already reachable from master, so the code and evidence remain preserved there.
- No Cargo, Go, or runtime checks were run.

## Deleted entries

| Branch | Head | Removed worktree |
| --- | --- | --- |
| `matching-cannot-port-review-20261024` | `b1a43b8d7a3035029e6a6b8ba547fdb0368efa0c` | `/mnt/storage/service/github/wt/cpreview` |
| `matching-gate-final2-20261023` | `bbd8ea86dd7795234eef8210036fcae039a691f0` | `/mnt/storage/service/github/wt/gatefinal2` |
| `matching-hash32-short-20261024` | `44e364e1479cfc2006cbce998452d139cbca88ce` | `/mnt/storage/service/github/wt/hash32` |
| `matching-hashring-view-20261024` | `2663bd1f79761fcfaa57a7d75c6777afecd7fb1b` | `/mnt/storage/service/github/wt/hashview` |
| `matching-lifecycle-gaps-20261024` | `72abda2e13a7e3d5824029860f74f22f15c59b6a` | `/mnt/storage/service/github/wt/lifecycle` |
| `matching-list-partitions-owner-20261024` | `c0b82d1f0c4a3e7dedcf8d6ca49aed11280ffdab` | `/mnt/storage/service/github/wt/lpowner` |
| `matching-liveness-paused-20261024` | `6b44630aed8b2440f83b39865419adc9f599ccf6` | `/mnt/storage/service/github/wt/livepause` |
| `matching-longpoll-diag-20261024` | `63f1dd430c6e4452ffb8a0902989240ba427f178` | `/mnt/storage/service/github/wt/longpoll` |
| `matching-mustoffer-family-20261024` | `97413fb0a8a3d2cc6b6a099682c2feba9b139f3c` | `/mnt/storage/service/github/wt/mustoffer` |
| `matching-open-gaps-v2-20261024` | `bed9d463698c579df996713ee069378419461e56` | `/mnt/storage/service/github/wt/gapsv2` |
| `matching-panic-recovery-20261024` | `a9e82593fa396e2b72b3fec65acc0c6bb16df1db` | `/mnt/storage/service/github/wt/panic` |
| `matching-parity-audit-v4-20261023` | `36181f4db4f75944526455dac38559b8689603f8` | `/mnt/storage/service/github/wt/audit4` |
| `matching-ratelimit-matrix-20261024` | `e1632efeae8dba3d027867786c0c0cda13df1224` | `/mnt/storage/service/github/wt/ratelimit` |
| `matching-rowskip-lateStop-20261024` | `238869b0c28f2688a162109702a341677f457c5e` | `/mnt/storage/service/github/wt/rowskip` |
| `matching-semaphore-rpc2-20261024` | `b6eee17ac8807afcfb920eb488d80afe10aba17b` | `/mnt/storage/service/github/wt/semrpc2` |

## Candidates left untouched

- `matching-engine-apis-20261023`: its worktree had uncommitted changes.
- `matching-twonode-20261024`: its worktree had uncommitted changes.
- `matching-final-candidate-20261024`: no associated worktree was present in the worktree inventory, so it was not treated as a clean inactive worktree candidate.
- `t3/matching-openfeature-config-parity`: clean and merged but left intact after reaching the 15-worktree batch cap.
- Parent-preserved branches/work and the active cache-oracle and scaler-audit worktrees were not touched.
