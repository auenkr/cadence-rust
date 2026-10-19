# Storage recovery — 2026-10-19

Disk pressure started at 111 MB free on `/dev/sda1` (96 GB filesystem, 100% used). Before cleanup, there were no active `cargo`, `rustc`, or `rust-analyzer` processes. The existing `cadence-rust` tmux session was left running; no unrelated processes, databases, source trees, commits, test evidence, or Git worktrees were removed.

Removed inactive reproducible Rust build output directories under `/home/ubuntu/.t3/worktrees/cadence-rust/*/rust/target` for these worktrees:

- `t3-ecd46278` (274 MB)
- `t3-f02b6d89` (320 MB)
- `t3-af875943` (190 MB)
- `t3-4843bdd9` (1.1 GB)
- `t3-3dfb4a95` (3.4 GB)
- `t3-bcbb155a` (1.1 GB)
- `t3-c8361a1c` (6.8 GB)
- `t3-63f520a4` (459 MB)
- `t3-b9747ee8` (6.0 GB)
- `t3-557b8204` (3.2 GB)
- `t3-481753f4` (459 MB)
- `t3-ce1e72cb` (13 GB)

The coordinator-designated worker caches `t3-e426148a` (7.4 GB) and `t3-975b4b2d` (6.6 GB) were preserved. The newest other warmed target, `t3-a33b4846` (3.6 GB), was preserved. Coordinator-designated paths `t3-367ce881` and `t3-6f14baa6` had no target directory in the discovered target list and were untouched. The `t3-ce1e72cb` target was deleted before the coordinator requested preserving it; its worktree itself remains intact and the target can be rebuilt.

After cleanup, `df -h /` reported 37 GB available (60 GB used, 63%). The operation recovered approximately 36.9 GB and exceeded the 8 GB target. No commit or push was made.
