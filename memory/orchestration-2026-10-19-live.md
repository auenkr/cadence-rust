# Live Matching orchestration recovery

Parent starts at f6e6363d7. Matching remains incomplete; no later service work
is authorized by the service parity gate. User requested ongoing orchestration
with normal-speed Luna workers. User's uncommitted AGENTS.md is preserved.

Native gpt-6-luna, medium reasoning workers (no nested delegation):

- `/root/swim`: retained t3-6f14baa6 worktree, SWIM runtime and actual Go probe.
  Sole heavy build slot; jobs=1, tmux cadence-rust. Must observe ALIVE while
  Rust peer is running, rather than weaken the assertion after process exit.
  Adaptive gossip cadence and waiting for in-flight shutdown remain review gates.
- `/root/runtime_cache`: matching-dns-provider worktree, recover production
  defaultStore/domain-cache wiring. Source only pending build grant. Existing
  draft preserved; parent has requested actual shared Go-store evidence and
  eventual actual production-binary mixed harness rather than adapter evidence.
- `/root/history_routing`: t3-83afa06b worktree, reconcile retained routing
  source with recovered r7 commit 98e559da4 on t3/matching-history-client-parity.
  Source only pending grant. The r7 worktree itself was removed, but commit is
  available. Parent found additional retry review issues: previous error must
  be captured before the operation, including cancellation during backoff;
  factory timeout must not be mistaken for ThrottleRetry WithOperationTimeout.
  Worker was instructed to prove/fix against actual Go behavior.

Root git status initially failed because .git/modules/idls/config core.worktree
pointed at deleted t3-ef455bb6. Parent repaired local metadata to root go/idls;
submodule is clean at pinned bfbf273b41b9e36ce21e47a49600d22d73e4614c.
No tracked submodule source changed. Disk had 60 GB free at recovery.

No new component accepted, no current-source combined validation yet. Prior
service/production gaps and frozen-evidence limitations still apply.
