# Matching stack diagnostic validation

Separate candidate branch `t3/matching-stall-stack-validation` now pins `7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6`. Production Rust source is unchanged from workspace-tested `2c3bc4a9a`; diagnostic patch source `ee33bd20f` was integrated as `62c2be2a7`.

The existing bounded process observer now adds per-thread syscall/kernel-stack samples and ss process/fd attribution. Workflow input `matching_gdb_diagnostic` defaults false and is limited to postgres. When selected, runner installs gdb, permits ptrace, and enables the existing bounded20s capture. Attaching briefly stops Matching; diagnostic results cannot be credited as an undisturbed acceptance pass.

Independent source review found a blocker in the preexisting GDB command: default backtraces print frame arguments, potentially retaining runtime values. Correction `7b46ef884` sets `print frame-arguments none` before `thread apply all bt 12`; no locals or full backtrace requested. No GDB gate has been dispatched yet. Other bounds/defaults/process attribution passed source review. Local git diff --check passed; no local executable validation.

Superseded fresh workspace run41112810382 received a cancellation request; do not credit it. Replacement package-unset clean_target=true run:
https://github.com/auenkr/cadence-rust/actions/runs/41112992201

Require exact head7b46ef884c7b33f1a57f29a1e1ac4055adfb8dc6, fresh target, Clippy and workspace test exits0 and terminal success before immutable PostgreSQL diagnostic rerun with matching_gdb_diagnostic=true. Two independent Luna audits of dynamic-config re-entry and Ringpop synchronous lock ordering remain pending. No source-proven cause for failed mixed gate41108824183 is established; no current mixed lane accepted; Matching incomplete.
