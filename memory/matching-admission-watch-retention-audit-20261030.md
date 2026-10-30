# Matching admission watch retention audit (2026-10-30)

Scope: source-only audit of the PostgreSQL mixed-service gate's validated Rust
commit `3a958668c040b289b495f9b76b2209969db7e2a0`. The active gate is owned by
the parent task; this audit did not inspect or interact with its process, and
did not dispatch validation.

## Finding

There is no lost-startup-signal defect in the Matching wrapper or native
admission gates. Neither production setter uses `watch::Sender::send`:

- `NativeMatchingHost::assemble` constructs `watch::channel(0).0`, retaining
  the sender in the host and dropping the initial receiver. `mark_started`
  calls `send_if_modified` to persist state `1` even with no receivers;
  `begin_shutdown` calls `send_replace(2)`. Every native RPC's
  `check_started` subscribes when invoked, then waits for a nonzero state.
  State `1` admits it, and state `2` rejects it.
- `Matching::with_start_pending` replaces the default initially-ready sender
  with `watch::channel(false).0`, also dropping the initial receiver.
  `Matching::mark_started` uses `send_replace(true)`. `wait_started` subscribes
  per call and `wait_for`s true. Cloned `Matching` handlers share the same
  sender state. The sender remains owned by the handler/runtime for the
  service lifetime; no startup receiver is retained or required.

At the pinned commit, the launcher marks the shared wrapper gate first and the
native runtime gate second, only after membership lifecycle startup completes.
The prebound TChannel listener is started before membership bootstrap so
Ringpop can join. Requests entering there pass through `wait_started` and wait
until the wrapper setter updates state. The gRPC health implementation also
calls `wait_started`; the gRPC listener is spawned after both setters, so a
health call that subscribes after startup sees the stored true value. Native
Matching RPCs additionally enter `NativeMatchingService::enter`, which awaits
the native gate.

For Thrift requests using the native service, native dispatch waits on the
wrapper gate before invoking the native API; the native API then waits on its
own gate. `Meta::health` waits on the wrapper gate. For the wrapper's gRPC
`Meta::health`, `wait_started` subscribes at RPC entry. These subscriptions
are all late-safe because their matching senders persist the latest value.

The launcher logs `Matching startup stage: admission opened` immediately
after both setters. Listener construction/spawning and the normal long-lived
serve select follow that log. Therefore, a log ending at that marker alone is
consistent with normal post-start service and does not establish a deadlock.

## Evidence boundary

This is a source audit only. No Cargo, Go, formatter, runtime, CI, or gate
commands were run. No source changes or behavior changes were warranted. The
audit is tied to the pinned SHA above; the checked-out branch differs from
that SHA in launcher and wrapper source, so these conclusions are based on the
pinned versions of those files.
