# perf-history-clones (2026-11-25)

Result: NO code changes; analysis only.

Findings (rust/crates/cadence-history/src):
- execution/history_builder.rs:25 `events.push(event.clone())` in add_event, :50 same for transient, mutable_state.rs:166 same.
  These are the biggest per-event clones, but `add_event` returns the event and ~66 call sites (all add_*_event helpers, timer.rs, signal.rs, child_workflow.rs, ...) consume the returned value.
  Removing the clone needs a signature change (return &HistoryEvent / index / Arc<HistoryEvent>) touching every call site; not safely doable without compiling, and local cargo is forbidden. Needs a CI-iterated refactor.
- execution/history_builder.rs:268 `get_history()` clones the Vec but has zero callers (dead).
- Remaining `.clone()` in history_builder.rs request->attributes conversions (lines 208-404) copy from borrowed requests; ownership is not available there without changing public signatures.

No CI runs dispatched (nothing to validate).
