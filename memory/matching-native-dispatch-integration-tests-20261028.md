# Native dispatch integration tests (2026-10-28) — UNVERIFIED, never compiled

Added to `matching_dispatch_native.rs` test module (scripted `NativeMatchingApi`, `NativeForwardState` with Service parent):
- `native_forward_backlog_keeps_raw_run_id_exact_for_decision_and_activity`: RunIDs `ff fe`, `a NUL b ff`, `"q\ fe` stay byte-exact (never quoted) for decision+activity adds; partitioned source `/__cadence_sys/q\xff/1` routes to raw parent `q\xff`, forwarded_from = raw source, domain ids exact.
- `native_forward_backlog_rejects_root_and_maps_parent_failure`.
- `native_sync_matcher_acknowledges_outcome_to_offerer_and_isolates_groups`.

Not covered: full real queue->writer->reader->sync path (NativeTaskWriter/Reader constructors need config/live-config plumbing); Dispatcher parent path; API pollers Mutex<Manager> beyond the existing lifecycle test; reader isolation with a real reader.
Likely compile risks: tuple destructure of `(&Option<_>, &Option<_>, &GoString)`, `let _ = task;` on `()`, `Arc::new(|| 10)` coercion to `Count`, `NativeParent::Service(api)` needing `Arc<dyn>` coercion (may need `as Arc<dyn NativeMatchingApi>`). Parent should run CI.

Parent review: removed the fixed sleep before the sync offer (the matcher rendezvous handles ordering), bounded the handoff join, and destructured the unit result to avoid clippy let_unit_value. Added fault injection around the production NativeParent::Dispatcher backlog branch using the exact legacy matching-add-{decision,activity}-task names. Service adapters handle their own injection. These updates remain unverified.
