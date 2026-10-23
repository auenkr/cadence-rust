# Group 1: forwarder_test.go (port pass 2)

Command (run from rust/ in worktree gotests2, shared target dir, one run):

    CARGO_TARGET_DIR=/mnt/storage/service/cargo-target-shared \
      /mnt/storage/service/bin/cargo-safe test -p cadence-matching --lib matching_forward_ -- --include-ignored

Log: forwarder-cargo-test.log. Exit: forwarder-cargo-test.exit = 101.

Result: 11 run, 10 passed, 1 failed (expected).
- FAILED (expected parity gap): matching_dispatch::tests::matching_forward_activity_task_fields_match_go_forwarder
  panicked at matching_dispatch.rs:2276: source_domain_id left `…0001` (list domain) right `…0005` (task domain).
  The same test is `#[ignore = "parity gap: ..."]` in the tree, so the default `cargo test` run is green.
- PASS: matching_forward_decision_task_fields_match_go_forwarder, matching_forward_poll_for_decision_fields_match_go_forwarder,
  matching_forward_poll_for_activity_fields_match_go_forwarder, plus the 7 existing matching_forward tests.

Default run (without --include-ignored) is therefore expected to exit 0 with 10 passed and 1 ignored.
Not run separately: AGENTS requires one run per group.

Inventory: memory/matching-go-test-inventory-port2-20261023.md.
